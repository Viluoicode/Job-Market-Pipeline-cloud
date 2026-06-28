#!/usr/bin/env python3
"""Land tech job postings from public ATS feeds into the S3 Bronze zone (or a local dir).

Bronze = raw, append-only landing. For each board in ``sources.json`` we hit the public ATS
endpoint, normalize every posting to one flat JSON shape (the same canonical fields SkillRadar
uses), and write newline-delimited JSON partitioned by source/date:

    s3://<LAKE_BUCKET>/bronze/source=<source>/date=<YYYY-MM-DD>/jobs.json

Downstream, the Glue job ``bronze_to_silver.py`` types + cross-source dedups this into Silver
Parquet. No API keys are required — all four sources are public.

Usage:
    export LAKE_BUCKET="$(terraform -chdir=infra output -raw lake_bucket)"
    python ingestion/land_to_bronze.py                   # -> S3
    python ingestion/land_to_bronze.py --out-dir ./out   # -> local dry-run, no AWS needed
    python ingestion/land_to_bronze.py --limit 3         # only first 3 boards (smoke test)
"""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import httpx

# Windows consoles often default to cp1252; force UTF-8 so logging the (possibly non-ASCII) lake
# path or company names never crashes the run.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

# --------------------------------------------------------------------------- text helpers
_HTML_TAG = re.compile(r"<[^>]+>", re.S)
_WHITESPACE = re.compile(r"\s+")


def strip_html(value: str | None) -> str:
    """ATS HTML (often entity-encoded) -> plain text. Port of SkillRadar domain/text.strip_html."""
    if not value or not value.strip():
        return ""
    decoded = html.unescape(value)
    without_tags = _HTML_TAG.sub(" ", decoded)
    return _WHITESPACE.sub(" ", without_tags).strip()


def parse_iso(value: str | None) -> str | None:
    """ISO-8601 (offset or trailing 'Z') -> normalized ISO string, else None (stored as text)."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).isoformat()
    except (ValueError, AttributeError):
        return None


def from_epoch_ms(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


# --------------------------------------------------------------------------- json accessors
def get_str(obj, key):
    if isinstance(obj, dict) and isinstance(obj.get(key), str):
        return obj[key]
    return None


def get_bool(obj, key) -> bool:
    return isinstance(obj, dict) and obj.get(key) is True


def get_child(obj, key):
    if isinstance(obj, dict) and isinstance(obj.get(key), dict):
        return obj[key]
    return None


def get_id(obj, key):
    """Read an id as a string, accepting a JSON string or number (mirrors SkillRadar JsonHelpers)."""
    if not isinstance(obj, dict) or key not in obj:
        return None
    v = obj[key]
    if isinstance(v, bool):  # bool is a subclass of int — exclude it
        return None
    if isinstance(v, str):
        return v
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        return str(int(v)) if v.is_integer() else str(v)
    return None


def _looks_remote(*values) -> bool:
    return any(v and "remote" in v.lower() for v in values)


def _record(source, board_token, source_job_id, company, title, location, remote,
            description, apply_url, posted_at, raw) -> dict:
    """One canonical Bronze posting (flat — Glue reads these columns directly)."""
    return {
        "source": source,
        "board_token": board_token,
        "source_job_id": source_job_id,
        "company": company,
        "title": title,
        "location": location,
        "remote": remote,
        "description": description,
        "apply_url": apply_url,
        "posted_at": posted_at,
        "raw_json": json.dumps(raw, ensure_ascii=False),
    }


# --------------------------------------------------------------------------- connectors
def _get_json(client: httpx.Client, url: str):
    resp = client.get(url)
    resp.raise_for_status()
    return resp.json()


def fetch_greenhouse(client, token, company_hint):
    """GET boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true"""
    url = f"https://boards-api.greenhouse.io/v1/boards/{quote(token, safe='')}/jobs?content=true"
    payload = _get_json(client, url)
    jobs = payload.get("jobs") if isinstance(payload, dict) else None
    out = []
    for job in jobs or []:
        jid = get_id(job, "id")
        if not jid:
            continue
        loc = get_child(job, "location")
        location = get_str(loc, "name") if loc else None
        company = get_str(job, "company_name") or company_hint or token
        out.append(_record(
            "greenhouse", token, jid, company,
            get_str(job, "title") or "", location, _looks_remote(location),
            strip_html(get_str(job, "content")),
            get_str(job, "absolute_url") or "",
            parse_iso(get_str(job, "updated_at") or get_str(job, "first_published")),
            job,
        ))
    return out


def fetch_lever(client, token, company_hint):
    """GET api.lever.co/v0/postings/{token}?mode=json (Lever has no company name -> use hint)."""
    url = f"https://api.lever.co/v0/postings/{quote(token, safe='')}?mode=json"
    payload = _get_json(client, url)
    out = []
    for job in payload if isinstance(payload, list) else []:
        jid = get_id(job, "id")
        if not jid:
            continue
        cats = get_child(job, "categories")
        location = get_str(cats, "location") if cats else None
        commitment = get_str(cats, "commitment") if cats else None
        description = get_str(job, "descriptionPlain") or strip_html(get_str(job, "description"))
        workplace = (get_str(job, "workplaceType") or "").lower()
        remote = workplace == "remote" or _looks_remote(location, commitment)
        created = job.get("createdAt")
        posted_at = from_epoch_ms(created) if isinstance(created, int) and not isinstance(created, bool) else None
        out.append(_record(
            "lever", token, jid, company_hint or token,
            get_str(job, "text") or "", location, remote,
            description or "",
            get_str(job, "hostedUrl") or get_str(job, "applyUrl") or "",
            posted_at, job,
        ))
    return out


def fetch_ashby(client, token, company_hint):
    """GET api.ashbyhq.com/posting-api/job-board/{token}?includeCompensation=true"""
    url = f"https://api.ashbyhq.com/posting-api/job-board/{quote(token, safe='')}?includeCompensation=true"
    payload = _get_json(client, url)
    jobs = payload.get("jobs") if isinstance(payload, dict) else None
    out = []
    for job in jobs or []:
        jid = get_id(job, "id")
        if not jid:
            continue
        description = get_str(job, "descriptionPlain") or strip_html(get_str(job, "descriptionHtml"))
        out.append(_record(
            "ashby", token, jid, company_hint or token,
            get_str(job, "title") or "", get_str(job, "location"), get_bool(job, "isRemote"),
            description or "",
            get_str(job, "jobUrl") or get_str(job, "applyUrl") or "",
            parse_iso(get_str(job, "publishedAt")),
            job,
        ))
    return out


def fetch_arbeitnow(client, token, company_hint, max_pages=2):
    """GET www.arbeitnow.com/api/job-board-api — one global feed (token ignored), paginated."""
    out = []
    url = "https://www.arbeitnow.com/api/job-board-api"
    pages = 0
    while url and pages < max_pages:
        payload = _get_json(client, url)
        if not isinstance(payload, dict):
            break
        for job in payload.get("data") or []:
            slug = get_id(job, "slug")
            if not slug:
                continue
            company = get_str(job, "company_name") or "arbeitnow"
            created = job.get("created_at")  # unix seconds
            posted_at = from_epoch_ms(created * 1000) if isinstance(created, int) and not isinstance(created, bool) else None
            out.append(_record(
                "arbeitnow", "arbeitnow", slug, company,
                get_str(job, "title") or "", get_str(job, "location"), get_bool(job, "remote"),
                strip_html(get_str(job, "description")),
                get_str(job, "url") or "",
                posted_at, job,
            ))
        links = payload.get("links")
        url = links.get("next") if isinstance(links, dict) else None
        pages += 1
    return out


CONNECTORS = {
    "greenhouse": fetch_greenhouse,
    "lever": fetch_lever,
    "ashby": fetch_ashby,
    "arbeitnow": fetch_arbeitnow,
}


# --------------------------------------------------------------------------- sinks
def write_local(out_dir, source, date, records) -> str:
    path = Path(out_dir) / "bronze" / f"source={source}" / f"date={date}"
    path.mkdir(parents=True, exist_ok=True)
    body = "\n".join(json.dumps(r, ensure_ascii=False) for r in records) + "\n"
    (path / "jobs.json").write_text(body, encoding="utf-8")
    return str(path / "jobs.json")


def write_s3(s3, bucket, source, date, records) -> str:
    key = f"bronze/source={source}/date={date}/jobs.json"
    body = ("\n".join(json.dumps(r, ensure_ascii=False) for r in records) + "\n").encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/x-ndjson")
    return f"s3://{bucket}/{key}"


# --------------------------------------------------------------------------- main
def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--sources", default=str(Path(__file__).with_name("sources.json")))
    parser.add_argument("--out-dir", default=None,
                        help="Write to this local dir instead of S3 (dry-run, no AWS needed).")
    parser.add_argument("--bucket", default=os.environ.get("LAKE_BUCKET"),
                        help="S3 lake bucket (defaults to env LAKE_BUCKET).")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only process the first N boards (smoke test).")
    parser.add_argument("--arbeitnow-pages", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=30.0)
    args = parser.parse_args(argv)

    if not args.out_dir and not args.bucket:
        parser.error("set --bucket / LAKE_BUCKET for S3, or --out-dir for a local dry-run")

    boards = json.loads(Path(args.sources).read_text(encoding="utf-8"))["boards"]
    if args.limit:
        boards = boards[: args.limit]

    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    by_source: dict[str, list] = {}
    seen_arbeitnow = False

    headers = {"User-Agent": "jobmarket-aws-pipeline/1.0 (+portfolio)"}
    with httpx.Client(timeout=args.timeout, headers=headers, follow_redirects=True) as client:
        for board in boards:
            source = (board.get("source") or "").strip().lower()
            token = board.get("token") or ""
            company_hint = board.get("company")
            fn = CONNECTORS.get(source)
            if fn is None:
                print(f"  ! skip unknown source: {board.get('source')!r}", file=sys.stderr)
                continue
            if source == "arbeitnow":
                if seen_arbeitnow:  # single global feed — fetch once
                    continue
                seen_arbeitnow = True
            label = f"{source}/{token}"
            try:
                if source == "arbeitnow":
                    records = fn(client, token, company_hint, max_pages=args.arbeitnow_pages)
                else:
                    records = fn(client, token, company_hint)
            except Exception as exc:  # noqa: BLE001 — stay resilient: log one board, keep going
                print(f"  ! {label}: {type(exc).__name__}: {exc}", file=sys.stderr)
                continue
            by_source.setdefault(source, []).extend(records)
            print(f"  + {label}: {len(records)} postings")

    total = sum(len(v) for v in by_source.values())
    if total == 0:
        print("No postings fetched — nothing written.", file=sys.stderr)
        return 1

    s3 = None
    if not args.out_dir:
        import boto3  # lazy: dry-run never needs AWS

        s3 = boto3.client("s3")

    for source, records in sorted(by_source.items()):
        if args.out_dir:
            dest = write_local(args.out_dir, source, date, records)
        else:
            dest = write_s3(s3, args.bucket, source, date, records)
        print(f"  -> {len(records):>5} {source:<11} {dest}")

    print(f"Done: {total} postings across {len(by_source)} sources (date={date}).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
