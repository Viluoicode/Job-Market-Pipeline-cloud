#!/usr/bin/env python3
"""Land tech job postings from public ATS feeds into the S3 Bronze zone (or a local dir).

Bronze = raw, append-only landing. For each board in ``sources.json`` we hit the public ATS
endpoint, normalize every posting to one flat JSON shape (the same canonical fields SkillRadar
uses), and write newline-delimited JSON partitioned by run and board:

    s3://<LAKE_BUCKET>/bronze/run_id=<run-id>/source=<source>/board=<board>/jobs.json

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
import time
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
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


# Bronze "remote" is tri-state: True = fully remote, False = onsite or hybrid, None = no signal.
# A plain city ("Paris") is not evidence of onsite work, so it stays None rather than False.
_NOT_REMOTE_WORDS = ("hybrid", "on-site", "onsite", "in office", "in-office")


def _remote_from_text(*values):
    text = " ".join(v.lower() for v in values if isinstance(v, str))
    if "remote" in text:
        return True
    if any(word in text for word in _NOT_REMOTE_WORDS):
        return False
    return None


def _remote_from_workplace(workplace):
    """Map an explicit workplace type (Lever/Ashby) to the tri-state flag; None if absent/unknown."""
    workplace = (workplace or "").strip().lower().replace("-", "").replace(" ", "")
    if workplace == "remote":
        return True
    if workplace in ("hybrid", "onsite"):
        return False
    return None


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
    for attempt in range(3):
        try:
            resp = client.get(url)
            resp.raise_for_status()
            return resp.json()
        except (httpx.TransportError, httpx.HTTPStatusError) as exc:
            retryable = not isinstance(exc, httpx.HTTPStatusError) or (
                exc.response.status_code == 429 or exc.response.status_code >= 500
            )
            if not retryable or attempt == 2:
                raise
            delay = 2 ** attempt
            if isinstance(exc, httpx.HTTPStatusError):
                try:
                    delay = min(30, max(delay, float(exc.response.headers.get("Retry-After", "0"))))
                except ValueError:
                    pass
            time.sleep(delay)


def require_jobs(value):
    """A broken API response must never masquerade as an empty, complete board."""
    if not isinstance(value, list) or any(not isinstance(job, dict) for job in value):
        raise ValueError("API response does not contain a list of job objects")
    return value


def fetch_greenhouse(client, token, company_hint):
    """GET boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true"""
    url = f"https://boards-api.greenhouse.io/v1/boards/{quote(token, safe='')}/jobs?content=true"
    payload = _get_json(client, url)
    jobs = payload.get("jobs") if isinstance(payload, dict) else None
    out = []
    for job in require_jobs(jobs):
        jid = get_id(job, "id")
        if not jid:
            raise ValueError("Greenhouse job is missing its id")
        loc = get_child(job, "location")
        location = get_str(loc, "name") if loc else None
        company = get_str(job, "company_name") or company_hint or token
        out.append(_record(
            "greenhouse", token, jid, company,
            get_str(job, "title") or "", location, _remote_from_text(location),
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
    for job in require_jobs(payload):
        jid = get_id(job, "id")
        if not jid:
            raise ValueError("Lever job is missing its id")
        cats = get_child(job, "categories")
        location = get_str(cats, "location") if cats else None
        commitment = get_str(cats, "commitment") if cats else None
        description = get_str(job, "descriptionPlain") or strip_html(get_str(job, "description"))
        remote = _remote_from_workplace(get_str(job, "workplaceType"))
        if remote is None:
            remote = _remote_from_text(location, commitment)
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
    for job in require_jobs(jobs):
        jid = get_id(job, "id")
        if not jid:
            raise ValueError("Ashby job is missing its id")
        description = get_str(job, "descriptionPlain") or strip_html(get_str(job, "descriptionHtml"))
        location = get_str(job, "location")
        # workplaceType first: Ashby sets isRemote=True on Hybrid jobs, which are not fully remote.
        remote = _remote_from_workplace(get_str(job, "workplaceType"))
        if remote is None:
            remote = job["isRemote"] if isinstance(job.get("isRemote"), bool) else _remote_from_text(location)
        out.append(_record(
            "ashby", token, jid, company_hint or token,
            get_str(job, "title") or "", location, remote,
            description or "",
            get_str(job, "jobUrl") or get_str(job, "applyUrl") or "",
            parse_iso(get_str(job, "publishedAt")),
            job,
        ))
    return out


def fetch_arbeitnow(client, token, company_hint, max_pages=2, with_completeness=False):
    """GET www.arbeitnow.com/api/job-board-api — one global feed (token ignored), paginated."""
    out = []
    url = "https://www.arbeitnow.com/api/job-board-api"
    pages = 0
    while url and pages < max_pages:
        payload = _get_json(client, url)
        if not isinstance(payload, dict):
            raise ValueError("Invalid Arbeitnow response")
        for job in require_jobs(payload.get("data")):
            slug = get_id(job, "slug")
            if not slug:
                raise ValueError("Arbeitnow job is missing its slug")
            company = get_str(job, "company_name") or "arbeitnow"
            created = job.get("created_at")  # unix seconds
            is_epoch = isinstance(created, int) and not isinstance(created, bool)
            posted_at = from_epoch_ms(created * 1000) if is_epoch else None
            out.append(_record(
                "arbeitnow", "arbeitnow", slug, company,
                get_str(job, "title") or "", get_str(job, "location"),
                job["remote"] if isinstance(job.get("remote"), bool) else None,
                strip_html(get_str(job, "description")),
                get_str(job, "url") or "",
                posted_at, job,
            ))
        links = payload.get("links")
        if not isinstance(links, dict) or "next" not in links:
            raise ValueError("Arbeitnow pagination metadata is missing")
        url = links["next"]
        if url is not None and (not isinstance(url, str) or not url.startswith("https://www.arbeitnow.com/")):
            raise ValueError("Unexpected Arbeitnow next-page URL")
        pages += 1
    return (out, not bool(url)) if with_completeness else out


# Concurrent API calls: polite to public job boards, ~5x fewer sequential waits.
FETCH_WORKERS = 5

CONNECTORS = {
    "greenhouse": fetch_greenhouse,
    "lever": fetch_lever,
    "ashby": fetch_ashby,
    "arbeitnow": fetch_arbeitnow,
}



# A manifest is the commit record: transforms never discover input by scanning Bronze.
def validate_run_id(run_id):
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", run_id):
        raise ValueError("run_id must contain 1-80 letters, digits, underscores or hyphens")
    return run_id


def put_bytes(key, body, *, s3=None, bucket=None, out_dir=None, content_type="application/json"):
    if out_dir:
        path = Path(out_dir) / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    else:
        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)


def resolve_run_clock(run_id, snapshot_date, execution_started_at, *, boards,
                      min_success_ratio, arbeitnow_pages, timeout):
    """Validate the run arguments; return (started, execution_start) as aware UTC datetimes.

    snapshot_date is the UTC date of the Step Functions execution start, which must be at most
    24 hours old, so a retried run keeps its original date even after midnight.
    """
    validate_run_id(run_id)
    if not boards or not 0 < min_success_ratio <= 1 or arbeitnow_pages < 1 or timeout <= 0:
        raise ValueError("Nonempty boards, positive timeout/pages and success ratio in (0, 1] required")
    started = datetime.now(timezone.utc)
    execution_start = (datetime.fromisoformat(execution_started_at.replace("Z", "+00:00"))
                       if execution_started_at else started)
    if execution_start.tzinfo is None:
        raise ValueError("snapshot_date must match the UTC execution start within the last 24 hours")
    same_utc_date = snapshot_date == execution_start.astimezone(timezone.utc).date().isoformat()
    recent = 0 <= (started - execution_start).total_seconds() <= 24 * 3600
    if not (same_utc_date and recent):
        raise ValueError("snapshot_date must match the UTC execution start within the last 24 hours")
    return started, execution_start


def read_existing_manifest(manifest_key, *, s3=None, bucket=None, out_dir=None):
    """Return the manifest already written for this run, or None."""
    if out_dir:
        path = Path(out_dir) / manifest_key
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None
    found = s3.list_objects_v2(Bucket=bucket, Prefix=manifest_key, MaxKeys=1)
    if any(item["Key"] == manifest_key for item in found.get("Contents", [])):
        return json.loads(s3.get_object(Bucket=bucket, Key=manifest_key)["Body"].read())
    return None


def committed_manifest(existing, run_id, snapshot_date):
    """A SUCCEEDED manifest is immutable: a retry reuses it instead of fetching again."""
    if not existing or existing.get("status") != "SUCCEEDED":
        return None
    if existing.get("run_id") != run_id or existing.get("snapshot_date") != snapshot_date:
        raise ValueError("Existing run manifest has different identity")
    return existing


def validate_boards(boards):
    """Reject unknown sources, empty or duplicate tokens and a misconfigured Arbeitnow feed."""
    unique = set()
    for board in boards:
        source, token = board.get("source", "").lower().strip(), board.get("token", "")
        if source not in CONNECTORS or not token or (source, token) in unique:
            raise ValueError("Invalid or duplicate configured board")
        if source == "arbeitnow" and token != "arbeitnow":
            raise ValueError("Arbeitnow uses the single board token 'arbeitnow'")
        unique.add((source, token))


def fetch_board(client, board, arbeitnow_pages):
    """Call one board's API. Returns (records, complete); raises on any API/parsing failure."""
    source, token = board["source"].lower().strip(), board["token"]
    if source == "arbeitnow":
        return fetch_arbeitnow(client, token, board.get("company"), arbeitnow_pages, with_completeness=True)
    return CONNECTORS[source](client, token, board.get("company")), True


def safe_fetch(client, board, arbeitnow_pages):
    """Worker-thread step: call the API and RETURN any failure instead of raising it.

    Returns (records, complete, fetched_at, error). Storage never happens here, so a storage
    error can only come from land_board in the main thread and still aborts the whole run.
    """
    try:
        records, complete = fetch_board(client, board, arbeitnow_pages)
    except Exception as exc:
        return None, False, None, exc
    return records, complete, datetime.now(timezone.utc).isoformat(), None


def land_board(board, outcome, *, run_id, s3=None, bucket=None, out_dir=None):
    """Land one fetched board in Bronze; return its manifest entry.

    An API failure only marks this board FAILED. A storage failure propagates and aborts the run:
    it says nothing about the source, and a manifest must never reference an unwritten object.
    """
    records, complete, fetched_at, error = outcome
    source, token = board["source"].lower().strip(), board["token"]
    entry = {"source": source, "board_token": token, "status": "FAILED", "complete": False, "row_count": 0}
    if error is not None:
        entry["error_type"] = type(error).__name__
        if isinstance(error, httpx.HTTPStatusError):
            entry["http_status"] = error.response.status_code
        print(f"[ingest] {source}/{token}: FAILED ({type(error).__name__}, "
              f"http_status={entry.get('http_status', 'n/a')})", file=sys.stderr)
        return entry
    for record in records:
        record.update(run_id=run_id, fetched_at=fetched_at)
    key = f"bronze/run_id={run_id}/source={source}/board={quote(token, safe='')}/jobs.json"
    body = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in records).encode("utf-8")
    put_bytes(key, body, s3=s3, bucket=bucket, out_dir=out_dir, content_type="application/x-ndjson")
    entry.update(status="SUCCEEDED", complete=complete, row_count=len(records), key=key, fetched_at=fetched_at)
    print(f"[ingest] {source}/{token}: {len(records)} postings (complete={complete})")
    return entry


def ingest_board(client, board, *, run_id, arbeitnow_pages, s3=None, bucket=None, out_dir=None):
    """Fetch and land a single board sequentially."""
    return land_board(board, safe_fetch(client, board, arbeitnow_pages),
                      run_id=run_id, s3=s3, bucket=bucket, out_dir=out_dir)


def fetch_in_order(client, boards, arbeitnow_pages, workers):
    """Yield (board, outcome) in configured order while up to `workers` API calls run at once.

    Only 2 x workers boards are ever fetched ahead of the consumer. Glue Python Shell has about
    1 GB of memory and one run's Bronze is ~150 MB of JSON, so outcomes must not pile up behind a
    slow board. The shared httpx.Client is thread-safe.
    """
    upcoming = iter(boards)
    pending = deque()
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="fetch") as pool:
        def submit_next():
            board = next(upcoming, None)
            if board is not None:
                pending.append((board, pool.submit(safe_fetch, client, board, arbeitnow_pages)))
        try:
            for _ in range(2 * workers):
                submit_next()
            while pending:
                board, future = pending.popleft()
                outcome = future.result()
                submit_next()
                yield board, outcome
        finally:
            # Writer aborted (storage error): do not start the boards still queued.
            for _, future in pending:
                future.cancel()


def finalize_manifest(manifest, n_boards, min_success_ratio):
    """Add completion counters and decide the run status."""
    succeeded = sum(b["status"] == "SUCCEEDED" for b in manifest["boards"])
    manifest.update(completed_at=datetime.now(timezone.utc).isoformat(),
                    successful_boards=succeeded, failed_boards=n_boards - succeeded,
                    success_ratio=succeeded / n_boards)
    # An empty market is not automatically published: a zero-row run requires investigation.
    has_rows = manifest["total_records"] > 0
    manifest["status"] = "SUCCEEDED" if has_rows and manifest["success_ratio"] >= min_success_ratio else "FAILED"
    return manifest


def run_ingestion(boards, *, run_id, snapshot_date, s3=None, bucket=None, out_dir=None,
                  min_success_ratio=0.8, arbeitnow_pages=2, timeout=20.0, client=None,
                  execution_started_at=None, fetch_workers=FETCH_WORKERS):
    """Fetch every board into Bronze, then commit the run manifest (written last).

    API calls run concurrently; Bronze writes and the manifest stay sequential in configured board
    order, so the manifest is deterministic and a storage error still aborts the run.
    """
    if fetch_workers < 1:
        raise ValueError("fetch_workers must be at least 1")
    started, execution_start = resolve_run_clock(
        run_id, snapshot_date, execution_started_at, boards=boards,
        min_success_ratio=min_success_ratio, arbeitnow_pages=arbeitnow_pages, timeout=timeout)
    manifest_key = f"control/ingestion/run_id={run_id}/manifest.json"
    existing = read_existing_manifest(manifest_key, s3=s3, bucket=bucket, out_dir=out_dir)
    committed = committed_manifest(existing, run_id, snapshot_date)
    if committed:
        print(f"[ingest] reusing immutable committed run {run_id}")
        return committed
    validate_boards(boards)

    manifest = {"version": 1, "run_id": run_id, "snapshot_date": snapshot_date,
                "started_at": started.isoformat(), "execution_started_at": execution_start.isoformat(),
                "boards": [], "total_records": 0}
    owned_client = client is None
    client = client or httpx.Client(timeout=timeout, follow_redirects=True,
                                   headers={"User-Agent": "jobmarket-aws-pipeline/2.0 (+portfolio)"})
    try:
        with closing(fetch_in_order(client, boards, arbeitnow_pages, fetch_workers)) as fetched:
            for board, outcome in fetched:
                entry = land_board(board, outcome, run_id=run_id, s3=s3, bucket=bucket, out_dir=out_dir)
                manifest["boards"].append(entry)
                manifest["total_records"] += entry["row_count"]
    finally:
        if owned_client:
            client.close()

    finalize_manifest(manifest, len(boards), min_success_ratio)
    put_bytes(manifest_key, json.dumps(manifest, indent=2).encode("utf-8"),
              s3=s3, bucket=bucket, out_dir=out_dir)
    print(f"[ingest] {manifest['status']}: {manifest['total_records']} rows; manifest={manifest_key}")
    return manifest


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sources", default=str(Path(__file__).with_name("sources.json")))
    parser.add_argument("--sources-s3-uri")
    parser.add_argument("--out-dir")
    parser.add_argument("--bucket", default=os.environ.get("LAKE_BUCKET"))
    parser.add_argument("--run-id", default=uuid.uuid4().hex)
    parser.add_argument("--snapshot-date", default=datetime.now(timezone.utc).date().isoformat())
    parser.add_argument("--execution-started-at")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--arbeitnow-pages", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--min-success-ratio", type=float, default=0.8)
    # Python Shell passes these options even when it omits --JOB_NAME. Recognize the
    # runtime contract explicitly while still rejecting misspelled application arguments.
    for runtime_flag in ("--additional-python-modules", "--scriptLocation", "--library-set",
                         "--python-version", "--JOB_NAME", "--JOB_ID", "--JOB_RUN_ID", "--TempDir"):
        parser.add_argument(runtime_flag, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if not args.out_dir and not args.bucket:
        parser.error("set --bucket / LAKE_BUCKET or --out-dir")
    if args.limit is not None and (args.limit < 1 or not args.out_dir):
        parser.error("--limit requires a positive value and --out-dir (partial production runs are unsafe)")
    s3 = None
    if not args.out_dir or args.sources_s3_uri:
        import boto3
        s3 = boto3.client("s3")
    if args.sources_s3_uri:
        from urllib.parse import urlparse
        uri = urlparse(args.sources_s3_uri)
        if uri.scheme != "s3" or not uri.netloc or not uri.path.lstrip("/"):
            parser.error("--sources-s3-uri must be an S3 object URI")
        body = s3.get_object(Bucket=uri.netloc, Key=uri.path.lstrip("/"))["Body"].read()
    else:
        body = Path(args.sources).read_text(encoding="utf-8")
    boards = json.loads(body)["boards"]
    if args.limit:
        boards = boards[:args.limit]
    manifest = run_ingestion(boards, run_id=args.run_id, snapshot_date=args.snapshot_date,
                             s3=s3, bucket=args.bucket, out_dir=args.out_dir,
                             min_success_ratio=args.min_success_ratio,
                             arbeitnow_pages=args.arbeitnow_pages, timeout=args.timeout,
                             execution_started_at=args.execution_started_at)
    return 0 if manifest["status"] == "SUCCEEDED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
