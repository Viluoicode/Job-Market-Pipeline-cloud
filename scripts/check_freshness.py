#!/usr/bin/env python3
"""Read-only pipeline health check. Exit 0 only for a fully published, fresh ingestion run."""
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "glue" / "jobs"))
from pipeline_contract import read_json, timestamp


def assess(completion, silver, manifest, *, now=None, max_age_hours=26):
    now = now or datetime.now(timezone.utc)
    if max_age_hours <= 0:
        raise ValueError("max_age_hours must be positive")
    if not completion or not silver or not manifest:
        return {"healthy": False, "reason": "missing_committed_run_metadata"}
    if not all(isinstance(value, dict) for value in (completion, silver, manifest)):
        return {"healthy": False, "reason": "invalid_committed_run_metadata"}
    if (completion.get("run_id") != silver.get("run_id") or
            completion.get("run_id") != manifest.get("run_id") or manifest.get("status") != "SUCCEEDED"):
        return {"healthy": False, "reason": "silver_not_fully_published"}
    age = (now - timestamp(manifest["completed_at"])).total_seconds() / 3600
    healthy = 0 <= age <= max_age_hours
    return {"healthy": healthy, "reason": "fresh" if healthy else "stale_or_future_ingestion",
            "run_id": completion["run_id"], "snapshot_date": completion["snapshot_date"],
            "ingested_at": manifest["completed_at"], "published_at": completion["completed_at"],
            "ingestion_age_hours": round(age, 2), "rows_ingested": manifest["total_records"],
            "successful_boards": manifest["successful_boards"], "failed_boards": manifest["failed_boards"],
            "incomplete_boards": sum(b["status"] == "SUCCEEDED" and not b["complete"] for b in manifest["boards"])}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--region", default="ap-southeast-1")
    parser.add_argument("--max-age-hours", type=float, default=26)
    args = parser.parse_args()
    import boto3
    s3 = boto3.client("s3", region_name=args.region)
    completion = read_json(s3, args.bucket, "state/pipeline/latest.json", optional=True)
    silver = read_json(s3, args.bucket, "state/silver/latest.json", optional=True)
    manifest = read_json(s3, args.bucket,
        f"control/ingestion/run_id={completion['run_id']}/manifest.json", optional=True) if isinstance(completion, dict) and completion.get("run_id") else None
    result = assess(completion, silver, manifest, max_age_hours=args.max_age_hours)
    print(json.dumps(result, indent=2))
    return 0 if result["healthy"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
