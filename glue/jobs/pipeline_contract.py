"""Run contract shared by Glue jobs; no Spark or Glue imports."""

import json
import re
from datetime import datetime, timezone
from urllib.parse import quote


def timestamp(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Observation timestamps must include a timezone")
    return parsed.astimezone(timezone.utc)


def validate_manifest(manifest, run_id, snapshot_date, *, now=None, max_age_hours=24):
    """Reject stale, partial/uncommitted, mismatched or malformed run input before Spark reads it."""
    now = now or datetime.now(timezone.utc)
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", run_id):
        raise ValueError("Invalid run_id")
    if (manifest.get("version") != 1 or manifest.get("status") != "SUCCEEDED" or
            manifest.get("run_id") != run_id or manifest.get("snapshot_date") != snapshot_date):
        raise ValueError("Manifest does not match a committed ingestion run")
    started, completed = timestamp(manifest["started_at"]), timestamp(manifest["completed_at"])
    if max_age_hours <= 0 or not 0 <= (now - completed).total_seconds() <= max_age_hours * 3600:
        raise ValueError("Ingestion is stale or dated in the future")
    if started > completed or (completed - started).total_seconds() > 24 * 3600:
        raise ValueError("Invalid ingestion time window")
    seen, keys, complete_boards, total = set(), [], [], 0
    boards = manifest.get("boards", [])
    if not boards:
        raise ValueError("Manifest has no boards")
    for board in boards:
        identity = (board["source"], board["board_token"])
        if identity in seen or identity[0] not in {"greenhouse", "lever", "ashby", "arbeitnow"} or not identity[1]:
            raise ValueError("Duplicate or invalid manifest board")
        seen.add(identity)
        if board["status"] == "FAILED":
            if board.get("row_count") != 0 or board.get("complete") is not False or board.get("key"):
                raise ValueError("Failed board cannot contribute observations")
            continue
        if board["status"] != "SUCCEEDED" or not isinstance(board.get("complete"), bool):
            raise ValueError("Invalid board outcome")
        count = board["row_count"]
        if type(count) is not int or count < 0:
            raise ValueError("Invalid board row count")
        expected_key = f"bronze/run_id={run_id}/source={identity[0]}/board={quote(identity[1], safe='')}/jobs.json"
        if board["key"] != expected_key or not started <= timestamp(board["fetched_at"]) <= completed:
            raise ValueError("Board key or observation time is outside this run")
        if count:
            keys.append(board["key"])
        total += count
        if board["complete"]:
            complete_boards.append(identity)
    if total <= 0 or total != manifest.get("total_records"):
        raise ValueError("Manifest row totals do not match")
    return keys, complete_boards


def read_json(s3, bucket, key, *, optional=False):
    from botocore.exceptions import ClientError
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except ClientError as exc:
        if optional and exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise


def silver_path(bucket, run_id):
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", run_id):
        raise ValueError("Invalid run_id")
    return f"s3://{bucket}/silver/runs/{run_id}/jobs/"
