"""Read an explicitly published Gold partition. Never select MAX(snapshot_date)."""
import json
import re
import time
from datetime import date, datetime, timezone


class PublicationUnavailable(RuntimeError):
    pass


def validate_publication(completion, silver, manifest, now=None):
    now = now or datetime.now(timezone.utc)
    run = completion.get("run_id")
    snapshot = completion.get("snapshot_date")
    if not run or run != silver.get("run_id") or run != manifest.get("run_id"):
        raise PublicationUnavailable("The latest run has not been fully published.")
    if snapshot != silver.get("snapshot_date") or snapshot != manifest.get("snapshot_date"):
        raise PublicationUnavailable("Publication dates do not match.")
    if manifest.get("status") != "SUCCEEDED":
        raise PublicationUnavailable("Ingestion did not succeed.")
    try:
        if date.fromisoformat(snapshot).isoformat() != snapshot:
            raise ValueError("Noncanonical date")
        ingested = datetime.fromisoformat(manifest["completed_at"].replace("Z", "+00:00"))
        age = (now - ingested).total_seconds() / 3600
    except (ValueError, TypeError, KeyError) as exc:
        raise PublicationUnavailable("Invalid publication metadata.") from exc
    if not 0 <= age <= 26:
        raise PublicationUnavailable("Data is older than 26 hours or has a future timestamp.")
    return {"run_id": run, "snapshot_date": snapshot, "ingested_at": manifest["completed_at"],
            "published_at": completion["completed_at"], "ingestion_age_hours": round(age, 2),
            "rows_ingested": manifest["total_records"], "successful_boards": manifest["successful_boards"],
            "failed_boards": manifest["failed_boards"],
            "incomplete_boards": sum(b["status"] == "SUCCEEDED" and not b["complete"] for b in manifest["boards"])}


def read_publication(session, bucket, lock_table):
    ddb = session.client("dynamodb")
    def unlocked():
        if ddb.get_item(TableName=lock_table, Key={"LockId": {"S": "pipeline"}}, ConsistentRead=True).get("Item"):
            raise PublicationUnavailable("A pipeline run is in progress. Try again after it completes.")
    unlocked()
    s3 = session.client("s3")
    fingerprints = []
    def read(key):
        response = s3.get_object(Bucket=bucket, Key=key)
        fingerprints.append((key, response.get("VersionId"), response["ETag"]))
        return json.loads(response["Body"].read())
    completion = read("state/pipeline/latest.json")
    silver = read("state/silver/latest.json")
    manifest = read(f"control/ingestion/run_id={completion['run_id']}/manifest.json")
    metadata = validate_publication(completion, silver, manifest)
    unlocked()
    return metadata, tuple(fingerprints)


def query_fact(session, database, workgroup, snapshot_date, *, timeout=120, max_rows=50000):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", database):
        raise ValueError("Invalid database identifier")
    if date.fromisoformat(snapshot_date).isoformat() != snapshot_date:
        raise ValueError("Invalid snapshot date")
    sql = ("SELECT job_id, company, title, location, source, is_remote, apply_url, "
           "first_seen_at, last_seen_at, snapshot_date FROM fact_job_posting "
           f"WHERE snapshot_date = '{snapshot_date}' ORDER BY company, title, job_id")
    athena = session.client("athena")
    query_id = athena.start_query_execution(QueryString=sql,
        QueryExecutionContext={"Database": database}, WorkGroup=workgroup)["QueryExecutionId"]
    deadline = time.monotonic() + timeout
    while True:
        query = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        state = query["Status"]["State"]
        if state in {"FAILED", "CANCELLED"}:
            raise PublicationUnavailable(f"Athena query {query_id} did not succeed. Inspect its execution in AWS.")
        if state == "SUCCEEDED":
            break
        if time.monotonic() >= deadline:
            athena.stop_query_execution(QueryExecutionId=query_id)
            raise PublicationUnavailable("Athena timed out; this query was cancelled. Retry later.")
        time.sleep(1)
    records, header = [], None
    pages = athena.get_paginator("get_query_results").paginate(QueryExecutionId=query_id)
    for page in pages:
        for row in page["ResultSet"]["Rows"]:
            cells = [c.get("VarCharValue") for c in row["Data"]]
            if header is None:
                header = cells
                continue
            record = dict(zip(header, cells))
            record["is_remote"] = record.get("is_remote") == "true"
            records.append(record)
            if len(records) > max_rows:
                raise PublicationUnavailable("Dataset exceeds the workshop display limit; no partial totals are shown.")
    if not records:
        raise PublicationUnavailable("The published fact partition is empty.")
    return records, {"query_id": query_id, "bytes_scanned": query.get("Statistics", {}).get("DataScannedInBytes", 0)}


def load_verified(session, bucket, lock_table, database, workgroup):
    metadata, before = read_publication(session, bucket, lock_table)
    records, query = query_fact(session, database, workgroup, metadata["snapshot_date"])
    _, after = read_publication(session, bucket, lock_table)
    if before != after:
        raise PublicationUnavailable("Publication changed while loading. Refresh to read the new run.")
    return records, metadata, query
