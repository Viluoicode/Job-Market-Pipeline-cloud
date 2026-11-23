"""Compare the published Parquet Gold marts with the shadow Iceberg marts for one snapshot date.

Phase A of task 5.1 keeps Parquet as the published contract and writes the same marts to Iceberg.
Run this after each scheduled run; switch readers to Iceberg only after several MATCH days.

  python scripts/compare_iceberg.py [--snapshot-date YYYY-MM-DD] [--output docs/evidence/...json]

Read-only: Athena SELECT queries (a few MB scanned) and S3/Glue metadata reads.
"""
import argparse
import json
import re
import subprocess
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MARTS = ["fact_job_posting", "demand_by_role", "role_opportunity"]
_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?")


def _checked(table, snapshot_date):
    if not _IDENT.fullmatch(table):
        raise ValueError(f"Invalid table identifier: {table!r}")
    if date.fromisoformat(snapshot_date).isoformat() != snapshot_date:
        raise ValueError(f"Invalid snapshot date: {snapshot_date!r}")


def _normalized(name, glue_type):
    # Compare instants in whole milliseconds: Athena reads the Hive/Parquet tables' timestamps at
    # millisecond precision, while Iceberg keeps Spark's microseconds (first run, 23 Nov 2026).
    if "timestamp" in glue_type:
        return f"floor(to_unixtime({name}) * 1000)"
    return f"CAST({name} AS varchar)"


def except_sql(columns, parquet_table, iceberg_table, snapshot_date, *, reverse=False):
    """Count rows of one side missing from the other for one day (partition column excluded)."""
    for table in (parquet_table, iceberg_table):
        _checked(table, snapshot_date)
    select = ", ".join(_normalized(n, t) for n, t in columns if n != "snapshot_date")
    parquet = f"SELECT {select} FROM {parquet_table} WHERE snapshot_date = '{snapshot_date}'"
    iceberg = f"SELECT {select} FROM {iceberg_table} WHERE snapshot_date = DATE '{snapshot_date}'"
    first, second = (iceberg, parquet) if reverse else (parquet, iceberg)
    return f"SELECT count(*) FROM ({first} EXCEPT {second})"


def verdict(counts, differences):
    same_counts = all(c["parquet"] == c["iceberg"] for c in counts.values())
    return "MATCH" if same_counts and not any(differences.values()) else "MISMATCH"


def _terraform_output(name):
    return subprocess.run(["terraform", f"-chdir={ROOT / 'infra'}", "output", "-raw", name],
                          check=True, capture_output=True, text=True).stdout.strip()


def _scalar(athena, sql, workgroup):
    query_id = athena.start_query_execution(QueryString=sql, WorkGroup=workgroup)["QueryExecutionId"]
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]
        if status["State"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            break
        time.sleep(1)
    if status["State"] != "SUCCEEDED":
        raise RuntimeError(f"Athena {query_id}: {status.get('StateChangeReason', status['State'])}")
    rows = athena.get_query_results(QueryExecutionId=query_id)["ResultSet"]["Rows"]
    return int(rows[1]["Data"][0]["VarCharValue"])


def compare(session, snapshot_date=None):
    sys.path.insert(0, str(ROOT / "glue" / "jobs"))
    from pipeline_contract import read_json

    s3, glue, athena = session.client("s3"), session.client("glue"), session.client("athena")
    lake, workgroup = _terraform_output("lake_bucket"), _terraform_output("athena_workgroup")
    parquet_db, iceberg_db = _terraform_output("gold_database"), _terraform_output("gold_iceberg_database")
    completion = read_json(s3, lake, "state/pipeline/latest.json")
    snapshot_date = snapshot_date or completion["snapshot_date"]
    shadow = read_json(s3, lake, f"state/gold_iceberg/run_id={completion['run_id']}.json", optional=True)

    counts = {}
    for mart in MARTS:
        counts[mart] = {
            side: _scalar(athena, f"SELECT count(*) FROM {db}.{mart} WHERE snapshot_date = "
                                  f"{literal}'{snapshot_date}'", workgroup)
            for side, db, literal in (("parquet", parquet_db, ""), ("iceberg", iceberg_db, "DATE "))
        }
    fact_table = glue.get_table(DatabaseName=parquet_db, Name="fact_job_posting")["Table"]
    columns = [(c["Name"], c["Type"]) for c in fact_table["StorageDescriptor"]["Columns"]]
    differences = {
        key: _scalar(athena, except_sql(columns, f"{parquet_db}.fact_job_posting",
                                        f"{iceberg_db}.fact_job_posting", snapshot_date, reverse=reverse), workgroup)
        for key, reverse in (("parquet_only", False), ("iceberg_only", True))
    }
    snapshots = _scalar(athena, f'SELECT count(*) FROM {iceberg_db}."fact_job_posting$snapshots"', workgroup)
    return {"checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "snapshot_date": snapshot_date, "run_id": completion["run_id"], "shadow_write": shadow,
            "row_counts": counts, "fact_row_differences": differences,
            "fact_iceberg_snapshots": snapshots, "verdict": verdict(counts, differences)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--snapshot-date")
    parser.add_argument("--region", default="ap-southeast-1")
    parser.add_argument("--output")
    args = parser.parse_args(argv)
    import boto3
    report = compare(boto3.Session(region_name=args.region), args.snapshot_date)
    if args.output:
        Path(args.output).write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0 if report["verdict"] == "MATCH" else 1


if __name__ == "__main__":
    raise SystemExit(main())
