"""Measure recent pipeline runs: stage timing, Glue DPU and cost, row counts, dashboard scan.

Read-only except for one Athena query (the dashboard's own fact query, to measure its scan) and,
with --billed, one Cost Explorer request ($0.01). Prices come from the AWS Pricing API at run time.

  python scripts/run_metrics.py --executions 7 --output docs/evidence/run-metrics-<date>.json

It measures the latest successful scheduled runs (add --include-manual for hand-started ones).
Cost Explorer bills per UTC day, so a day's Glue bill is divided by every execution started that
day, manual and failed ones included, before it is reported per run.
"""
import argparse
import json
import re
import statistics
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STAGES = ["Ingest", "BronzeToSilver", "SilverToGold"]
# EventBridge names each execution it starts "<uuid>_<uuid>"; start-execution by hand gives one uuid.
SCHEDULED_NAME = re.compile(r"^[0-9a-f-]{36}_[0-9a-f-]{36}$")


def is_scheduled(name):
    return bool(SCHEDULED_NAME.match(name))


def runs_per_day(executions):
    """Executions per UTC day of their start. Cost Explorer bills a day for all of them,
    scheduled or manual, succeeded or failed."""
    counts = {}
    for execution in executions:
        day = execution["startDate"].astimezone(timezone.utc).date().isoformat()
        counts[day] = counts.get(day, 0) + 1
    return counts


def stage_durations(events):
    """Seconds per pipeline stage from a Step Functions execution history.

    A stage spans its first *StateEntered to its last *StateExited event. The crawl spans
    StartCrawler entered to BuildCompletion entered, i.e. start plus every poll until verified;
    runs after the Iceberg switch (Nov 2026) have no crawler, so their Crawl is None.
    """
    first_entered, last_exited = {}, {}
    for event in events:
        if event["type"].endswith("StateEntered"):
            first_entered.setdefault(event["stateEnteredEventDetails"]["name"], event["timestamp"])
        elif event["type"].endswith("StateExited"):
            last_exited[event["stateExitedEventDetails"]["name"]] = event["timestamp"]

    def span(start, end):
        if start is None or end is None:
            return None
        return (end - start).total_seconds()

    durations = {name: span(first_entered.get(name), last_exited.get(name)) for name in STAGES}
    durations["Crawl"] = span(first_entered.get("StartCrawler"), first_entered.get("BuildCompletion"))
    return durations


def find_job_runs(glue, job_names, run_id, max_pages=5):
    """Glue job runs started by one execution: the state machine passes its name as the run id."""
    found = []
    for job in job_names:
        token, pages = None, 0
        while pages < max_pages:
            response = glue.get_job_runs(JobName=job, MaxResults=200, **({"NextToken": token} if token else {}))
            pages += 1
            for run in response.get("JobRuns", []):
                args = run.get("Arguments", {})
                if run_id in (args.get("--run-id"), args.get("--run_id")):
                    seconds = run.get("ExecutionTime", 0)
                    # Spark runs report DPUSeconds; Python Shell runs only report capacity.
                    dpu = run.get("DPUSeconds") or seconds * run.get("MaxCapacity", 0)
                    found.append({"job": job, "job_run_id": run["Id"], "state": run["JobRunState"],
                                  "execution_seconds": seconds, "dpu_seconds": dpu})
            token = response.get("NextToken")
            if not token or any(r["job"] == job for r in found):
                break
    return found


def glue_price(pricing, region, usage_suffix):
    """On-demand USD per DPU-hour for an exact Glue usage type, e.g. 'ETL-DPU-Hour'."""
    response = pricing.get_products(
        ServiceCode="AWSGlue", MaxResults=100,
        Filters=[{"Type": "TERM_MATCH", "Field": "regionCode", "Value": region}])
    for item in response["PriceList"]:
        product = json.loads(item)
        usagetype = product["product"]["attributes"].get("usagetype", "")
        if usagetype.split("-", 1)[-1] != usage_suffix:
            continue
        for term in product["terms"]["OnDemand"].values():
            for dimension in term["priceDimensions"].values():
                return {"usd_per_dpu_hour": float(dimension["pricePerUnit"]["USD"]),
                        "usagetype": usagetype, "description": dimension["description"]}
    raise LookupError(f"No AWS Glue price for {usage_suffix} in {region}")


def cost_usd(dpu_seconds, usd_per_dpu_hour):
    return round(dpu_seconds / 3600 * usd_per_dpu_hour, 6)


def billed_glue_by_day(ce, days):
    """Actual (pre-credit) Glue cost per UTC day and usage type from Cost Explorer."""
    start, end = min(days), (datetime.fromisoformat(max(days)) + timedelta(days=1)).date().isoformat()
    response = ce.get_cost_and_usage(
        TimePeriod={"Start": start, "End": end}, Granularity="DAILY", Metrics=["UnblendedCost"],
        GroupBy=[{"Type": "DIMENSION", "Key": "USAGE_TYPE"}],
        Filter={"And": [{"Dimensions": {"Key": "SERVICE", "Values": ["AWS Glue"]}},
                        {"Not": {"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Credit", "Refund"]}}}]})
    billed = {}
    for day in response["ResultsByTime"]:
        groups = {g["Keys"][0]: round(float(g["Metrics"]["UnblendedCost"]["Amount"]), 4) for g in day["Groups"]}
        billed[day["TimePeriod"]["Start"]] = {k: v for k, v in groups.items() if v}
    return billed


def _duration(seconds):
    if seconds is None:
        return "n/a"
    minutes, secs = divmod(round(seconds), 60)
    return f"{minutes}m {secs:02d}s"


def _median(values):
    values = [v for v in values if v is not None]
    return statistics.median(values) if values else None


def to_markdown(report):
    runs = report["executions"]
    latest = report["latest"]
    kind = "scheduled runs" if report.get("scheduled_only", True) else "runs (scheduled and manual)"
    lines = [f"Last {len(runs)} successful {kind}, {report['region']}, medians; "
             f"generated {report['generated_at'][:10]}.", "",
             "| Metric | Value |", "| --- | --- |",
             f"| End-to-end | {_duration(_median(r['total_seconds'] for r in runs))} |"]
    for stage in STAGES + ["Crawl"]:
        median = _median(r["stages"].get(stage) for r in runs)
        if median is not None:                       # no crawler row once runs have none
            lines.append(f"| {stage} | {_duration(median)} |")
    lines += [
        f"| Glue job DPU-seconds | {_median(r['glue_dpu_seconds'] for r in runs):,.0f} |",
        f"| Glue job cost (estimate) | ${_median(r['glue_cost_usd'] for r in runs):.4f} |",
        f"| Glue price | ${report['price']['usd_per_dpu_hour']}/DPU-hour ({report['price']['usagetype']}) |",
        f"| Bronze rows (latest) | {latest['bronze_rows']:,} from "
        f"{latest['successful_boards']}/{latest['boards']} boards |",
        f"| Silver lifecycle rows | {latest['silver_rows']:,} |",
        f"| Gold postings published | {latest['gold_rows']:,} |",
        f"| Dashboard query scan | {latest['dashboard_scan_bytes'] / 2**20:.1f} MiB |",
    ]
    # A day's bill covers every run that day; a day whose run count is unknown cannot be split.
    counts = report.get("runs_per_day", {})
    days = [(bill, counts[day]) for day, bill in report.get("billed", {}).items() if bill and counts.get(day)]
    if days:
        def billed(suffix):
            return _median(sum(v for k, v in bill.items() if k.endswith(suffix)) / n for bill, n in days)
        lines.append(f"| Glue billed per run: jobs (Cost Explorer) | ${billed('ETL-DPU-Hour'):.4f} |")
        if billed("Crawler-DPU-Hour"):
            lines.append(f"| Glue billed per run: crawler (Cost Explorer) | ${billed('Crawler-DPU-Hour'):.4f} |")
        split = ", ".join(f"{day}: day's bill divided by its {counts[day]} runs"
                          for day, bill in sorted(report["billed"].items()) if bill and counts.get(day))
        lines += ["", f"Billed per run, median over UTC days ({split})."]
    return "\n".join(lines) + "\n"


def _terraform_output(name):
    return subprocess.run(["terraform", f"-chdir={ROOT / 'infra'}", "output", "-raw", name],
                          check=True, capture_output=True, text=True).stdout.strip()


def collect(session, *, executions, region, with_billed, include_manual=False):
    sys.path.insert(0, str(ROOT / "dashboard"))
    sys.path.insert(0, str(ROOT / "glue" / "jobs"))
    from data_access import query_fact
    from pipeline_contract import read_json

    sfn, glue, s3 = session.client("stepfunctions"), session.client("glue"), session.client("s3")
    state_machine = _terraform_output("state_machine_arn")
    jobs = [_terraform_output(n) for n in ("ingestion_job", "bronze_to_silver_job", "silver_to_gold_job")]
    lake = _terraform_output("lake_bucket")
    price = glue_price(session.client("pricing", region_name="us-east-1"), region, "ETL-DPU-Hour")

    every = [e for page in sfn.get_paginator("list_executions").paginate(stateMachineArn=state_machine)
             for e in page["executions"]]                                   # newest first
    listed = [e for e in every if e["status"] == "SUCCEEDED"
              and (include_manual or is_scheduled(e["name"]))][:executions]
    runs = []
    for execution in listed:
        events = []
        for page in sfn.get_paginator("get_execution_history").paginate(executionArn=execution["executionArn"]):
            events.extend(page["events"])
        job_runs = find_job_runs(glue, jobs, execution["name"])
        dpu = sum(r["dpu_seconds"] for r in job_runs)
        # UTC: the 01:00 UTC+7 run belongs to the previous UTC day, which is how AWS bills it.
        runs.append({"name": execution["name"],
                     "trigger": "scheduled" if is_scheduled(execution["name"]) else "manual",
                     "start": execution["startDate"].astimezone(timezone.utc).isoformat(),
                     "total_seconds": (execution["stopDate"] - execution["startDate"]).total_seconds(),
                     "stages": stage_durations(events), "glue_job_runs": job_runs,
                     "glue_dpu_seconds": dpu, "glue_cost_usd": cost_usd(dpu, price["usd_per_dpu_hour"])})

    completion = read_json(s3, lake, "state/pipeline/latest.json")
    silver = read_json(s3, lake, "state/silver/latest.json")
    manifest = read_json(s3, lake, f"control/ingestion/run_id={completion['run_id']}/manifest.json")
    records, query = query_fact(session, _terraform_output("gold_database"), _terraform_output("athena_workgroup"),
                                completion["snapshot_date"],
                                completion["iceberg_snapshots"]["fact_job_posting"]["snapshot_id"])
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "region": region,
        "method": "Step Functions history, Glue job runs, AWS Pricing API, published state, dashboard Athena query",
        "price": price, "scheduled_only": not include_manual, "executions": runs,
        "latest": {"snapshot_date": completion["snapshot_date"], "run_id": completion["run_id"],
                   "bronze_rows": manifest["total_records"], "successful_boards": manifest["successful_boards"],
                   "boards": len(manifest["boards"]), "silver_rows": silver["row_count"],
                   "gold_rows": len(records), "dashboard_scan_bytes": query["bytes_scanned"],
                   "dashboard_query_id": query["query_id"]},
    }
    if with_billed:
        days = sorted({r["start"][:10] for r in runs})
        report["billed"] = billed_glue_by_day(session.client("ce", region_name="us-east-1"), days)
        per_day = runs_per_day(every)
        report["runs_per_day"] = {day: per_day.get(day, 0) for day in days}
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--executions", type=int, default=7)
    parser.add_argument("--region", default="ap-southeast-1")
    parser.add_argument("--billed", action="store_true", help="add Cost Explorer actuals ($0.01 per run)")
    parser.add_argument("--include-manual", action="store_true",
                        help="also measure runs started by hand (default: scheduled runs only)")
    parser.add_argument("--output", help="write the JSON report here")
    args = parser.parse_args(argv)
    import boto3
    report = collect(boto3.Session(region_name=args.region), executions=args.executions,
                     region=args.region, with_billed=args.billed, include_manual=args.include_manual)
    if args.output:
        Path(args.output).write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    print(to_markdown(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
