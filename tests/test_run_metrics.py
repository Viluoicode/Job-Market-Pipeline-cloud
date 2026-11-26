"""Pure parts of scripts/run_metrics.py: stage timing, Glue run matching, pricing and reporting."""
import importlib.util
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

path = Path(__file__).resolve().parents[1] / "scripts" / "run_metrics.py"
spec = importlib.util.spec_from_file_location("run_metrics", path)
metrics = importlib.util.module_from_spec(spec)
spec.loader.exec_module(metrics)

T0 = datetime(2026, 11, 21, 18, 0, tzinfo=timezone.utc)


def at(seconds):
    return T0 + timedelta(seconds=seconds)


def entered(kind, name, s):
    return {"timestamp": at(s), "type": f"{kind}StateEntered", "stateEnteredEventDetails": {"name": name}}


def exited(kind, name, s):
    return {"timestamp": at(s), "type": f"{kind}StateExited", "stateExitedEventDetails": {"name": name}}


HISTORY = [
    {"timestamp": at(0), "type": "ExecutionStarted"},
    entered("Task", "Ingest", 2), exited("Task", "Ingest", 122),
    entered("Task", "BronzeToSilver", 123), exited("Task", "BronzeToSilver", 300),
    entered("Task", "SilverToGold", 301), exited("Task", "SilverToGold", 420),
    entered("Pass", "MarkCrawlerStart", 421), exited("Pass", "MarkCrawlerStart", 421),
    entered("Task", "StartCrawler", 421), exited("Task", "StartCrawler", 422),
    entered("Task", "GetCrawler", 452), exited("Task", "GetCrawler", 453),
    entered("Task", "GetCrawler", 483), exited("Task", "GetCrawler", 484),   # polled twice
    entered("Pass", "BuildCompletion", 485), exited("Pass", "BuildCompletion", 485),
    {"timestamp": at(540), "type": "ExecutionSucceeded"},
]


def test_stage_durations_from_execution_history():
    assert metrics.stage_durations(HISTORY) == {
        "Ingest": 120.0, "BronzeToSilver": 177.0, "SilverToGold": 119.0,
        "Crawl": 64.0,          # StartCrawler entered -> BuildCompletion entered, across polls
    }


def test_stage_missing_from_history_is_reported_as_none():
    partial = [e for e in HISTORY if e.get("stateEnteredEventDetails", {}).get("name") != "BuildCompletion"]
    assert metrics.stage_durations(partial)["Crawl"] is None


class Glue:
    def __init__(self, runs):
        self.runs = runs

    def get_job_runs(self, JobName, MaxResults, NextToken=None):
        return {"JobRuns": self.runs.get(JobName, [])}


def test_glue_runs_are_matched_by_the_execution_run_id_argument():
    glue = Glue({
        "ingest": [
            {"Id": "jr_other", "Arguments": {"--run-id": "other"}, "ExecutionTime": 50, "MaxCapacity": 0.0625,
             "StartedOn": at(-86400), "JobRunState": "SUCCEEDED"},
            {"Id": "jr_i", "Arguments": {"--run-id": "exec-1"}, "ExecutionTime": 70, "MaxCapacity": 0.0625,
             "StartedOn": at(2), "JobRunState": "SUCCEEDED"},
        ],
        "silver": [{"Id": "jr_s", "Arguments": {"--run_id": "exec-1"}, "ExecutionTime": 141, "DPUSeconds": 283.0,
                    "MaxCapacity": 2.0, "StartedOn": at(123), "JobRunState": "SUCCEEDED"}],
    })
    runs = metrics.find_job_runs(glue, ["ingest", "silver"], "exec-1")
    assert runs == [
        {"job": "ingest", "job_run_id": "jr_i", "state": "SUCCEEDED", "execution_seconds": 70,
         "dpu_seconds": 4.375},                       # Python Shell: ExecutionTime x 0.0625 DPU
        {"job": "silver", "job_run_id": "jr_s", "state": "SUCCEEDED", "execution_seconds": 141,
         "dpu_seconds": 283.0},                       # Spark: Glue reports DPUSeconds
    ]


def price_item(usagetype, usd, description="d"):
    return json.dumps({
        "product": {"attributes": {"usagetype": usagetype, "regionCode": "ap-southeast-1"}},
        "terms": {"OnDemand": {"t": {"priceDimensions": {"p": {
            "unit": "DPU-Hour", "pricePerUnit": {"USD": usd}, "description": description}}}}},
    })


class Pricing:
    def get_products(self, **kwargs):
        self.kwargs = kwargs
        return {"PriceList": [price_item("APS1-ETL-Flex-DPU-Hour", "0.29"),
                              price_item("APS1-ETL-DPU-Hour", "0.4400000000", "$0.44 per DPU-Hour for Glue ETL job"),
                              price_item("APS1-ETL-DPU-Hour-Gen2", "0.308")]}


def test_glue_price_is_read_from_the_pricing_api_not_hard_coded():
    pricing = Pricing()
    price = metrics.glue_price(pricing, "ap-southeast-1", "ETL-DPU-Hour")
    assert price == {"usd_per_dpu_hour": 0.44, "usagetype": "APS1-ETL-DPU-Hour",
                     "description": "$0.44 per DPU-Hour for Glue ETL job"}
    assert {"Type": "TERM_MATCH", "Field": "regionCode", "Value": "ap-southeast-1"} in pricing.kwargs["Filters"]


def test_glue_price_missing_usagetype_fails_loudly():
    with pytest.raises(LookupError):
        metrics.glue_price(Pricing(), "ap-southeast-1", "Crawler-DPU-Hour")


def test_cost_estimate_from_dpu_seconds():
    assert metrics.cost_usd(3600, 0.44) == 0.44
    assert metrics.cost_usd(283 + 250 + 4.375, 0.44) == pytest.approx(0.0657, abs=1e-4)


def test_markdown_report_summarizes_runs():
    report = {
        "generated_at": "2026-11-21T12:00:00+00:00", "region": "ap-southeast-1",
        "price": {"usd_per_dpu_hour": 0.44, "usagetype": "APS1-ETL-DPU-Hour"},
        "executions": [
            {"name": "a", "start": "2026-11-20T18:00:16Z", "total_seconds": 540.0,
             "stages": {"Ingest": 120.0, "BronzeToSilver": 177.0, "SilverToGold": 119.0, "Crawl": 64.0},
             "glue_dpu_seconds": 537.4, "glue_cost_usd": 0.0657},
            {"name": "b", "start": "2026-11-19T18:00:16Z", "total_seconds": 600.0,
             "stages": {"Ingest": 100.0, "BronzeToSilver": 200.0, "SilverToGold": 130.0, "Crawl": 70.0},
             "glue_dpu_seconds": 600.0, "glue_cost_usd": 0.0733},
        ],
        "billed": {"2026-11-19": {"APS1-ETL-DPU-Hour": 0.06, "APS1-Crawler-DPU-Hour": 0.05},
                   "2026-11-20": {"APS1-ETL-DPU-Hour": 0.07, "APS1-Crawler-DPU-Hour": 0.049},
                   "2026-11-21": {}},                              # today's bill is not in yet
        "runs_per_day": {"2026-11-19": 1, "2026-11-20": 1, "2026-11-21": 1},
        "latest": {"snapshot_date": "2026-11-20", "bronze_rows": 8099, "successful_boards": 35,
                   "boards": 36, "silver_rows": 43540, "gold_rows": 8000, "dashboard_scan_bytes": 1048576},
    }
    md = metrics.to_markdown(report)
    assert "| End-to-end | 9m 30s |" in md           # median of 540 s and 600 s
    assert "| Ingest | 1m 50s |" in md
    assert "| Glue job cost (estimate) | $0.0695 |" in md
    assert "| Dashboard query scan | 1.0 MiB |" in md
    assert "8,099" in md and "43,540" in md
    assert "Last 2 successful scheduled runs" in md
    assert "| Glue billed per run: jobs (Cost Explorer) | $0.0650 |" in md
    assert "| Glue billed per run: crawler (Cost Explorer) | $0.0495 |" in md


def test_markdown_omits_crawler_rows_once_runs_have_no_crawler():
    # After the Iceberg switch (Nov 2026) runs have no crawl stage and no crawler bill.
    run = {"name": "x", "start": "2026-11-26T18:00:16Z", "total_seconds": 420.0,
           "stages": {"Ingest": 110.0, "BronzeToSilver": 170.0, "SilverToGold": 125.0, "Crawl": None},
           "glue_dpu_seconds": 500.0, "glue_cost_usd": 0.061}
    report = {"generated_at": "2026-11-27T00:00:00+00:00", "region": "ap-southeast-1",
              "price": {"usd_per_dpu_hour": 0.44, "usagetype": "APS1-ETL-DPU-Hour"}, "executions": [run],
              "billed": {"2026-11-26": {"APS1-ETL-DPU-Hour": 0.06}}, "runs_per_day": {"2026-11-26": 1},
              "latest": {"snapshot_date": "2026-11-26", "bronze_rows": 1, "successful_boards": 36, "boards": 36,
                         "silver_rows": 2, "gold_rows": 3, "dashboard_scan_bytes": 1048576}}
    md = metrics.to_markdown(report)
    assert "| Crawl |" not in md
    assert "crawler" not in md
    assert "| Glue billed per run: jobs (Cost Explorer) | $0.0600 |" in md


# ---- billed cost per run and run triggers -----------------------------------------------------
SCHEDULED = "1dc39c4d-b58f-4c64-f0ab-ebe79733375c_c435bf36-af5c-5073-3cec-2083f31cba48"   # EventBridge
MANUAL = "1d82b466-ef3a-4610-9dc9-09cebcda73d1"                                            # start-execution


def test_scheduled_runs_are_told_apart_from_manual_ones():
    assert metrics.is_scheduled(SCHEDULED)
    assert not metrics.is_scheduled(MANUAL)
    assert not metrics.is_scheduled("acceptance-run_2")


def test_runs_per_day_counts_every_execution_by_its_utc_start():
    # Cost Explorer bills by UTC day, whoever started the run and however it ended.
    ict = timezone(timedelta(hours=7))
    executions = [
        {"name": MANUAL, "status": "SUCCEEDED", "startDate": datetime(2026, 11, 25, 12, 48, tzinfo=ict)},
        {"name": MANUAL, "status": "SUCCEEDED", "startDate": datetime(2026, 11, 25, 22, 12, tzinfo=ict)},
        {"name": SCHEDULED, "status": "FAILED", "startDate": datetime(2026, 11, 26, 1, 0, tzinfo=ict)},
        {"name": SCHEDULED, "status": "SUCCEEDED", "startDate": datetime(2026, 11, 25, 1, 0, tzinfo=ict)},
    ]
    assert metrics.runs_per_day(executions) == {"2026-11-25": 3, "2026-11-24": 1}


def billed_report(billed, runs_per_day, scheduled_only=True):
    run = {"name": SCHEDULED, "start": "2026-11-25T18:00:16Z", "total_seconds": 517.0,
           "stages": {"Ingest": 114.0, "BronzeToSilver": 235.0, "SilverToGold": 167.0, "Crawl": None},
           "glue_dpu_seconds": 623.0, "glue_cost_usd": 0.0762}
    return {"generated_at": "2026-11-26T02:00:00+00:00", "region": "ap-southeast-1",
            "price": {"usd_per_dpu_hour": 0.44, "usagetype": "APS1-ETL-DPU-Hour"}, "executions": [run],
            "billed": billed, "runs_per_day": runs_per_day, "scheduled_only": scheduled_only,
            "latest": {"snapshot_date": "2026-11-25", "bronze_rows": 1, "successful_boards": 36, "boards": 36,
                       "silver_rows": 2, "gold_rows": 3, "dashboard_scan_bytes": 1048576}}


def test_a_days_bill_is_shared_by_every_run_that_day():
    # 25 Nov 2026 (UTC) had four runs; the day's $0.2541 is not one run's cost.
    md = metrics.to_markdown(billed_report(
        {"2026-11-25": {"APS1-ETL-DPU-Hour": 0.2541, "APS1-Crawler-DPU-Hour": 0.0502}}, {"2026-11-25": 4}))
    assert "| Glue billed per run: jobs (Cost Explorer) | $0.0635 |" in md
    assert "| Glue billed per run: crawler (Cost Explorer) | $0.0126 |" in md
    assert "day's bill divided by its 4 runs" in md


def test_a_billed_day_with_no_known_runs_is_left_out():
    md = metrics.to_markdown(billed_report({"2026-11-25": {"APS1-ETL-DPU-Hour": 0.25}}, {}))
    assert "Glue billed per run" not in md


def test_header_says_when_manual_runs_are_included():
    md = metrics.to_markdown(billed_report({}, {}, scheduled_only=False))
    assert "Last 1 successful runs (scheduled and manual)" in md
