"""Scheduled Lambda: read publication metadata and emit health/coverage metrics.

No credentials, dataset writes, execution starts or notification recipients are accepted
from the event. CloudWatch alarms own notification and recovery transitions.
"""
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "glue" / "jobs"))
from check_freshness import assess
from pipeline_contract import read_json


def inspect_health(s3, bucket, *, max_age_hours=26, now=None):
    completion = read_json(s3, bucket, "state/pipeline/latest.json", optional=True)
    silver = read_json(s3, bucket, "state/silver/latest.json", optional=True)
    if not isinstance(completion, dict) or not completion.get("run_id"):
        return {"healthy": False, "reason": "missing_or_invalid_completion"}
    run_id = completion["run_id"]
    from pipeline_contract import silver_path
    silver_path(bucket, run_id)  # Reject malformed run IDs before constructing an S3 key.
    manifest = read_json(s3, bucket, f"control/ingestion/run_id={run_id}/manifest.json", optional=True)
    result = assess(completion, silver, manifest, now=now, max_age_hours=max_age_hours)
    if isinstance(manifest, dict) and manifest.get("status") == "SUCCEEDED":
        boards = manifest.get("boards", [])
        statuses = [board.get("status") for board in boards]
        if not statuses or any(status not in {"SUCCEEDED", "FAILED"} for status in statuses):
            return {"healthy": False, "reason": "invalid_board_coverage"}
        # Incomplete pagination is not an API failure. Report it separately in the log.
        result["source_success_ratio"] = statuses.count("SUCCEEDED") / len(statuses)
    return result


def run_check(s3, cloudwatch, *, bucket, project, namespace, max_age_hours=26, now=None):
    try:
        result = inspect_health(s3, bucket, max_age_hours=max_age_hours, now=now)
    except Exception as exc:
        # Missing/malformed/inaccessible metadata is unhealthy, never a silent skipped check.
        result = {"healthy": False, "reason": "metadata_read_or_validation_error",
                  "error_type": type(exc).__name__}
    dimensions = [{"Name": "Project", "Value": project}]
    metrics = [{"MetricName": "Healthy", "Value": int(result["healthy"]),
                "Unit": "Count", "Dimensions": dimensions}]
    if "source_success_ratio" in result:
        metrics.append({"MetricName": "SourceSuccessRatio", "Value": result["source_success_ratio"],
                        "Unit": "None", "Dimensions": dimensions})
    # Let publication failures fail the Lambda invocation. Missing Healthy data also alarms.
    cloudwatch.put_metric_data(Namespace=namespace, MetricData=metrics)
    print(json.dumps(result, sort_keys=True))
    return result


def handler(event, context):
    import boto3
    print(json.dumps({"trigger": event.get("source", "manual"),
                      "request_id": getattr(context, "aws_request_id", None)}))
    return run_check(boto3.client("s3"), boto3.client("cloudwatch"),
                     bucket=os.environ["LAKE_BUCKET"], project=os.environ["PROJECT"],
                     namespace=os.environ["METRIC_NAMESPACE"],
                     max_age_hours=float(os.environ.get("MAX_AGE_HOURS", "26")))
