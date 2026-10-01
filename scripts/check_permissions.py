"""Read-only IAM simulation and S3 guardrail checks. No destructive API probes."""
import argparse
import json
from datetime import datetime, timezone

import boto3


def verify(session, project, lake, scripts, results, database, workgroup):
    iam = session.client("iam")
    account = session.client("sts").get_caller_identity()["Account"]
    region = session.region_name
    role = lambda name: f"arn:aws:iam::{account}:role/{project}-{name}-role"
    obj = lambda bucket, key: f"arn:aws:s3:::{bucket}/{key}"
    checks = []
    def check(name, action, resource, allowed, context=None):
        # The simulator asks for keys from unrelated statements too. Supply a complete
        # context so negative cases cannot pass merely because a condition key is absent.
        context_by_name = {
            "s3:prefix": {"ContextKeyName": "s3:prefix", "ContextKeyValues": ["gold/fact_job_posting/"], "ContextKeyType": "string"},
            "cloudwatch:namespace": {"ContextKeyName": "cloudwatch:namespace", "ContextKeyValues": ["JobMarket/Pipeline" if name == "health-monitor" else "Glue"], "ContextKeyType": "string"},
            "dynamodb:LeadingKeys": {"ContextKeyName": "dynamodb:LeadingKeys", "ContextKeyValues": ["pipeline"], "ContextKeyType": "stringList"},
        }
        context_by_name.update({c["ContextKeyName"]: c for c in context or []})
        response = iam.simulate_principal_policy(
            PolicySourceArn=role(name), ActionNames=[action], ResourceArns=[resource],
            ContextEntries=list(context_by_name.values()),
        )["EvaluationResults"][0]
        actual = response["EvalDecision"]
        passed = (actual == "allowed") if allowed else (actual in {"implicitDeny", "explicitDeny"})
        checks.append({"role": name, "action": action, "resource": resource,
                       "expected": "allowed" if allowed else "denied", "decision": actual,
                       "missing_context": response.get("MissingContextValues", []), "passed": passed})
    for name in ["ingestion", "silver", "gold", "crawler", "health-monitor", "dashboard-reader"]:
        check(name, "s3:PutObject", obj(scripts, "jobs/silver_to_gold.py"), False)
        check(name, "s3:DeleteObject", obj(lake, "bronze/run_id=probe/source=x/board=y/jobs.json"), False)
        check(name, "s3:PutObject", obj(lake, "state/pipeline/latest.json"), False)
    check("ingestion", "s3:PutObject", obj(lake, "bronze/run_id=probe/source=x/board=y/jobs.json"), True)
    check("ingestion", "s3:PutObject", obj(lake, "gold/fact_job_posting/probe.parquet"), False)
    for name, prefix, opposite in [("silver", "silver/runs/probe/jobs/x.parquet", "gold/fact_job_posting/x.parquet"),
                                   ("gold", "gold/fact_job_posting/snapshot_date=2026-09-30/x.parquet", "silver/runs/probe/jobs/x.parquet")]:
        for action in ["s3:PutObject", "s3:DeleteObject"]:
            check(name, action, obj(lake, prefix), True)
            check(name, action, obj(lake, opposite), False)
        check(name, "glue:UpdateJob", f"arn:aws:glue:{region}:{account}:job/{project}-silver-to-gold", False)
    for mart in ["fact_job_posting", "demand_by_role", "role_opportunity"]:
        check("gold", "s3:PutObject", obj(lake, "gold/" + mart + "_$folder$"), True)
    check("gold", "s3:PutObject", obj(lake, "gold/other_$folder$"), False)
    check("silver", "s3:PutObject", obj(lake, "state/silver/latest.json"), True)
    check("crawler", "s3:GetObject", obj(lake, "gold/fact_job_posting/x.parquet"), True)
    check("crawler", "s3:PutObject", obj(lake, "gold/fact_job_posting/x.parquet"), False)
    check("crawler", "s3:GetObject", obj(lake, "bronze/x.json"), False)
    check("sfn", "s3:PutObject", obj(lake, "state/pipeline/latest.json"), True)
    check("sfn", "s3:PutObject", obj(lake, "gold/fact_job_posting/x.parquet"), False)
    check("dashboard-reader", "s3:GetObject", obj(lake, "gold/fact_job_posting/x.parquet"), True)
    check("dashboard-reader", "s3:GetObject", obj(lake, "state/pipeline/latest.json"), True)
    check("dashboard-reader", "s3:GetObject", obj(lake, "bronze/x.json"), False)
    check("dashboard-reader", "s3:PutObject", obj(lake, "gold/fact_job_posting/x.parquet"), False)
    check("dashboard-reader", "s3:PutObject", obj(results, "results/query.csv"), True)
    check("dashboard-reader", "glue:StartJobRun", f"arn:aws:glue:{region}:{account}:job/{project}-ingestion", False)
    check("dashboard-reader", "athena:StartQueryExecution", f"arn:aws:athena:{region}:{account}:workgroup/{workgroup}", True)
    check("dashboard-reader", "athena:StartQueryExecution", f"arn:aws:athena:{region}:{account}:workgroup/primary", False)
    for name, action in [("sfn", "dynamodb:PutItem"), ("dashboard-reader", "dynamodb:GetItem")]:
        for key in ["pipeline", "another-key"]:
            check(name, action, f"arn:aws:dynamodb:{region}:{account}:table/{project}-pipeline-lock", key == "pipeline",
                  [{"ContextKeyName": "dynamodb:LeadingKeys", "ContextKeyValues": [key], "ContextKeyType": "stringList"}])
    bucket_checks = []
    s3 = session.client("s3")
    for bucket in [lake, scripts, results]:
        policy = json.loads(s3.get_bucket_policy(Bucket=bucket)["Policy"])
        block = s3.get_public_access_block(Bucket=bucket)["PublicAccessBlockConfiguration"]
        ownership = s3.get_bucket_ownership_controls(Bucket=bucket)["OwnershipControls"]["Rules"]
        statements = policy["Statement"]
        transport = any(st.get("Effect") == "Deny" and st.get("Principal") == "*"
                        and st.get("Condition", {}).get("Bool", {}).get("aws:SecureTransport") == "false"
                        and set(st.get("Resource", [])) == {f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"}
                        for st in statements)
        bucket_checks.append({"bucket": bucket, "https_deny": transport, "public_access_block": block,
                              "ownership": ownership,
                              "versioning": s3.get_bucket_versioning(Bucket=bucket).get("Status", "Disabled"),
                              "passed": transport and all(block.values()) and ownership == [{"ObjectOwnership": "BucketOwnerEnforced"}]})
    return {"checked_at": datetime.now(timezone.utc).isoformat(),
            "method": "IAM identity-policy simulation plus direct S3 configuration reads; not destructive probes or a complete account audit",
            "passed": all(c["passed"] and not c["missing_context"] for c in checks) and all(c["passed"] for c in bucket_checks),
            "checks": checks, "buckets": bucket_checks}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="jobmarket-aws")
    parser.add_argument("--region", default="ap-southeast-1")
    for name in ["lake", "scripts", "results"]:
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--database", default="jobmarket_aws_gold")
    parser.add_argument("--workgroup", default="jobmarket-aws")
    parser.add_argument("--output")
    args = parser.parse_args()
    report = verify(boto3.Session(region_name=args.region), args.project, args.lake, args.scripts,
                    args.results, args.database, args.workgroup)
    if args.output:
        from pathlib import Path
        Path(args.output).write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"passed": report["passed"], "checks": len(report["checks"]),
                      "failures": [c for c in report["checks"] if not c["passed"] or c["missing_context"]]}, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
