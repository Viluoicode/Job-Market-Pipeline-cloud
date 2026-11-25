# Deployment evidence

This directory stores dated, machine-readable acceptance records. These files prove what was
observed at a specific time; they are not a live status page.

| File | Purpose |
| --- | --- |
| `iceberg-parity-<date>.json` | Shadow Iceberg Gold vs published Parquet Gold for one snapshot: row counts of all marts, row-level fact differences, verdict (task 5.1 phase A). 22, 24 and 25 Nov match; 23 Nov records the Spark 3.3 overwrite failure fixed before the switch |
| `run-metrics-20261125-iceberg.json` | First run after the Iceberg switch (phase B): stage timing, Glue DPU/cost, row counts, dashboard scan |
| `security-permissions-20261125.json` | IAM allowed/denied simulations and S3 guardrails after the Iceberg switch and crawler removal (60/60) |
| `run-metrics-20261121.json` | Baseline before parallel fetch/Iceberg: stage timing, Glue DPU/cost (Pricing API + Cost Explorer), row counts, dashboard scan for 7 scheduled runs |
| `handoff-check-20261001.json` | Next EventBridge execution after hardening, fresh reader query, dashboard filters and alarm status |
| `security-acceptance-20260930.json` | Manual initial run, failed overwrite attempt and recovery; DQ, reader queries and validation |
| `security-permissions-20260930.json` | Read-only IAM allowed/denied simulations and deployed S3 guardrails |
| `workshop-validation-20260928.json` | Tests, Terraform validation, UI checks and redacted SNS/alarm status |
| `workshop-20260928.json` | Current scheduled pipeline and verified dashboard query |
| `deployment-20260915.json` | Manual end-to-end acceptance after the ingestion and lifecycle upgrade |
| `scheduled-run-20260916.json` | First verified EventBridge-triggered daily execution |
| `monitoring-20260917.json` | Monitoring, metrics, alarms, tests, and notification-path acceptance |

Add a new dated record for a new milestone. Do not rewrite a historical observation, and never
include credentials, personal email addresses, tokens, or sensitive payloads.

The AWS account ID is not published: on 22 November 2026 it was replaced by `REDACTED_ACCOUNT_ID`
in every ARN and bucket name of the files above. Nothing else in those records changed.
`tests/test_evidence.py` keeps every evidence file valid JSON and fails if an account ID appears.
