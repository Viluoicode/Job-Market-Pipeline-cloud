# Deployment evidence

This directory stores dated, machine-readable acceptance records. These files prove what was
observed at a specific time; they are not a live status page.

| File | Purpose |
| --- | --- |
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
