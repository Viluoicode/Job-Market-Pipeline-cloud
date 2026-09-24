# Deployment evidence

This directory stores dated, machine-readable acceptance records. These files prove what was
observed at a specific time; they are not a live status page.

| File | Purpose |
| --- | --- |
| `deployment-20260915.json` | Manual end-to-end acceptance after the ingestion and lifecycle upgrade |
| `scheduled-run-20260916.json` | First verified EventBridge-triggered daily execution |
| `monitoring-20260917.json` | Monitoring, metrics, alarms, tests, and notification-path acceptance |

Add a new dated record for a new milestone. Do not rewrite a historical observation, and never
include credentials, personal email addresses, tokens, or sensitive payloads.
