# Documentation map

This directory separates system design, operations, learning material, and point-in-time evidence.
Keeping those concerns separate prevents deployment history and tutorials from making the project
README difficult to use.

| Document | Purpose | Update when |
| --- | --- | --- |
| [Architecture](architecture.md) | Current system boundaries, data flow, contracts, lifecycle, and design decisions | The deployed design or a core invariant changes |
| [Operations](operations.md) | Deploy, run, verify, pause, recover, and safely retire the pipeline | An operator procedure or Terraform workflow changes |
| [Monitoring](monitoring.md) | Health model, alarms, notification setup, and AWS data inspection | Monitoring behavior or an alarm changes |
| [Learning guide](LEARN.md) | Concepts and a guided reading path through the implementation | The implementation changes how a concept is taught |
| [Sample results](sample_results.md) | Reproducible, point-in-time Athena results and interpretation limits | A new result set is intentionally recorded |
| [Evidence](evidence/README.md) | Machine-readable deployment and acceptance records | A deployment milestone is formally accepted |
| [Screenshots](screenshots/README.md) | Visual evidence checklist for the workshop and demo | New review-ready screenshots are captured |

`dashboard.html` is a historical static serving prototype. It is not the production interface and
is not part of the current ingestion, freshness, lifecycle, or monitoring scope.

## Documentation rules

- `README.md` is the repository entry point, not an operations log.
- Current design belongs in `architecture.md`; commands and incident steps belong in
  `operations.md`.
- Time-sensitive numbers must include an observation date and link to evidence.
- Evidence files are append-only records. Correct the describing document instead of rewriting a
  past observation.
- Never publish credentials, personal email addresses, Terraform state, or unredacted secrets.
