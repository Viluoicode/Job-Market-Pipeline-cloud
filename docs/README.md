# Documentation map

Project documentation covers purpose, system design, operations, and recorded results.

| Document | Purpose | Update when |
| --- | --- | --- |
| [Proposal](proposal.md) | Canonical problem statement, target users, value proposition, scope, and adoption case | The business problem or intended users change |
| [Architecture](architecture.md) | Current system boundaries, data flow, contracts, lifecycle, and design decisions | The deployed design or a core invariant changes |
| [Architecture drawing guide](architecture-drawing-guide.md) | Learner drawing steps, service/arrow mapping and final diagram review checklist | Diagram preparation or acceptance changes |
| [Operations](operations.md) | Deploy, run, verify, pause, recover, and safely retire the pipeline | An operator procedure or Terraform workflow changes |
| [Monitoring](monitoring.md) | Health model, alarms, notification setup, and AWS data inspection | Monitoring behavior or an alarm changes |
| [Sample results](sample_results.md) | Reproducible, point-in-time Athena results and interpretation limits | A new result set is intentionally recorded |
| [Evidence](evidence/README.md) | Machine-readable deployment and acceptance records | A deployment milestone is formally accepted |

## Documentation rules

- `README.md` is the repository entry point, not an operations log.
- Current design belongs in `architecture.md`; commands and incident steps belong in
  `operations.md`.
- Time-sensitive numbers must include an observation date and link to evidence.
- Evidence files are append-only records. Correct the describing document instead of rewriting a
  past observation.
- Never publish credentials, personal email addresses, Terraform state, or unredacted secrets.
