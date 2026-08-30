# V1 status audit

## Current final-V1 status — 2026-08-30

Under the formal scope amendment, final V1 means **implemented, offline/static
validated, documented/designed, runtime deferred**. Jira MDEP-6 through MDEP-13,
MDEP-54 through MDEP-60, and Epics MDEP-1 through MDEP-5 are Done. Physical
integration remains a V1.x lab and is not claimed as runtime validated.

## Historical pre-amendment / pre-reconciliation snapshot

The table below is retained to explain why runtime-oriented items once remained
open. It is **not the current Jira state** and must not be used as a completion
report.

| Story / Epic | Historical Jira status | Historical runtime classification | Historical reason |
| --- | --- | --- | --- |
| MDEP-6 / MDEP-1 | Done | not runtime-dependent | Contract implementation was already complete. |
| MDEP-7 / MDEP-1 | In Review | RUNTIME_PARTIAL | Source runtime exercises had not run. |
| MDEP-8 through MDEP-13 | In Progress | RUNTIME_BLOCKED | Physical runtime evidence was unavailable on the host. |

The old status model required physical runtime acceptance. The Charter amendment
superseded that requirement for V1 while preserving the unexecuted work as
explicit V1.x runtime-deferred validation.
