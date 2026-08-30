# Code Deep-Dive: `validation/mdep-13-validation-matrix.yml`
**Source of truth:** [`validation/mdep-13-validation-matrix.yml`](../../validation/mdep-13-validation-matrix.yml).
## Read beside
- **Source:** [`mdep-13-validation-matrix.yml`](../../validation/mdep-13-validation-matrix.yml)
- **Tests:** [`tests/test_mdep13_validation_framework.py`](../../tests/test_mdep13_validation_framework.py)
- **Architecture:** [`docs/finalization/offline-validation-coverage.md`](../finalization/offline-validation-coverage.md)
- **Interview topics:** [`validation-runner-reconciliation.md`](validation-runner-reconciliation.md)
## 1. Why this file exists
It records what each MDEP-8–13 runtime exercise would prove and, crucially, why it is not asserted as passed.
## 2. Where it sits in the architecture
It spans batch, Spark, CDC, Flink, Snowflake/dbt, and cross-layer reconciliation as an evidence plan rather than a processing component.
## 3. Inputs / outputs / state
Each item carries id, story, component, command, required environment, expected result, actual status, evidence path, blocker, notes. YAML itself is declarative state-of-knowledge.
## 4. Important symbols
`NOT_RUN`, `PASSED`, `FAILED`, `BLOCKED`; M8 through M13 items; `<run-id>` evidence paths.
## 5. Execution flow
A suitable future runtime lab executes its command, captures evidence, and changes status only when evidence supports it. Current items remain BLOCKED because required hosts/services/credentials were unavailable when the matrix was written.
## 6. Function-by-function walkthrough
This file has no functions. M8 defines DAG/rerun/backfill evidence; M9 Silver/replay proof; M10 connector/snapshot/tombstone/transaction observation; M11 Flink checkpoint/current-state proof; M12 dbt/Snowflake proof; M13 business-key/count reconciliation. Each names required environment and concrete blocker.
## 7. Critical code-block reasoning
`PASSED` requires evidence path without `<run-id>` under the test contract. `BLOCKED` means capability unavailable; `NOT_RUN` means intentionally not attempted. That distinction prevents an absent runtime attempt from being reported as a failed system or a passed one.
## 8. Correctness invariants
- Runtime debt is preserved rather than erased by static tests.
- No passed item lacks concrete evidence location.
- Cross-layer count equality is not assumed as a semantic reconciliation rule.
## 9. Failure behavior
A failed execution must retain logs/evidence and record failure rather than overwrite status. Missing capability stays BLOCKED.
## 10. Tests that protect the behavior
MDEP-13 framework tests assert all prior debt items/fields and prohibit evidence-less PASSED items. **MDEP OFFLINE TESTED.**
## 11. What is not implemented / runtime deferred
**MDEP RUNTIME DEFERRED:** every listed physical exercise; the matrix documents planned proof, not observed result.
## 12. Production concepts beyond current code
**GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED:** automated evidence promotion, centralized test reporting, alert routing, and environment provisioning.
## 13. Common misunderstandings
BLOCKED does not mean implementation absent; it means runtime evidence unavailable. A command listed in YAML has not necessarily run.
## 14. Interview questions
**How do you avoid overclaiming validation?** I separate implemented/static-tested from blocked/not-run runtime exercises and require logged evidence before calling a stage passed.
## 15. 30-second spoken explanation
“The validation matrix is a truthfulness contract. It lists exactly what each runtime exercise would prove, its prerequisites and evidence location, and currently labels unavailable infrastructure as blocked instead of treating configuration as execution.”
## 16. Senior follow-up discussion
Describe an evidence lifecycle: immutable run IDs, captured commands/logs, reviewer decision, and explicit status transition—not editing a dashboard because a component was deployed.
