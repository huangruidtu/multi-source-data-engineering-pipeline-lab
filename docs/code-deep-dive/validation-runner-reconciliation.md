# Code Deep-Dive: validation runner and reconciliation templates
**Source of truth:** [`scripts/validate-mdep-13-e2e.ps1`](../../scripts/validate-mdep-13-e2e.ps1) and [`validation/reconciliation/`](../../validation/reconciliation/).
## Read beside
- **Source:** [`validate-mdep-13-e2e.ps1`](../../scripts/validate-mdep-13-e2e.ps1), [`reconciliation README`](../../validation/reconciliation/README.md)
- **Tests:** [`tests/test_mdep13_validation_framework.py`](../../tests/test_mdep13_validation_framework.py)
- **Architecture:** [`docs/finalization/failure-and-recovery-reasoning.md`](../finalization/failure-and-recovery-reasoning.md)
- **Interview topics:** [`validation-matrix.md`](validation-matrix.md)
## 1. Why this file exists
The runner records validation outcomes honestly; reconciliation templates compare semantic business sets across layers rather than blindly comparing counts.
## 2. Where it sits in the architecture
It is the evidence/reliability layer after batch, CDC, Silver, Gold, and dbt paths.
## 3. Inputs / outputs / state
Parameters choose runtime/self-test/run ID/roots/bucket. Outputs are timestamped evidence directories, stdout/stderr logs, and `validation-summary.json`. Reconciliation inputs are bounded source/Silver/Gold query contexts.
## 4. Important symbols
`Invoke-RecordedCommand`, `Add-StageResult`, `Write-ValidationSummary`, `-SelfTest`, `-RunRuntime`, R01–R10.
## 5. Execution flow
The script creates evidence directories, preflights capability, runs static tests if possible, records runtime stages only when explicitly requested/capable, and writes summary. Self-test intentionally exercises success, native nonzero failure, PowerShell throw, blocked, and not-run statuses.
## 6. Function-by-function walkthrough
`Invoke-RecordedCommand` resets `LASTEXITCODE`, changes working directory, redirects stdout/stderr, treats nonzero native exit as failure, restores location, and records status/evidence. This prevents the earlier false-PASS risk of ignoring native exit codes. The runtime branch gates Docker/Spark/dbt/Snowflake work on preflight. Reconciliation README defines correct semantic comparisons: Bronze history differs from Silver current state; Gold can filter/aggregate. Spark SQL templates anti-join source/current Silver; Snowflake templates compare facts/marts, duplicates/nulls, and orphans.
## 7. Critical code-block reasoning
`PASSED` means exit code zero **and** evidence log, not merely no PowerShell exception. The runner distinguishes blocked capability from deliberate not-run invocation. Reconciliation uses anti-joins and declared grain because equal raw counts can hide wrong business keys, while unequal layer counts can be correct due to history/aggregation.
## 8. Correctness invariants
- Every recorded command has separate stdout/stderr evidence.
- Native nonzero exit cannot become PASS.
- Runtime is opt-in (`-RunRuntime`).
- Reconciliation checks business keys/attributes/exceptions, not only counts.
## 9. Failure behavior
Command error captures stderr and FAILED status; unavailable requirements become BLOCKED. Self-test validates those classifications. SQL templates require explicit run boundaries and do not embed credentials.
## 10. Tests that protect the behavior
MDEP-13 tests assert runner exit-code/self-test constructs, reconciliation IDs R01–R10, quality-gate categories, secret absence, and matrix debt semantics. **MDEP OFFLINE TESTED.**
## 11. What is not implemented / runtime deferred
**MDEP RUNTIME DEFERRED:** full `-RunRuntime` execution, real evidence from Docker/Spark/Flink/S3/Snowflake/dbt, and actual cross-layer counts.
## 12. Production concepts beyond current code
**GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED:** CI artifact upload, immutable evidence store, test dashboards, scheduled reconciliation, alerting, and approval workflows.
## 13. Common misunderstandings
A generated evidence directory alone is not success. Matching Bronze/Silver/Gold row counts is not a reconciliation strategy. `NOT_RUN` is not `BLOCKED`.
## 14. Interview questions
**Why compare keys with anti-joins instead of only counts?** Counts can match while the wrong records are present, or differ legitimately across history/current/aggregate layers. Anti-joins expose missing/extra business keys and make exceptions reviewable.
## 15. 30-second spoken explanation
“The MDEP-13 runner is evidence-first: it captures command output, respects native exit codes, separates passed/failed/blocked/not-run, and only attempts runtime when asked. Its reconciliation templates compare semantic keys, duplicates, nulls, deletes, facts, and marts rather than assuming every layer should have equal row counts.”
## 16. Senior follow-up discussion
Discuss a production reconciliation service: bounded watermark/run scope, exception ownership, thresholds, late-arrival policy, persisted results, alert severity, and how to prevent a transient endpoint outage from being misreported as data mismatch.
