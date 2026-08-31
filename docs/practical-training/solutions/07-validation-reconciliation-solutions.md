# Solutions — 07 Validation and reconciliation workbook

Read these only after attempting [the workbook](../07-validation-reconciliation.md). These answers describe checked-in automation, templates, and V1 scope. They do not claim an infrastructure E2E run.

## VR-01 — False PASS from a native exit code

**Correct answer:** The stage is `FAILED`, with `exit_code: 7`; it is never `PASSED` merely because PowerShell did not throw on native-process invocation. In PowerShell, a native executable’s termination result is exposed through `$LASTEXITCODE`, rather than necessarily as a terminating PowerShell error. A naive `try/catch` that does not explicitly inspect that code can complete normally and incorrectly record success.

**Step-by-step evidence reasoning:** `Invoke-RecordedCommand` creates per-stage stdout/stderr files and records the intended current working directory. It resets `$global:LASTEXITCODE` to avoid inheriting an earlier command’s code, changes to `WorkingDirectory`, runs the script block with output redirects, captures `$global:LASTEXITCODE`, and throws when it is nonzero. The catch captures the failure, appends error text to stderr, and the finally block restores the prior directory and appends both logs to `commands.log`. A failure becomes an `Add-StageResult` entry with status `FAILED`, evidence path, exit code, and message. `validation-summary.json` contains the resulting stage list.

The self-test is the executable specification: `SELFTEST-NATIVE-FAILURE` executes a native exit `7` and asserts `FAILED` plus code `7`; separate cases protect success, a PowerShell throw, `BLOCKED`, and `NOT_RUN`. This is why the runner itself needs tests, not just the commands it executes.

**Relevant actual runner/matrix logic:** The runner’s `ValidateSet` supports `PASSED`, `FAILED`, `BLOCKED`, and `NOT_RUN`. Its terminal output defines `PASSED` as exit code 0 with evidence. The historical bug fixed here was native nonzero exit being treated as PASS because PowerShell did not throw; checking `$LASTEXITCODE` and retaining stdout/stderr/working-directory evidence fixes framework correctness. It is not a production outage record.

**Correctness invariant:** A passed native-command stage has recorded evidence and exit code 0; a native nonzero exit cannot be converted into PASS by wrapper control flow.

**Common wrong answer:** “No PowerShell exception means success,” or “only stderr matters.” Some native failures write useful information to stdout, and a process exit code is independent evidence.

**Production consequence:** False PASS evidence can close a validation stage without a successful command, concealing broken deployments or quality checks.

**Suggested regression/contract test:** Run the runner’s self-test and assert `SELFTEST-NATIVE-FAILURE.actual_status == FAILED` and `exit_code == 7`, with both log paths present; source-level tests also assert the explicit exit-code check, working-directory change, and self-test names. Static runner behavior is `MDEP IMPLEMENTED`/`MDEP OFFLINE TESTED`; real E2E validation is `MDEP RUNTIME DEFERRED`.

**Interview-ready English answer:** “For native tools, PowerShell may not throw on a nonzero exit, so try/catch alone can create a false pass. Our evidence wrapper captures `$LASTEXITCODE`, stdout, stderr, and working directory; only exit code zero with evidence is PASSED. The runner self-tests an exit-7 command so that rule is protected.”

**Senior follow-up discussion:** Add command timeout, structured evidence metadata, artifact checksums, immutable run IDs, and CI publication rules. Those are **GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED** unless separately evidenced.

## VR-02 — Semantic reconciliation trace

**Correct answer:** The counts do not prove failure and do not prove correctness. Bronze’s 1,000 records are append/history evidence and can include replayed, duplicate, malformed, old-schema, or quarantined input. Silver’s 900 rows represent validated, deduplicated current business state, normally one row per business key. Gold’s 30 daily-mart rows represent a different aggregation grain—order date plus currency—not 900 order-grain rows.

**Step-by-step reconciliation reasoning:** Start with a bounded logical interval and record run ID. R01 compares current source order business keys, counts, and key-level attributes with Silver `core_orders`; R02 does the analogous payment comparison. R03 anti-joins Silver orders and Gold `fct_orders`, retaining excluded-key explanations; R04 reconciles payments and records orphan exceptions. R05 recomputes the daily order-date/currency aggregation from `fct_orders` and compares it with `mart_daily_sales`. R06 lists/counts missing DKK-rate exceptions; R07 lists/counts payment `order_id`s absent from facts; R08 checks selected source deletes are absent downstream or have documented retention semantics; R09 requires zero duplicate current-state keys; R10 evaluates required-field null counts or documented exceptions.

An anti-join exposes the actual keys in `source_not_silver`, `silver_not_source`, `silver_not_gold`, or `gold_not_silver`; equal totals could conceal two offsetting missing/extra populations. R08 is not a freshness check: a recent source timestamp does not show whether a current-state delete propagated. R05 compares values only after transforming both sides to the same aggregation grain.

**Relevant actual runner/matrix/reconciliation logic:** The reconciliation README is authoritative for R01–R10. `spark-iceberg.sql` supplies R01/R02 business-key anti-joins plus R08/R09/R10 examples; `snowflake-dbt.sql` supplies R03–R05, R07, R09, and R10 examples. Before any future execution, review each template against its current model contract. In particular, the checked-in R05 Snowflake template refers to `amount`/`gross_amount`, while the current `fct_orders` and `mart_daily_sales` models use `order_total`/`gross_sales`; that is a pre-execution compatibility check to resolve, not evidence that the query passed.

**Correctness invariant:** Reconciliation compares intended business truth at an explicitly declared scope and grain, preserves exception keys, and records a decision; raw cross-layer count equality is neither expected nor sufficient.

**Common wrong answer:** “1,000 must equal 900 must equal 30,” or “same totals prove all keys agree.” This ignores history/current-state/aggregate semantics and hides offsetting errors.

**Production consequence:** Row-count-only controls can produce false failures for correct transformations and false passes for mismatched keys or wrong aggregates.

**Suggested regression/contract test:** Assert the reconciliation guide retains R01–R10 and the required query/run ID/count/exception/decision evidence requirements; add a future compatibility test that parses the R05 source fields against the Gold model contract before executing it. Cross-system query execution and run-specific evidence are `MDEP RUNTIME DEFERRED`.

**Interview-ready English answer:** “I do not reconcile Bronze, Silver, and Gold with one raw count. Bronze is history, Silver is current state, and the mart is aggregated. I first compare business keys with anti-joins, then test duplicates, nulls, deletes, FX and orphan exceptions, and finally recompute facts at the mart’s own grain. Every check retains scope, counts, exception keys, and a decision.”

**Senior follow-up discussion:** Define source-of-truth rules for late-arriving data, exception thresholds, owner/expiry policy, and automated escalation. A reconciliation failure should result in preserved diagnostic evidence, not automatic deletion or hidden normalization.

## VR-03 — Historical BLOCKED versus final runtime-deferred status

**Correct answer:** `BLOCKED` in the matrix is a historical runtime-evidence status: the physical check could not run because required capability was unavailable. It is not a claim that the implemented correctness failed. `NOT_RUN` in the runner means an invocation intentionally did not attempt a stage—for example, without `-RunRuntime`. `FAILED` means a check executed and did not meet its expected outcome; `PASSED` means exit code 0 plus evidence. `MDEP RUNTIME DEFERRED` is not an evidence-row status at all: it is the final-V1 scope/closure statement that physical integration is excluded from V1 acceptance while remaining visible for V1.x.

| Term | Kind | Meaning in this repository | Does it prove runtime success? |
| --- | --- | --- | --- |
| `PASSED` | evidence status | executed check completed with required evidence; runner command stage requires exit code 0 | only for that evidenced check |
| `FAILED` | evidence status | executed check/command failed or returned nonzero | no |
| `BLOCKED` | evidence status | required runtime capability/environment was unavailable | no |
| `NOT_RUN` | evidence status | current invocation intentionally did not attempt the stage | no |
| `MDEP RUNTIME DEFERRED` | V1 scope/closure statement | physical integration retained for a future V1.x lab, not a V1 implementation blocker | no |

**Step-by-step evidence/scope reasoning:** The validation matrix preserves `BLOCKED` for its historical physical evidence plan and names blockers such as unavailable Docker, Java/Spark, AWS/S3, dbt/Snowflake, or cross-system endpoints. The 2026-08-30 baseline and amended V1 scope explicitly change final closure criteria: code/configuration, deterministic/offline tests, reconciliation logic, failure reasoning, documentation, and interview readiness are required; unexecuted physical infrastructure work remains in the runtime-debt register. The policy permits a Story to be implementation complete, offline/static validated, runtime deferred, and portfolio ready.

Historical facts must not be rewritten. Replacing a `BLOCKED` row with `PASSED` would fabricate execution evidence. Calling it runtime validated would likewise overclaim. Correct README/interview wording is: “The V1 implementation and offline/static contracts are complete; physical Airflow, Spark/Iceberg, Debezium/Kafka, Flink/S3, Snowflake/dbt, and E2E runs are runtime deferred to a V1.x hands-on validation phase, not represented as passed.”

**Relevant actual runner/matrix/reconciliation logic:** The matrix declares exactly four evidence statuses. The runner self-test protects `BLOCKED` and `NOT_RUN` semantics, while closure documents define their relationship to V1 completion. The runtime-debt register records RD-08 through RD-13 and explicitly says no row is runtime-validated.

**Correctness invariant:** Closure language must preserve historical execution status and clearly separate implemented/offline-tested work from unexecuted physical runtime acceptance.

**Common wrong answer:** “BLOCKED means V1 failed,” “deferred means passed,” or “mark all historical blockers as PASSED once scope changes.” Each confuses evidence status with portfolio closure policy.

**Production consequence:** False status communication undermines auditability, interview credibility, and the ability to prioritize the real V1.x runtime work.

**Suggested regression/contract test:** Assert the matrix retains the allowed status vocabulary and its blocked rows/evidence fields; assert closure documents retain `RUNTIME DEFERRED` language and the runtime-debt register retains RD-08 through RD-13 without a runtime-valid claim. These textual/static contracts are `MDEP OFFLINE TESTED`; infrastructure execution remains `MDEP RUNTIME DEFERRED`.

**Interview-ready English answer:** “The matrix’s BLOCKED rows are historical evidence that physical checks could not run on the available host, not failed implementation. V1 was formally closed on implemented code, offline validation, documentation, and reconciliation design, while all physical integrations remain explicitly runtime deferred to V1.x. I never call deferred work runtime validated or rewrite BLOCKED to PASS.”

**Senior follow-up discussion:** A future V1.x validation plan should retain the original matrix as baseline, create new run IDs/evidence bundles, record actual PASS/FAIL results, and reconcile each runtime-debt item without changing historical records.
