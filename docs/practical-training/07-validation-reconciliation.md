# 07 — Validation and reconciliation workbook

Attempt this file before [the matching solutions](solutions/07-validation-reconciliation-solutions.md). Record work in a copy of [the session template](training-records/07-validation-reconciliation-session-template.md). This workbook uses the checked-in matrix, scripts, reconciliation templates, and final V1 scope; it does not supply infrastructure runtime evidence.

## VR-01 — False PASS from a native exit code

- **Difficulty:** Senior
- **Task type:** AUTOMATION INCIDENT / EVIDENCE TRACE / REGRESSION TEST DESIGN
- **Source files/artifacts to inspect:** `scripts/validate-mdep-13-e2e.ps1` (`Invoke-RecordedCommand`, `Invoke-RunnerSelfTest`); `scripts/preflight-mdep-13.ps1`; `tests/test_mdep13_validation_framework.py`; [validation-runner deep-dive](../code-deep-dive/validation-runner-reconciliation.md).
- **Scenario:** A native command exits with code `7`. PowerShell itself does not throw, and a naive wrapper equates “no exception” with PASS. Review the actual evidence runner’s behavior and classify the stage correctly.
- **Concrete evidence/status/data facts:** `Invoke-RecordedCommand` creates `<id>.stdout.log` and `<id>.stderr.log`, records `cwd=<WorkingDirectory>` in `commands.log`, resets `$global:LASTEXITCODE=0`, changes to the requested working directory, invokes the command with redirected output, then reads `$global:LASTEXITCODE`. A nonzero code causes `throw "Recorded command exited with code $exitCode."`; finally restores the previous directory and appends both logs. `Add-StageResult` records `id`, `actual_status`, `evidence_path`, `exit_code`, and `note`. The runner’s self-test invokes a native `exit 7` command and requires `SELFTEST-NATIVE-FAILURE` to be `FAILED` with code `7`.
- **Engineering deliverables:**
  1. Draw the evidence path: runner -> native command -> stdout/stderr -> `$LASTEXITCODE` -> failure/classification -> summary JSON.
  2. Explain why PowerShell `try/catch` alone is insufficient for a native executable and what `$LASTEXITCODE` represents after invocation.
  3. State the exact stage status for exit `7`, the evidence that must remain, and how a naive wrapper could falsely produce PASS.
  4. Explain why the runner resets the prior native exit code, restores working directory, and self-tests success, native failure, PowerShell throw, `BLOCKED`, and `NOT_RUN` semantics.
  5. Propose one regression test or review assertion and give a 30–60 second interview explanation.
  6. Include the historical note: this fixed a runner/test-framework correctness issue where a native nonzero exit could be recorded as PASS. It is not evidence of a production infrastructure incident.
- **Constraints:** Do not execute the runner or generate fake evidence. `MDEP IMPLEMENTED` covers the script; `MDEP OFFLINE TESTED` covers source-level framework checks; real E2E stage execution is `MDEP RUNTIME DEFERRED`. A stage may be `FAILED` even if PowerShell did not receive a native exception.
- **Competency trained:** evidence-first automation, PowerShell/native-process boundaries, and reliable validation status.
- **Learner workspace/template:**

```text
Evidence path: ___
Why try/catch alone fails: ___
$LASTEXITCODE meaning: ___
Exit 7 stage status/evidence: ___
Working-directory and self-test value: ___
Regression test: ___
Historical note: ___
Interview explanation: ___
```

## VR-02 — Semantic reconciliation trace

- **Difficulty:** Intermediate
- **Task type:** RECONCILIATION DESIGN / GRAIN ANALYSIS / EVIDENCE REVIEW
- **Source files/artifacts to inspect:** `validation/reconciliation/README.md`; `validation/reconciliation/spark-iceberg.sql`; `validation/reconciliation/snowflake-dbt.sql`; `validation/quality-gates.yml`; `tests/test_mdep13_validation_framework.py`; [validation-matrix deep-dive](../code-deep-dive/validation-matrix.md).
- **Scenario:** For one bounded logical interval, observed counts are Bronze records `1,000`, Silver current-state rows `900`, and Gold daily-mart rows `30`. A reviewer says, “Counts differ, so the pipeline is broken.” Evaluate that claim using MDEP’s actual R01–R10 reconciliation set.
- **Concrete evidence/status/data facts:** Bronze is append/evidence/history and may retain replays, duplicates, malformed input, old schemas, and quarantine evidence. Silver is validated/deduplicated current state with one row per business key. Gold facts/marts are analytical outputs; `mart_daily_sales` has an aggregation grain. The reconciliation guide defines: R01 source orders -> Silver `core_orders`; R02 source payments -> Silver `core_payments`; R03 Silver orders -> Gold `fct_orders`; R04 Silver payments -> Gold `fct_payments` with orphan exceptions; R05 `fct_orders` -> `mart_daily_sales` grouped order-date/currency aggregation; R06 missing DKK-rate exceptions; R07 orphan-payment `order_id`s; R08 source deletes -> Silver/Gold; R09 duplicate keys; R10 required-field nulls/contract exceptions. Every completed check must record query, run ID, counts, exception keys, and an explicit pass/fail decision.
- **Engineering deliverables:**
  1. Explain why `1,000 -> 900 -> 30` can be correct without claiming that it is proven correct by the three counts alone. Define each layer’s semantic grain.
  2. Select at least four relevant actual checks by ID and describe their exact comparison/result: include one source-to-Silver check (R01 or R02), one Silver-to-Gold check (R03 or R04), R05, and at least one of R06–R10.
  3. Contrast business-key anti-join with a raw count comparison. State what exceptions and attributes must be retained as evidence.
  4. Explain why R08 delete reconciliation differs from source freshness, why R09 detects a current-state invariant, and how R05 compares aggregates at the mart grain rather than at Silver row grain.
  5. Describe what would prove correctness for this example, including bounded interval, queries, expected/actual counts, exception keys, and decision. Identify any template/model field mismatch you would verify before execution instead of silently treating a template as proof.
  6. Give a 30–60 second interview explanation.
- **Constraints:** Do not invent R-check IDs or redefine their meanings. Do not demand equal counts across layers. The templates and quality gates are `MDEP IMPLEMENTED`; their presence/static contracts are `MDEP OFFLINE TESTED`; cross-system query execution and evidence bundles are `MDEP RUNTIME DEFERRED`.
- **Competency trained:** semantic reconciliation, anti-join reasoning, grain-aware aggregates, and evidence design.
- **Learner workspace/template:**

```text
Why 1,000 -> 900 can be valid: ___
Why 900 -> 30 can be valid: ___
Layer grains: ___
Checks selected (ID -> purpose): ___
Anti-join versus counts: ___
Delete / duplicate / null logic: ___
Evidence bundle: ___
Pre-execution template/model checks: ___
Interview explanation: ___
```

## VR-03 — Historical BLOCKED versus final runtime-deferred status

- **Difficulty:** Foundation
- **Task type:** STATUS REVIEW / SCOPE GOVERNANCE / INTERVIEW COMMUNICATION
- **Source files/artifacts to inspect:** `validation/mdep-13-validation-matrix.yml`; `scripts/validate-mdep-13-e2e.ps1`; `docs/closure/v1-baseline.md`; `docs/closure/runtime-debt-register.md`; `docs/closure/story-closure-policy.md`; `docs/planning/v1-scope.md`; [validation-matrix deep-dive](../code-deep-dive/validation-matrix.md).
- **Scenario:** A reviewer sees `actual_status: BLOCKED` on historical physical evidence-plan rows such as `M8-AIRFLOW-BATCH` and concludes, “BLOCKED means V1 failed.” Produce a truthful response using the final scope decision without rewriting history.
- **Concrete evidence/status/data facts:** The matrix vocabulary is `[NOT_RUN, PASSED, FAILED, BLOCKED]`; its physical runtime rows currently preserve `BLOCKED` with explicit unavailable-environment blockers. The runner uses `PASSED`, `FAILED`, `BLOCKED`, and `NOT_RUN`: without `-RunRuntime`, named runtime stages become `NOT_RUN`; unavailable capability during a requested runtime attempt is `BLOCKED`; a recorded nonzero native command becomes `FAILED`; `PASSED` requires exit code `0` and evidence. The 2026-08-30 amendment says physical Airflow, Spark/Iceberg, Debezium/Kafka, Flink/S3, Snowflake/dbt, and cross-system execution are `RUNTIME DEFERRED` for V1, while implementation, static/offline validation, reconciliation logic, documentation, and interview readiness remain required.
- **Engineering deliverables:**
  1. Build a concise table defining `PASSED`, `FAILED`, `BLOCKED`, `NOT_RUN`, and the final-V1 phrase `MDEP RUNTIME DEFERRED`—including whether each is a runtime evidence status or a scope/closure statement.
  2. Explain what the historical `BLOCKED` rows mean, why unavailable infrastructure differs from failed correctness, and what `NOT_RUN` means in the current runner.
  3. Explain how the final scope amendment changes V1 closure criteria without retroactively changing historical evidence. State why `BLOCKED` must not be rewritten to `PASSED` and why runtime deferred is not runtime validated.
  4. Draft a truthful README/interview statement that says what was implemented/offline tested, what remains deferred, and how a future V1.x lab can supply runtime evidence.
  5. Identify one status/closure regression check and provide a 30–60 second interview explanation.
- **Constraints:** Use only the matrix/script status vocabulary for evidence rows. Do not claim runtime execution, erase historical blockers, or call deferred work complete runtime acceptance. The closure policy permits a Story to be implementation complete, offline/static validated, runtime deferred, and portfolio ready.
- **Competency trained:** evidence-status literacy, scope governance, and non-overclaiming technical communication.
- **Learner workspace/template:**

```text
Status table: ___
Historical BLOCKED meaning: ___
NOT_RUN versus BLOCKED: ___
Final V1 closure change: ___
Why deferred != validated: ___
README/interview wording: ___
Regression check: ___
Interview explanation: ___
```
