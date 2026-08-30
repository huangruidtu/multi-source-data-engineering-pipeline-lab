# MDEP-13 interview Q&A

Each answer is grounded in the implemented MDEP-13 framework; runtime outcomes are never invented.

## How do you validate a data pipeline end to end?

**Direct answer:** Preflight capabilities, execute a bounded run, capture evidence, reconcile keys and values across layers, then exercise recovery cases.

**Deeper explanation:** A successful process exit is insufficient; I need source, Bronze, Silver, and Gold semantic checks plus logs.

**Project example:** `scripts/validate-mdep-13-e2e.ps1` writes `preflight.json`, logs, and `validation-summary.json` per run.

**Likely follow-up:** What if a system is unavailable? **Answer:** Record BLOCKED with its prerequisite; do not claim it passed.

**Senior extension:** Make the evidence contract part of release approval and trend quality gates over time.

## Why are row counts not enough, and what is reconciliation?

**Direct answer:** Counts are a signal; reconciliation verifies that the right business keys and values have the right semantics.

**Deeper explanation:** Bronze append history, Silver current state, and Gold aggregations intentionally differ in row count.

**Project example:** R03–R05 use anti-joins and fact-to-mart aggregation rather than equality of all layer counts.

**Likely follow-up:** How do you treat exceptions? **Answer:** Save key-level exception lists with an explained disposition.

**Senior extension:** Add tolerances only when they are business-approved, never to hide unexplained drift.

## How do you prove idempotency and test replay/stale CDC?

**Direct answer:** Repeat the same logical input and prove canonical state does not duplicate or regress.

**Deeper explanation:** Exact replay should be a no-op; stale input must lose according to source version ordering, not payload hash alone.

**Project example:** F04, F05, F11–F13 and Q02 describe duplicate rerun, stale Bronze, duplicate CDC, lower LSN, and transaction-order exercises.

**Likely follow-up:** How do you test recovery? **Answer:** stop/restart from a completed checkpoint, then reconcile source and current state.

**Senior extension:** Test offset loss separately because it has different data-loss and duplicate risks from a normal restart.

## How do you validate Snowflake/dbt outputs?

**Direct answer:** Validate access, parse/compile/run/test/freshness, then reconcile Silver keys and Gold fact/mart aggregates.

**Deeper explanation:** dbt tests establish model contracts but do not prove the upstream external Iceberg data is current.

**Project example:** M12-SNOWFLAKE-DBT and `snowflake-dbt.sql` cover external access, Gold facts, orphan warnings, and daily-sales aggregation.

**Likely follow-up:** Why rebuild payments? **Answer:** payment deletes and upstream order changes must be reflected every run.

**Senior extension:** Observe query profile and credits after correctness is proven.

## What gates block deployment, what would you monitor, and what would change in production?

**Direct answer:** Duplicate current-state keys, stale-regression proof, required nulls, canonical rerun behavior, and blocking dbt tests are deployment gates.

**Deeper explanation:** Warnings such as missing rates and orphan payments remain visible; operations watches lag, retries, checkpoints, commits, freshness, tests, and cost.

**Project example:** `validation/quality-gates.yml` classifies gates and `docs/interview/production-improvements.md` lists production changes.

**Likely follow-up:** Hardest tradeoff? **Answer:** preserving one canonical writer per Silver table while still demonstrating both batch and CDC.

**Senior extension:** Use managed services, IAM/secrets, production catalog/governance, and integration CI after manual behavior is understood.

## How do you discuss an unvalidated component, and what did you learn?

**Direct answer:** I state implemented code/configuration, static evidence, required runtime test, and exact blocker separately.

**Deeper explanation:** That preserves trust and makes the next validation action clear.

**Project example:** MDEP-8 through MDEP-12 are BLOCKED in the validation matrix rather than silently marked passed.

**Likely follow-up:** Does that make the project incomplete? **Answer:** the implementation is reviewable, but runtime acceptance remains open.

**Senior extension:** Treat evidence freshness as an operational asset and expire old validation when dependencies change.
