# Module 07 — Core concepts

## Evidence and status vocabulary
1. **Definition:** evidence is a run-scoped artifact supporting a result. 2. **Why:** a static test cannot prove a Docker/cloud integration. 3. **How:** `validation/mdep-13-validation-matrix.yml` defines durable debt; a run emits `validation-summary.json`. 4. **MDEP:** MDEP-13. 5. **Misunderstanding:** a local script’s exit is a complete E2E result. 6. **Failure:** false PASSED. 7. **Production:** retain logs/query outputs/run identity and freshness. 8. **Interview:** `BLOCKED` means required environment unavailable; `NOT_RUN` means intentionally not attempted.

## Reconciliation and quality gates
Counts alone fail because Bronze is history while Silver is current state and Gold is an analytical grain. Use bounded counts, business-key anti-joins, duplicate/null checks, aggregations and documented exceptions. Gates are BLOCKING, WARNING or INFORMATIONAL according to consumer risk.

## Closure and runtime debt
MDEP-14 separates implementation, static validation, runtime acceptance and portfolio completeness. RD-08 through RD-13 record batch, Spark/Iceberg, CDC, Flink, Snowflake/dbt and cross-system evidence gaps. A production hardening discussion includes IAM/secrets, retention, cost, lineage, observability, SLA and DR—but is not a claim that these were built.
