# V1 retrospective

## Goal, architecture, and sequence

V1 aimed to teach one coherent mainstream data platform: multi-source batch ingestion, PostgreSQL CDC, Bronze/Silver/Gold, Spark, Kafka/Flink, Iceberg, Snowflake/dbt, quality, and recovery thinking. The implementation sequence was contracts (MDEP-6), sources (7), batch Bronze (8), Spark Silver (9), Debezium/Kafka (10), Flink CDC (11), Snowflake/dbt Gold (12), then evidence (13). The final architecture retained one Silver writer per dataset: Spark for batch/reference and Flink for CDC/event state.

## Decisions and corrections

- **MDEP-9:** a different record hash initially allowed an old Bronze replay to overwrite newer Silver state. The fix made business timestamp, extraction time, ingestion time, then hash the deterministic ordering; hash is not freshness.
- **MDEP-10:** `include.transaction` was the wrong Debezium 3.0 switch. It was corrected to `provide.transaction.metadata`; configuration is not observation.
- **MDEP-11:** an initial topology was too stub-like. It was replaced with concrete source/state/sink wiring, while preserving the distinction between code and a running job.
- **Ordering:** Kafka partition identity is not global CDC freshness. LSN is primary source order; transaction order resolves supported same-transaction cases; unresolved conflicts are conservative.
- **MDEP-12:** model schema overrides risked `GOLD_GOLD`; physical Gold schema is profile-owned. Incremental was not applied everywhere: `fct_payments` rebuilds so deletes/relinks are visible.
- **MDEP-13 false-PASSED bug:** the first evidence runner could mark a native failure passed if it did not throw. It now captures stdout/stderr/exit code and only passes exit code zero.

## What went well and what was inefficient

Contracts, static tests, review-driven corrections, and learning documents made implementation inspectable. The inefficient part was discovering several configuration/evidence bugs after initial PRs; earlier adversarial contract tests and an explicit runtime-debt register would have caught them sooner.

## Runtime gap, learning, and V2 boundary

Docker, Spark/Flink, S3, Snowflake, and dbt integrations are still unexecuted on the current host. That prevents runtime-acceptance claims but not an honest implementation/portfolio baseline. The learning outcome is not tool enumeration; it is ownership, idempotency, ordering, reconciliation, and evidence discipline. A future V2 may improve production hardening, catalog/governance, managed services, CI integration tests, and observability; it does not begin here.
