# Jira Story Dependencies

**Status:** PLANNED

## Dependency order

```text
MDEP-S01 Data contracts and local foundations
 ├─→ MDEP-S02 Source systems and imperfect test data
 │    ├─→ MDEP-S03 Airflow batch Bronze ingestion
 │    │    └─→ MDEP-S04 Spark Bronze-to-Silver Iceberg
 │    └─→ MDEP-S05 PostgreSQL CDC, Debezium, and Kafka
 │         └─→ MDEP-S06 Flink streaming Bronze/Silver
 └───────────────────────────────────────────────┐
MDEP-S04 ─────────────────────────────────────────┼─→ MDEP-S07 Snowflake and dbt Gold
MDEP-S06 ─────────────────────────────────────────┘
MDEP-S03 + MDEP-S04 + MDEP-S05 + MDEP-S06 + MDEP-S07 ─→ MDEP-S08 End-to-end failure lab
```

## Blocking relationships

| Story | Blocks | Why it blocks |
| --- | --- | --- |
| MDEP-S01 | S02–S08 | Defines entities, event contracts, ownership, quality rules, identifiers, and layer boundaries. |
| MDEP-S02 | S03, S05 | Batch and CDC work need reproducible sources and intentionally invalid cases. |
| MDEP-S03 | S04, S08 | Spark needs Bronze batch data and Airflow retry/backfill behavior for validation. |
| MDEP-S04 | S07, S08 | Warehouse/dbt needs trusted batch Silver Iceberg datasets and documented schemas. |
| MDEP-S05 | S06, S08 | Flink CDC and stream validation need Kafka topics, Debezium events, and known source mutations. |
| MDEP-S06 | S07, S08 | Gold marts and final validation need streaming/current-state Silver data. |
| MDEP-S07 | S08 | The complete V1 proof includes the Gold dimensional models and dbt quality tests. |

## Parallel work that remains safe

After MDEP-S02, MDEP-S03 (batch path) and MDEP-S05 (CDC/Kafka path) may proceed in parallel, provided both preserve the dataset ownership matrix from MDEP-S01. MDEP-S04 and MDEP-S06 may then proceed in parallel. Snowflake/dbt begins only after both paths expose their agreed Silver contract.

## Non-blocking sequencing notes

- SCD Type 2 is deliberately bounded to one dimension or dbt snapshot in S07; it must not delay the first fact/mart path.
- A lightweight GitHub Actions check is optional and may be added only after the relevant manual validation exists; it does not block V1.
- Documentation is updated story by story, but final documentation reconciliation occurs in S08.
