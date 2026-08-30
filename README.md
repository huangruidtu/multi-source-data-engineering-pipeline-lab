# Multi-Source Data Engineering Pipeline Lab

An interview-oriented Commerce & Operations data-engineering portfolio. V1
implements a coherent batch and CDC architecture, its correctness contracts,
offline validation, failure reasoning, and learning material—without tool sprawl.

> **Final V1 truth:** implemented, offline/statically validated, and
> documented/designed. **Full infrastructure runtime validation is deferred from
> V1** to a separate V1.x hands-on lab. This repository does not claim that
> Airflow, Spark, Flink, Kafka, S3, Snowflake, and dbt ran together.

## Architecture in one minute

```text
PostgreSQL + REST + CSV/JSON -- Airflow / batch --> Bronze Parquet -- Spark --> Silver Iceberg
PostgreSQL WAL -- Debezium --> Kafka --> Flink --> CDC Bronze archive + Silver Iceberg
Silver Iceberg -- Snowflake external access --> dbt --> Gold dimensions, facts, marts
```

| Path | Purpose | Canonical owner |
| --- | --- | --- |
| Batch | Reference ingestion and bounded replay | Airflow → Bronze; Spark → batch Silver |
| CDC | Commerce changes and current state | Debezium/Kafka → Flink → CDC Bronze/Silver |
| Analytics | Dimensional reporting and marts | Snowflake/dbt → Gold |
| Reliability | Quality, reconciliation, failure reasoning | `validation/` |

## What V1 demonstrates

- Commerce contracts, source ownership, imperfect fixtures, and CDC publication
  / connector boundaries.
- Idempotent batch patterns, deterministic Bronze paths, retries, and quarantine
  that preserves rejection evidence.
- Spark-owned reference Silver with lexicographic freshness. An older Bronze
  replay cannot overwrite newer state merely because `record_hash` differs; hash
  is only the final tie-breaker.
- Debezium PostgreSQL `pgoutput`, explicit publication/slot, initial snapshot,
  delete tombstones, and `provide.transaction.metadata=true`.
- Flink current-state semantics: PostgreSQL LSN first, same-transaction order
  next, proven replay identity last, and conservative equal-position conflicts.
- Iceberg/Snowflake/dbt Gold design, declared dimensional grains, and the
  intentional full-rebuild trade-off for `fct_payments`.
- Offline tests, a validation runner that cannot falsely pass a failed native
  command, reconciliation templates, and documented failure scenarios.

## Important correctness decisions

1. **One canonical Silver writer per dataset:** Spark owns batch reference
   entities; Flink owns CDC commerce state; dbt owns Gold only.
2. **Freshness is evidence, not payload difference:** Spark uses business time →
   extract time → ingestion time → hash; Flink uses LSN → transaction order →
   known transport identity.
3. **Kafka offsets are transport positions, not source freshness.**
4. **Bronze and quarantine preserve evidence** instead of silently losing data.
5. **Runtime claims require runtime evidence.** Configured is not the same as
   physically validated.

## Validation status

Offline `unittest` coverage exists for batch ingestion, Spark contracts,
Debezium configuration, Flink CDC model/topology, Snowflake/dbt contracts,
closure policy, and the validation framework. The final matrix records the
remaining V1.x runtime work and never treats it as a failed V1 implementation.

- [Architecture ↔ Implementation Mapping](docs/finalization/architecture-implementation-mapping.md)
- [Offline Validation Coverage](docs/finalization/offline-validation-coverage.md)
- [Runtime-Deferred Register](docs/closure/runtime-debt-register.md)
- [V1 Scope and Acceptance Model](docs/planning/v1-scope.md)

## Repository guide

| Start here | Then inspect |
| --- | --- |
| [Data contracts](source-data/contracts/commerce-operations.md) | `source-data/postgres/`, `source-data/rest-api/`, `source-data/files/` |
| [Batch ingestion](ingestion/batch/pipeline.py) | [Airflow DAG](orchestration/dags/bronze_ingestion.py), `tests/test_bronze_ingestion.py` |
| [Spark Silver](processing/spark/contracts.py) | `processing/spark/silver_batch.py`, `tests/test_silver_contracts.py` |
| [CDC connector](ingestion/cdc/debezium-postgres-connector.json) | `ingestion/cdc/contracts.py`, `tests/test_cdc_contracts.py` |
| [Flink CDC](processing/flink/cdc_model.py) | `processing/flink/flink_cdc_job.py`, Flink tests |
| [Warehouse/dbt](warehouse/snowflake/01_setup.sql) | `analytics/dbt/`, `tests/test_mdep12_warehouse_contract.py` |
| [Reliability](validation/) | `tests/test_mdep13_validation_framework.py` |

## Learning and interview preparation

- [Final Learning Guide](docs/finalization/learning-guide.md)
- [Project-specific Interview Q&A](docs/finalization/interview-qa.md)
- [Architecture Walkthrough](docs/interview/architecture-walkthrough.md)
- [Final V1 Story](docs/interview/v1-final-story.md)
- [Story-level Learning Notes](docs/learning/README.md)

The Study Pack under `docs/study/` is supplementary reading; the finalization
documents above are the compact source of truth.

## V1.x runtime lab (explicitly deferred)

Future work is to run—not redesign—the existing implementation: start Docker
services, execute Airflow/Spark/Flink paths, capture Debezium/Kafka evidence,
verify Iceberg/checkpoint behavior, run dbt/Snowflake, and reconcile layers. See
[`validation/mdep-13-validation-matrix.yml`](validation/mdep-13-validation-matrix.yml)
for required environments and commands.
