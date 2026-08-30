# MDEP Code Deep-Dive Master Map

This is the navigation layer for the post-V1 Code Deep-Dive corpus. It adds no
implementation claims. Use it with the authoritative [V1 finalization index](../finalization/README.md).

## 1. Full code-reading route

1. [Source extractors](source-extractors.md): REST, file, and PostgreSQL boundary behavior.
2. [Batch pipeline](batch-pipeline.md): extractor output plus deterministic Bronze composition.
3. [Airflow Bronze DAG](airflow-bronze-dag.md): daily control plane, not data transformation.
4. [Bronze publication](bronze-batch-publication.md): identity, conditional writes, manifest, quarantine.
5. [Spark Silver](silver-batch.md): validation, deterministic winner, and replay-safe merge.
6. [CDC transport contracts](cdc-transport-contracts.md) and [Debezium connector](debezium-postgres-connector.md): scope, keys, publication, slot, snapshot, tombstone.
7. [CDC model](cdc-model.md): LSN/transaction/replay semantics for one state key.
8. [Flink CDC job](flink-cdc-job.md): raw archive, parser, side outputs, managed state, and Iceberg sinks.
9. [Snowflake/Iceberg setup](snowflake-iceberg-setup.md): external Silver access and native Gold target.
10. [dbt staging](dbt-staging-models.md) → [intermediate](dbt-intermediate-models.md) → [Gold](dbt-gold-models.md).
11. [Validation matrix](validation-matrix.md) → [runner and reconciliation](validation-runner-reconciliation.md).

## 2. Architecture → code mapping

| Node | Real source | Deep-Dive | Tests / evidence |
|---|---|---|---|
| Batch sources | `ingestion/batch/extractors.py` | [extractors](source-extractors.md) | `test_bronze_ingestion.py` |
| Batch control | `orchestration/dags/bronze_ingestion.py` | [Airflow](airflow-bronze-dag.md) | batch contract tests |
| Bronze landing | `ingestion/batch/pipeline.py`, `bronze.py` | [pipeline](batch-pipeline.md), [Bronze](bronze-batch-publication.md) | `test_bronze_ingestion.py` |
| Batch Silver | `processing/spark/silver_batch.py` | [Spark Silver](silver-batch.md) | `test_silver_contracts.py` |
| CDC transport | `ingestion/cdc/contracts.py`, connector JSON | [contracts](cdc-transport-contracts.md), [connector](debezium-postgres-connector.md) | `test_cdc_contracts.py` |
| CDC state/topology | `processing/flink/cdc_model.py`, `flink_cdc_job.py` | [model](cdc-model.md), [Flink](flink-cdc-job.md) | Flink model/topology tests |
| Warehouse/dbt | `warehouse/snowflake/`, `analytics/dbt/models/` | [setup](snowflake-iceberg-setup.md), [dbt](dbt-gold-models.md) | MDEP-12 contract tests |
| Validation | `validation/`, `scripts/validate-mdep-13-e2e.ps1` | [matrix](validation-matrix.md), [runner](validation-runner-reconciliation.md) | MDEP-13 tests |

For complete ownership mapping, see [architecture implementation mapping](../finalization/architecture-implementation-mapping.md).

## 3. Interview topic routes

| Topic | Read in order | Key answer |
|---|---|---|
| Idempotency | [extractors](source-extractors.md) → [pipeline](batch-pipeline.md) → [Bronze](bronze-batch-publication.md) | All pages succeed before one deterministic key is conditionally published. |
| Backfill | [Airflow](airflow-bronze-dag.md) → [pipeline](batch-pipeline.md) | `catchup=False` disables automatic catchup, not an explicit historical run. |
| Batch replay safety | [Spark Silver](silver-batch.md) | Batch winner and existing-state merge use the same version tuple. |
| CDC ordering | [connector](debezium-postgres-connector.md) → [contracts](cdc-transport-contracts.md) → [CDC model](cdc-model.md) | LSN is primary; transaction order resolves defined ties; partitions are not freshness. |
| Exactly-once | [Flink](flink-cdc-job.md) → [matrix](validation-matrix.md) | Configuration exists; end-to-end delivery proof is runtime deferred. |
| Delete handling | [CDC model](cdc-model.md) → [Flink](flink-cdc-job.md) → [dbt Gold](dbt-gold-models.md) | Delete, tombstone, state retraction, and Gold deletion are distinct. |
| Data quality | [Spark Silver](silver-batch.md) → [dbt intermediate](dbt-intermediate-models.md) | Preserve rejected/missing-value evidence instead of inventing facts. |
| Iceberg ownership | [Spark](silver-batch.md) → [Flink](flink-cdc-job.md) → [Snowflake](snowflake-iceberg-setup.md) | Processing owns Silver; Snowflake reads externally; dbt owns Gold. |
| dbt incremental | [staging](dbt-staging-models.md) → [Gold](dbt-gold-models.md) | Merge does not synchronize delete by itself. |
| Reconciliation | [matrix](validation-matrix.md) → [runner](validation-runner-reconciliation.md) | Compare keys, grain, and exceptions—not only row counts. |

## 4. Time-boxed study routes

### 30 minutes

Read the [interview cheat sheet](../finalization/interview-cheat-sheet.md), then rehearse the 30-second sections in [Bronze](bronze-batch-publication.md), [Spark](silver-batch.md), [CDC model](cdc-model.md), [dbt Gold](dbt-gold-models.md), and [validation](validation-runner-reconciliation.md).

### 2 hours

Read the full route’s Bronze, Spark, CDC model/Flink, dbt Gold, and validation stops. Open each source and test beside its walkthrough. For each, explain an invariant, failure scenario, and runtime-deferred boundary.

### Full study

Follow every step in section 1: source first, test second, walkthrough third, finalization evidence fourth. End with the [final interview playbook](../study/99-final-interview-playbook.md).

## 5. What not to overclaim

| Label | Meaning |
|---|---|
| **MDEP IMPLEMENTED** | Present in current repository source/configuration. |
| **MDEP OFFLINE TESTED** | Covered by repository tests/static validation, not physical infrastructure execution. |
| **MDEP RUNTIME DEFERRED** | Physical exercise outside final V1 completion; never call it runtime validated. |
| **GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED** | Useful discussion context, not an MDEP feature claim. |

Be exact about Airflow retry versus data idempotency, Flink exactly-once configuration versus proof, Debezium configuration versus observed events, and reconciliation SQL templates versus completed cross-layer reconciliation. The canonical scope amendment remains in the [project charter](../project-charter.md).
