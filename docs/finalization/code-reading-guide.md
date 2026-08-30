# Code Reading Guide

Read tests beside source. They express the intended contract without requiring a
local Kafka, Spark, or Snowflake installation.

## A. Whole architecture
1. `README.md` — what V1 proves and does not prove.
2. `source-data/contracts/commerce-operations.md` — source and layer ownership.
3. `architecture-implementation-mapping.md` — the actual code-to-architecture map.

Question answered: “Which component owns which dataset?” Interview concept:
canonical ownership and bounded claims.

## B. Batch and Airflow
1. `orchestration/dags/bronze_ingestion.py` — task boundaries and scheduling.
2. `ingestion/batch/pipeline.py` — `land_rest`, `land_postgres`, `land_files`.
3. `ingestion/batch/extractors.py` — `fetch_paginated_json`, `file_identity`, `postgres_rows`.
4. `ingestion/batch/bronze.py` — `BatchContext`, `enrich_record`, `BronzePublisher`.
5. `tests/test_bronze_ingestion.py`.

Ask: “How does one rerun avoid a duplicate canonical Bronze object?” Learn:
deterministic identity, fail-closed pagination, and quarantine evidence.

## C. Spark and Iceberg
1. `processing/spark/contracts.py` — `VERSION_FIELDS`, `version_order_key`,
   `incoming_is_newer`, normalisation, and ownership rejection.
2. `tests/test_silver_contracts.py` — read stale replay tests before the job.
3. `processing/spark/silver_batch.py` — `split_valid_and_quarantine`,
   `merge_iceberg`, and incremental boundaries.

Ask: “Why is a hash insufficient for freshness?” Learn: lexicographic version
evidence and batch-owned Silver boundaries.

## D. PostgreSQL CDC, Debezium, Kafka
1. `source-data/postgres/schema.sql` and `cdc-init.sql` — source keys and publication.
2. `ingestion/cdc/debezium-postgres-connector.json` — actual connector settings.
3. `ingestion/cdc/contracts.py` — topic/key/envelope rules.
4. `tests/test_cdc_contracts.py` — configuration is read from actual JSON.

Ask: “How are transaction metadata and deletes configured?” Learn: pgoutput,
publication/slot, `provide.transaction.metadata`, and tombstone distinction.

## E. Flink
1. `processing/flink/cdc_model.py` — `parse_debezium`, `version_decision`,
   `apply_current_state`.
2. `tests/test_flink_cdc_model.py` — LSN, transaction order, replay, conflict.
3. `processing/flink/flink_cdc_job.py` — `topology_spec`, `CdcStateApplier`,
   source wiring, checkpoints, and sink DDL.
4. `tests/test_flink_topology.py`.

Ask: “What happens to an equal-LSN event?” Learn: stateful correctness, not
Kafka arrival order. Note the value-only deserializer limitation.

## F. Snowflake and dbt
1. `warehouse/snowflake/01_setup.sql` — external Silver and native Gold boundary.
2. `analytics/dbt/models/sources.yml` — allowed Silver sources.
3. staging → intermediate → marts in that order.
4. `schema.yml`, `analytics/dbt/tests/positive_amounts.sql`, and
   `tests/test_mdep12_warehouse_contract.py`.

Ask: “What is the grain of each fact?” Learn: external Iceberg consumption,
dimensional keys, current-state Gold, and the `fct_payments` rebuild trade-off.

## G. Validation and reconciliation
1. `validation/quality-gates.yml` and `failure-scenarios.yml`.
2. `validation/reconciliation/README.md` and SQL templates.
3. `scripts/validate-mdep-13-e2e.ps1` — `Invoke-RecordedCommand` and self-test.
4. `tests/test_mdep13_validation_framework.py`.

Ask: “What qualifies as a pass?” Learn: PASS needs exit code zero plus evidence;
runtime-deferred is neither a pass nor a hidden failure.
