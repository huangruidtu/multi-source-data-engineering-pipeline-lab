# Final V1 Learning Guide

This is the compact starting point for studying the implemented project. Read
the authoritative maps first, then follow the code path that supports the topic.
It intentionally links to existing useful material instead of creating a second
copy of every explanation.

## First 30 minutes: architecture and boundaries

1. Read [Architecture ↔ Implementation Mapping](architecture-implementation-mapping.md).
2. Read [Final V1 Offline Validation Coverage](offline-validation-coverage.md).
3. Read `source-data/contracts/commerce-operations.md` for entity ownership.
4. Follow the flow in `README.md` and `docs/project-evidence.md`.

The core interview narrative is: sources land immutably in Bronze; one canonical
writer owns each Silver dataset; dbt owns Gold; reconciliation and failure logic
make the design explainable even though the physical multi-service lab is deferred.

## Batch and Airflow

Start at `orchestration/dags/bronze_ingestion.py`, then trace into
`ingestion/batch/pipeline.py`, `extractors.py`, and `bronze.py`.

Learn these concrete choices:

* Airflow schedules, retries, and backfills; it is not the transform engine.
* File identity is content-based so a repeat file can be recognised.
* Invalid records retain source evidence and a rejection reason in quarantine.
* Bronze is a replayable landing boundary; Silver logic belongs in Spark.

Use `tests/test_bronze_ingestion.py` alongside the code. The MDEP-8 learning
documents describe the intended runtime exercise; it remains V1.x deferred.

## Spark and Iceberg

Read `processing/spark/contracts.py` before `silver_batch.py`, then run through
`tests/test_silver_contracts.py`.

The central correctness lesson is not "deduplicate by hash." The job compares a
business timestamp, extraction evidence, ingestion evidence, and only then the
hash. That prevents an old Bronze replay with a changed payload from regressing
Silver. The code intentionally rejects CDC-owned entities so Spark cannot become
a competing writer for commerce current state.

## Debezium and Kafka

Read `ingestion/cdc/debezium-postgres-connector.json` and
`ingestion/cdc/contracts.py`, then `tests/test_cdc_contracts.py`.

Focus on the actual connector contract: `pgoutput`, an explicit publication and
slot, four approved tables, an initial snapshot, tombstones, and
`provide.transaction.metadata=true`. Be precise in an interview: configured
transaction metadata is expected Debezium behavior, not observed runtime proof.

## Flink and current-state CDC

Read `processing/flink/cdc_model.py`, then `flink_cdc_job.py`, then the two
Flink test modules.

The key idea is that Kafka offset is delivery position, not a universal freshness
clock. Per entity/key, MDEP prefers PostgreSQL LSN, then transaction ordering in
the same transaction, then a proven transport identity for replay. Equal position
without enough evidence is ignored conservatively. The job also makes a useful
distinction between replayable raw CDC Bronze and current-state Silver.

## Snowflake and dbt

Read `warehouse/snowflake/01_setup.sql`, then `analytics/dbt/models/sources.yml`
and the models in staging, intermediate, and marts order. Finally read
`tests/test_mdep12_warehouse_contract.py`.

Study the separation: external Iceberg Silver is read-only from the warehouse
view; dbt creates Snowflake-native Gold. Explain the intentional full rebuild of
`fct_payments`: a bounded V1 choice that correctly reflects deletes/relinks,
while a scalable incremental strategy is future work.

## Validation, reconciliation, and reality

Use `validation/mdep-13-validation-matrix.yml` only as a record of deferred
physical exercises. The final V1 acceptance evidence lives in code, unit tests,
static configuration checks, `validation/reconciliation/`, and the finalization
documents. `scripts/validate-mdep-13-e2e.ps1 -SelfTest` validates the runner's
own handling of passed, failed, blocked, and not-run stages without starting the
platform.

## Supporting material

The per-story learning documents under `docs/learning/mdep-6` through
`docs/learning/mdep-13` remain useful implementation notes. The Study Pack under
`docs/study/` is supplementary reading, not the final source of truth. For the
final project narrative and concise prompts, use
[Interview Q&A](interview-qa.md) and `docs/interview/v1-final-story.md`.

## What to say honestly

Say that V1 implements and offline-tests the contracts and correctness rules.
Say that the repository includes runtime configurations and planned commands.
Do **not** say that Airflow, Spark, Flink, Kafka, S3, Snowflake, dbt, or logical
replication was integration-tested: that physical work is an explicit V1.x lab.
