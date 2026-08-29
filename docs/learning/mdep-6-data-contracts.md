# Learning Notes — MDEP-6 Data Contracts and Ownership

**Status:** IMPLEMENTED documentation for MDEP-6.

## What was implemented

This Story adds the contract baseline in [Commerce & Operations Data Contracts](../../source-data/contracts/commerce-operations.md) and the Iceberg access decision in [ADR-0001](../decisions/0001-iceberg-catalog-and-snowflake-access.md). They define the V1 logical entities, keys, grains, source-to-layer ownership, batch metadata, Kafka business-event envelope, Debezium CDC semantics, quality expectations, and quarantine evidence.

No source system, connector, bucket, Spark job, Flink job, or warehouse table was created. Those belong to later Jira Stories.

## Why this exists

The architecture has two potential paths from PostgreSQL: batch snapshots and CDC. The contract resolves the dangerous ambiguity before code exists: snapshots are Bronze audit/backfill input, while Debezium/Flink is the only writer of CDC-enabled current-state Silver tables. That prevents a later batch job and stream job from independently publishing conflicting versions of `orders` or `payments`.

## Important concepts in this implementation

- **Grain:** `core_orders` is one current row per `order_id`; `evt_orders` is one immutable row per `event_id`. They answer different questions and must not be merged accidentally.
- **Idempotency:** Bronze may hold duplicates. Batch metadata and record hashes make them diagnosable; Silver uses business keys, source versions, and event identifiers to produce one logical record.
- **CDC mutation semantics:** an update replaces a keyed current-state record; a delete is still an auditable CDC event even when the current-state row is removed.
- **Event time:** the contract separates `occurred_at` from `produced_at`. Flink’s later watermark policy must operate on business time rather than blindly using arrival time.
- **Quarantine:** rejected data retains its original payload/reference and reason so it can be diagnosed and replayed instead of disappearing.

## Design decisions to inspect

- The source-to-layer ownership matrix in the contract is the central V1 guardrail.
- Batch records use `record_hash` because not every file/API record has a stable source key.
- Kafka ordering is intentionally scoped to the aggregate key/partition; there is no global-order promise.
- [ADR-0001](../decisions/0001-iceberg-catalog-and-snowflake-access.md) chooses HadoopCatalog on S3 so Spark and Flink can share Iceberg metadata without adding a catalog service. Snowflake is a reader of Silver and dbt owns Gold.

## Files to review

- [source-data/contracts/commerce-operations.md](../../source-data/contracts/commerce-operations.md) — contract, data dictionary, envelopes, ownership, and validation walkthrough.
- [docs/decisions/0001-iceberg-catalog-and-snowflake-access.md](../decisions/0001-iceberg-catalog-and-snowflake-access.md) — catalog/access decision and intentionally deferred work.
- This document — explanation of the implementation boundary.

## Manual inspection checklist

1. Trace an `orders` update through the ownership matrix and confirm that Spark never becomes a second current-state writer.
2. Compare `core_payments` with `evt_payments`; explain why a payment failure event is not automatically a replacement for CDC state.
3. Read the batch and event envelopes. Identify which fields support replay, deduplication, and audit.
4. For each walkthrough scenario, state whether the record belongs in Bronze, Silver, Quarantine, or all of them at different stages.
5. Read ADR-0001 and explain why a catalog is required even though data files live in S3.

## Interview questions derived from this Story

- How would you avoid conflicts when both batch snapshots and CDC exist for the same PostgreSQL table?
- What is the difference between a current-state table’s grain and an immutable event table’s grain?
- How would you make file ingestion idempotent when the source has no primary key?
- What information from a Debezium event is needed to safely apply updates and deletes?
- What does Kafka ordering guarantee, and what does it not guarantee?
- Why is an Iceberg catalog necessary in addition to object storage?
