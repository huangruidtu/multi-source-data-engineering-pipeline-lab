# Code Deep-Dive: `processing/spark/silver_batch.py`

**Source of truth:** [`processing/spark/silver_batch.py`](../../processing/spark/silver_batch.py), with pure ordering rules in [`processing/spark/contracts.py`](../../processing/spark/contracts.py). This walkthrough describes MDEP implementation and offline tests; Spark/Iceberg execution is **MDEP RUNTIME DEFERRED**.

## 1. Why this file exists

This is MDEP-9's batch Bronze-to-Silver job for exactly two reference entities: REST `exchange_rates` and file-derived `locations`. It creates Iceberg tables, reads MDEP-8 Bronze Parquet, validates/normalizes it, selects one deterministic winner per business key, quarantines non-winners/invalid rows, and merges winners into current Silver state.

## 2. Where it sits in the architecture

`REST API / files -> Airflow-oriented batch landing -> Bronze Parquet -> this Spark job -> Iceberg Silver -> Snowflake/dbt Gold`.

The file explicitly refuses CDC commerce entities. PostgreSQL `customers`, `products`, `orders`, and `payments` are Flink-owned current state, so this job cannot accidentally become a duplicate writer.

## 3. Inputs, outputs, and state

| Input/output | Contract |
|---|---|
| Input | Source-aligned Bronze Parquet at `bronze/rest_api/<entity>/**/*.parquet`. |
| Valid output | `mdep.silver.ref_exchange_rates` or `mdep.silver.ref_locations` Iceberg table. |
| Rejected output | Parquet under `quarantine/silver/<entity>`, partitioned by logical date. |
| State | Existing Iceberg row per natural key; table snapshots provide physical table history when runtime is executed. |

`JobConfig` isolates paths, logical date, optional half-open increment bounds, selected entity, additive-schema exercise, and explain/skew inspection flags from business logic.

## 4. Important symbols

| Symbol | Meaning |
|---|---|
| `table_name` | Enforces MDEP-9 ownership and returns `mdep.silver.ref_*`. |
| `build_spark` | Configures Hadoop Iceberg catalog and Spark Iceberg extensions. |
| `create_namespace_and_tables` | Defines concrete table schemas and keys. |
| `validate_exchange_rates`, `validate_locations` | Spark-side normalization/rejection evidence. |
| `split_valid_and_quarantine` | In-batch winner selection with a deterministic window. |
| `merge_iceberg` | Existing-state update predicate using the same ordering. |
| `VERSION_FIELDS` / `incoming_is_newer` | Pure contract that tests the version rule without Spark. |

## 5. Execution flow

1. `run` builds Spark and creates the Silver namespace/tables if absent.
2. It limits work to all batch-owned entities or one requested entity.
3. It reads Bronze, casts envelope timestamps, and optionally selects `[start, end)` by `ingested_at`.
4. Entity-specific validation produces normalized rows plus `rejection_reason` and original payload.
5. A window picks one valid incoming winner per natural key; invalid/non-winning rows are retained in quarantine.
6. The winners merge into Iceberg only when their complete version tuple is newer than the existing row.
7. Optional inspection prints plans/snapshots; the job stops Spark after all entities.

## 6. Function-by-function walkthrough

### `table_name`, `bronze_path`, and `quarantine_path`

`table_name` is a guardrail, not merely string formatting. It raises for `orders`, preventing a batch job from writing a CDC-owned table. Paths mirror the Bronze layout rather than discovering arbitrary files, making source ownership visible in configuration.

### `build_spark` and table creation

The Spark session registers `mdep` as a Hadoop-backed Iceberg catalog and installs Iceberg's Spark extensions. Tables retain business fields and landing evidence: `ingestion_id`, locator, source extract time, ingest time, and hash. Exchange rates are partitioned by `days(rate_date)`; locations are not, matching the small reference-data model rather than adding a decorative partition.

### `apply_incremental_boundary`

The boundary is start-inclusive/end-exclusive. That is essential for adjacent retries: `[00:00, 01:00)` and `[01:00, 02:00)` do not overlap at the hour boundary. If no bounds are supplied, it processes the available Bronze input.

### Validation functions

`validate_exchange_rates` normalizes date/currency/rate/timestamp and accumulates reasons such as non-positive rate or equal currencies. `validate_locations` normalizes identifiers/country/time zone and broadcasts the small `COUNTRY_REGIONS` reference map. Both retain `original_payload`, so quarantine explains what failed instead of only reporting a count.

### `split_valid_and_quarantine`

This is the **within-one-input-batch** deduplication layer. It partitions by the documented natural key and orders descending by business timestamp, `source_extract_ts`, `ingested_at`, then `record_hash`. Rank one is a Silver candidate; rank greater than one receives `duplicate_business_key_non_winner`. This does not decide whether an incoming winner may replace an older existing Iceberg row—that is the next function's job.

### `merge_iceberg`

The generated `MERGE` joins on the natural key. Its update condition is deliberately verbose: it compares business version first, then extraction evidence, then ingestion evidence, then hash. The final hash comparison is permitted only after all three timestamps tie. Consequently an old `updated_at` with a different hash cannot overwrite newer `locations` state, and exact replay matches no update clause.

### `write_quarantine`, schema/inspection helpers

Quarantine overwrites the deterministic logical-date partition; a retry produces the same rejection evidence rather than appending duplicate failures. `add_nullable_source_note` is intentionally additive-only. Inspection helpers are learning tools for partitions, wide transformations, skew, and Iceberg snapshots—not a claim that a cluster was run.

## 7. Critical code-block reasoning

The order in `split_valid_and_quarantine` and `merge_iceberg` must match. If the window used one freshness rule while `MERGE` used another, the winner selected in a batch might be rejected—or a replay might regress existing Silver. The pure `VERSION_FIELDS` tuple in `contracts.py` documents the exact order: `retrieved_at` for rates / `updated_at` for locations, then source extraction, ingest time, hash.

`COALESCE(source_extract_ts, TIMESTAMP '0001-01-01 ...')` in the SQL predicate makes missing extraction evidence deterministic and older than a non-null value. It does not turn a missing business timestamp into acceptable data: validation requires the business timestamp before a row reaches this merge.

The condition does **not** contain `OR s.record_hash <> t.record_hash`. That earlier-shaped shortcut is unsafe: a replayed old payload naturally has a different hash, but its older business version must lose. Hash here is a deterministic final tie-breaker, never a freshness signal.

## 8. Correctness invariants

- Only `exchange_rates` and `locations` are batch-owned Silver.
- One run selects at most one candidate per natural key.
- Invalid/duplicate input remains observable in quarantine.
- Version order is identical for incoming-batch dedup and existing-state merge.
- Older business state cannot overwrite newer state because its hash differs.
- Exact replay is a no-op at the Iceberg merge boundary.
- Incremental intervals are half-open.

## 9. Failure behavior

Validation failures are quarantined rather than inserted. Unknown countries and impossible rates are data-quality failures, not silently coerced facts. A Spark/Iceberg exception during table write is not simulated or swallowed; it fails the job. The current V1 model has no executed physical recovery proof, so do not state that Iceberg atomic commits or S3 failure recovery have been runtime validated.

## 10. Tests that protect the behavior

[`tests/test_silver_contracts.py`](../../tests/test_silver_contracts.py) directly tests the pure contracts: normalization, ownership, stable hash, half-open interval, newer business version, old different-hash replay rejection, exact replay, each timestamp tie-breaker, and hash as final tie-breaker. Static topology/CLI checks cover the callable job structure. **MDEP OFFLINE TESTED** does not equal physical Spark SQL execution.

## 11. What is not implemented / runtime deferred

**MDEP RUNTIME DEFERRED:** a Spark cluster, Iceberg catalog, S3A connectivity, actual Parquet read/write, merge execution, plan inspection, skew exercise, snapshot inspection, and recovery from a partial physical write.

## 12. Production concepts beyond current code

**GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED:** table maintenance/compaction, snapshot expiration policy, schema-evolution governance, data-quality alerting, and formal backfill orchestration. Iceberg supports relevant capabilities, but MDEP only defines an additive-column exercise and the table-level correctness semantics.

## 13. Common misunderstandings

- "`record_hash` tells us which record is freshest." No; it only breaks an otherwise exact timestamp tie.
- "Window dedup makes MERGE unnecessary." No; it has no knowledge of an already-existing Silver row.
- "Quarantine means discarded." No; it is preserved evidence with reason/payload.
- "`NOT ENFORCED`/logical keys automatically prevent duplicate writes." No; the job's deterministic winner and merge rule carry that responsibility.

## 14. Interview questions

**How do you stop a late Bronze replay from overwriting Silver?** I use a lexicographic version tuple in both batch deduplication and Iceberg `MERGE`: business timestamp, source extract timestamp, ingestion timestamp, then hash. A different hash alone cannot win.

**Why use a half-open incremental interval?** It makes retries and adjacent schedules composable: boundary records belong to exactly one time slice.

**Why separate batch-owned and CDC-owned entities?** The two paths have different freshness contracts. A batch timestamp/hash rule is not a substitute for PostgreSQL LSN and transaction ordering.

## 15. 30-second spoken explanation

“`silver_batch.py` transforms only the REST/file reference data from Bronze Parquet into Iceberg Silver. It validates records, quarantines bad or non-winning rows, and applies the same deterministic version order both inside the incoming batch and against existing Silver state. The key design is that an old replay with a different hash cannot regress Silver, because hash is only the final tie-breaker. Those contracts are unit-tested offline; Spark and Iceberg runtime execution are deferred.”

## 16. Senior follow-up discussion

Discuss why one generic `updated_at` rule was not forced onto every source. Source contracts determine freshness: exchange rates use `retrieved_at`, locations use `updated_at`, and commerce CDC uses WAL LSN. A senior design preserves that difference while centralizing the comparison function and testing it independently of the compute engine.
