# MDEP-9 interview Q&A

## Why Spark, and how does it execute this job?

**Direct answer:** Spark is appropriate for bounded Parquet transformation and scales the validation, join, deduplication, and Iceberg write across partitions. The Driver builds a lazy DataFrame plan; actions execute physical stages on Executors.

**Project example:** `silver_batch.py` validates MDEP-8 REST Bronze data, broadcast-joins locations to the country reference, windows duplicates, and merges trusted rows. **Follow-up:** “What causes shuffle?” Window partitioning and `groupBy` require matching keys to move together. **Senior extension:** inspect the formatted plan and task metrics before tuning partition counts, join strategy, or skew.

## Why Parquet and what does Iceberg add?

**Direct answer:** Parquet is efficient columnar storage; Iceberg adds a transactional table abstraction over files. **Project example:** Bronze is Parquet; Silver uses Iceberg metadata/snapshots and `MERGE`. **Follow-up:** “What is a snapshot/manifest?” A snapshot is a committed table state; manifests list the files that comprise it. **Senior extension:** track snapshot and small-file growth, compact deliberately, and do not equate a path listing with a consistent table state.

## How are quality, duplicates, and bad records handled?

**Direct answer:** required/type/business/reference rules are applied before a deterministic natural-key window. The latest source version wins, then extract/ingest time and hash break ties; invalid and non-winning rows retain payload, lineage, and reason in Quarantine.

**Project example:** a zero exchange rate becomes `non_positive_rate`; a duplicate rate is `duplicate_business_key_non_winner`. **Follow-up:** “Why not drop it?” Diagnosis, reconciliation, and replay need evidence. **Senior extension:** measure reject rates by rule and set alert thresholds, ownership, and replay processes.

## How do incremental processing and idempotency work?

**Direct answer:** the job optionally reads an inclusive/exclusive Bronze `ingested_at` interval and merges by documented natural key. The same rerun does not create a second logical table row.

**Project example:** rate key is date/base/quote and location key is location ID. **Follow-up:** “Is that a watermark?” No—this is a bounded batch processing boundary; Flink event-time watermarks are later scope. **Senior extension:** persist a reconciled processing state/checkpoint and handle late arrivals by deliberately reprocessing an overlap.

## How do you prevent a late or replayed old batch from overwriting newer Silver state?

**Direct answer:** I compare the incoming record with the existing Silver record using a complete lexicographic version tuple, not merely a changed payload hash. Exchange rates use `retrieved_at`, then `source_extract_ts`, `ingested_at`, and finally `record_hash`; locations use `updated_at` followed by the same evidence fields. Only a greater tuple updates.

**Project example:** a location already at `updated_at=2026-08-20` ignores a replay from `2026-07-01` even if its `record_hash` differs. An exact replay has an equal tuple and is a no-op. **Likely follow-up:** “Why include the hash?” It deterministically resolves a true timestamp tie but is never treated as freshness. **Senior-level extension:** define producer version semantics explicitly, retain audit history/snapshots, monitor rejected or stale-replay rates, and use an overlap/reconciliation policy for late batch arrivals.

## What are repartitioning and skew, and how would you diagnose them?

**Direct answer:** repartition redistributes data with a shuffle; coalesce reduces partitions without necessarily redistributing. Skew is uneven key distribution that leaves one task much slower/larger.

**Project example:** `--skew-exercise` groups repeated currency/country keys and prints a plan. **Follow-up:** “First fix?” inspect key distribution and stage metrics, then consider broadcast, salting, AQE, or a better partition key. **Senior extension:** choose the smallest intervention and prove it reduces max task time rather than only changing plan text.

## Why not let Spark write CDC current state?

**Direct answer:** MDEP-6 assigns PostgreSQL current-state Silver to Debezium/Kafka/Flink. **Project example:** MDEP-9 rejects unsupported entity names in `deterministic_silver_key`/`table_name`. **Follow-up:** “Why does it matter?” batch snapshots can overwrite newer CDC state. **Senior extension:** enforce ownership in review, CI, naming, access policies, and reconciliation procedures.

## What happens on partial failure and how would you scale it?

**Direct answer:** retry from immutable Bronze; Iceberg exposes a prior or committed snapshot, and deterministic merge/quarantine boundaries make the replay explainable. **Project example:** logical-date Quarantine overwrite avoids multiplying evidence on retries. **Senior extension:** add metrics, compaction, catalog/S3 concurrency tests, durable checkpoints, data-quality SLAs, and real runtime integration tests.
