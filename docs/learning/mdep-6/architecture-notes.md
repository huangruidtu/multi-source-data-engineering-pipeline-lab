# MDEP-6 Architecture Notes — Data Contracts and Ownership

## Implemented decisions

1. **One canonical Silver writer per logical dataset.** PostgreSQL batch extracts are Bronze-only once CDC is active; Flink CDC owns `core_customers`, `core_products`, `core_orders`, and `core_payments`. Spark owns batch-derived references and Flink owns event tables.
2. **Bronze preserves evidence; Silver determines validity.** Raw payloads and metadata are retained, including duplicates and invalid records. Rejection results go to Quarantine with source location, reason, contract version, and original payload/reference.
3. **S3-backed HadoopCatalog is the planned shared Iceberg catalog.** The accepted ADR selects `s3://<bucket>/iceberg/<namespace>/<table>/`, shared by future Spark and Flink writers. Snowflake reads externally managed Silver and dbt owns Gold; Snowflake does not write back to Silver.
4. **Contracts are additive by default.** New fields may be added; removals or semantic/type changes are breaking changes requiring a controlled migration.

## Why this design

The central problem is not tool choice; it is avoiding two paths independently changing the same current-state table. A Bronze-only batch path still supports snapshot recovery and backfill without competing with CDC. Using the Debezium envelope rather than inventing a second CDC protocol preserves source position and mutation semantics.

HadoopCatalog was chosen because the approved V1 already requires S3 and Iceberg. It avoids adding Glue, a metastore, or an Iceberg REST service solely for catalog coverage.

## Alternatives and trade-offs

| Alternative | Why not selected for V1 | Trade-off retained |
| --- | --- | --- |
| Batch and CDC both upsert PostgreSQL Silver tables | Creates dual-writer inconsistency | Snapshot reconciliation must be designed later instead of silently writing Silver from batch |
| Glue catalog | Adds a service outside the smallest stack | HadoopCatalog has fewer managed catalog capabilities |
| Iceberg REST catalog | Adds a service boundary without required concept coverage | More manual catalog/table-location discipline |
| Snowflake-managed Iceberg tables | Conflicts with Spark/Flink external ownership | Snowflake external access requires later integration work |

## Ownership and failure boundaries

Source systems own business data correctness at origin. Bronze owns replayable capture, not business quality. Silver owners validate, deduplicate, normalize, and quarantine. Gold/dbt owns business-facing models. A malformed payload is not silently lost: it crosses the Bronze-to-Quarantine boundary with evidence.

An order update and a payment failure are deliberately different: CDC updates `core_orders` current state; the event belongs in `evt_payments`. Treating an event as a replacement for CDC state would create incorrect data consistency.

## Scalability, reliability, and consistency implications

`aggregate_id` is the Kafka partition/key choice, so ordering is only per key and not global. Consumers must tolerate at-least-once delivery and deduplicate events by `event_id` or CDC changes by source position. `occurred_at` is reserved for event-time handling; a watermark is intentionally not selected until MDEP-11.

The current-state CDC rule gives per-key source order. It deliberately does not claim cross-key transactional business ordering. This keeps the contract honest about what Kafka partitioning can guarantee.

## Relationship to the target architecture

MDEP-6 defines the rules for all three layers without implementing them: Bronze is source-aligned/replayable, Silver is technically trusted, and Gold is the dbt/Snowflake consumer layer. It also separates batch references, CDC current state, and streaming business events before their processing technologies are introduced.

## Assumptions and deferred decisions

Assumptions: future sources can emit stable keys or a usable payload hash; S3 will host the Iceberg warehouse; Debezium will provide normal operation/source metadata.

Deferred: physical schema types, topic setup, table namespaces, retention, actual S3/Snowflake integration, watermark/allowed lateness, delete representation, and all runtime validation. None are implemented by this Story.
