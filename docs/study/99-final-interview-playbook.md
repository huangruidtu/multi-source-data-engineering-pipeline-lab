# MDEP final interview playbook

Use **PAUSE → CLARIFY → DIRECT → STRUCTURE → STOP**. State the boundary first, a project example second, the trade-off third, and the evidence status last.

## Tell me about this project

**30 seconds:** “I built a Commerce & Operations data-engineering V1 that demonstrates a replayable batch path and a CDC path. Airflow lands PostgreSQL/REST/file data as Bronze Parquet; Spark owns reference Iceberg Silver; Debezium, Kafka and Flink own CDC state; Snowflake and dbt own Gold.”

**60 seconds:** Add: “The main design rule is one canonical Silver writer per dataset. I added provenance and quarantine at the boundary, deterministic replay handling in batch and CDC, dimensional Gold models, and an evidence matrix. Integration runtimes are documented as blocked rather than claimed as proven.”

**2 minutes:** Add the MDEP-9 stale-hash correction, MDEP-10 transaction-property correction, MDEP-11 LSN/transaction ordering, MDEP-12 `fct_payments` rebuild and MDEP-13 false-PASSED lesson.

## Architecture and choices

**Why Bronze/Silver/Gold?** Bronze is replay evidence; Silver is trusted current state; Gold has analytical grain. **Why Spark?** bounded reference transforms. **Why Flink/Kafka?** keyed unbounded CDC transport/state. **Why Iceberg?** snapshots/atomic table state above Parquet. **Why Snowflake/dbt?** governed SQL and dimensional Gold without taking Silver ownership. 中文：每题先说职责，再说文件和真实边界。

## Failure answers

**Duplicates/stale updates:** “Batch accepts only a lexicographically newer business/extract/ingest/hash tuple. CDC accepts LSN, then same-transaction total order, then exact known transport replay.”

**Deletes:** “Debezium delete changes current state; tombstone is a Kafka compaction marker. Gold `fct_orders` synchronises deletes; `fct_payments` rebuilds.”

**Hardest problem:** “Correct ordering, not tool syntax. A changed hash initially looked fresh even when the business version was older.”

## Correct interview wording

Say: “I implemented/configured/statically tested X.” Say: “I would validate Y using `validation/mdep-13-validation-matrix.yml`.” Do not say Spark/Iceberg/Kafka/Flink/Snowflake paths passed E2E; they are runtime unvalidated. 中文：诚实地说明债务会提升可信度。

## Production answer

“After executing the current acceptance matrix, I would add least-privilege IAM/secrets, lag/WAL/checkpoint/quality/cost monitoring, lifecycle/compaction policies, lineage/governance, incident runbooks and DR. I would measure before changing partitioning or incremental materialization.”
