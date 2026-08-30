# MDEP-11 implementation guide

## Implemented

`processing/flink/flink_cdc_job.py` is now a real PyFlink topology, not a guard-rail stub. It creates one `KafkaSource` per approved Debezium topic, starts each at the earliest retained offset under `mdep-flink-cdc-silver-v1`, normalizes each value through `parse_debezium`, emits malformed records to a Flink side output, and keys valid records by `entity:primary_key`.

`CdcStateApplier` is a `KeyedProcessFunction` with managed `ValueState` for the last accepted CDC version and the prior current row. `r`/`c`/`u` emit an Iceberg changelog insert/upsert; a newer `d` emits a physical delete from the stored prior row; tombstones, stale events, replays, and ambiguous equal positions produce no Silver mutation.

The topology writes append-oriented Bronze and Quarantine using Flink filesystem Parquet tables. It creates the approved HadoopCatalog at `mdep`, database `silver`, and V2 Iceberg tables `core_customers`, `core_products`, `core_orders`, and `core_payments`; all retain source LSN, `source_tx_id`, Debezium `transaction_id`, `transaction_total_order`, `transaction_data_collection_order`, source-time, transport, and `applied_at` audit fields. `preferred_language` is nullable in `core_customers`.

For one key, freshness is: PostgreSQL `source_lsn`; then, only for the same Debezium transaction with both values present, `transaction.total_order`; then known topic/partition/offset identity for exact replay detection. Neither a Kafka partition number nor event-time watermark is a database freshness clock. Initial-snapshot `r` records normally lack transaction metadata; they are accepted as the first value for their key, while later equal-position ambiguity is conservatively ignored.

## PyFlink Kafka metadata limitation

Flink 1.20 PyFlink's public `KafkaSource` wrapper supports value-only deserialization, not a Python record deserializer exposing Kafka key, partition, and offset. Topic is accurately preserved because the job creates one source per known topic. The primary key is derived from Debezium `after`/`before`; partition and offset are null rather than invented. A metadata-capable Java deserializer may use the same `CdcEvent` contract later.

**Static checks passed:** Python compilation, 18 model/topology tests, PowerShell parser, and Git whitespace check. **Runtime unvalidated:** Docker build, Kafka, Debezium transaction ordering, Flink state/checkpoint recovery, S3 Parquet, and Iceberg commits have not run on this host.
