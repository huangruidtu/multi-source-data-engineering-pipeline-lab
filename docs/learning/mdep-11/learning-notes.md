# MDEP-11 learning notes

- `cdc_model.py` is a pure test oracle. `CdcStateApplier` is the real checkpointed Flink `ValueState` implementation.
- A newer delete emits the stored prior row as `RowKind.DELETE`, clears current-row state, and retains the latest LSN to block a stale resurrection.
- PostgreSQL LSN orders database changes. For equal LSN in the same Debezium transaction, `transaction.total_order` orders changes inside that transaction. Kafka topic/partition/offset is transport identity only; event-time/watermark is lateness logic only.
- An exact replay requires known equal topic, partition, and offset. `None` partition/offset cannot establish replay; missing transaction/transport position becomes a conservative equal-position conflict and does not mutate Silver.
- Snapshot `r` events can lack transaction metadata and are accepted as initial per-key state. A later event still needs a higher LSN or valid same-transaction total-order evidence.
- Filesystem Parquet is used for Bronze and Quarantine. Iceberg V2 tables use primary keys and `write.upsert.enabled`; their physical behavior remains runtime unvalidated.
- The existing S3-backed HadoopCatalog is retained. No Glue, Hive, REST catalog, Databricks, or alternative table format was introduced.
