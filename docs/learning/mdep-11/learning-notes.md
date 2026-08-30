# MDEP-11 learning notes

- `cdc_model.py` is a pure test oracle. `CdcStateApplier` is the real checkpointed Flink `ValueState` implementation.
- A newer delete emits the stored prior row as `RowKind.DELETE`, clears current-row state, and retains the latest LSN to block a stale resurrection.
- Exact replays, lower LSNs, and same-LSN different transport coordinates do not mutate Silver. Raw Bronze preserves their evidence.
- Filesystem Parquet is used for Bronze and Quarantine. Iceberg V2 tables use primary keys and `write.upsert.enabled`; their physical behavior remains runtime unvalidated.
- The existing S3-backed HadoopCatalog is retained. No Glue, Hive, REST catalog, Databricks, or alternative table format was introduced.
