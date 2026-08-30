# MDEP-11 implementation guide

MDEP-11 is the canonical CDC current-state writer: Kafka Debezium topics feed Flink, which preserves append-oriented evidence at `bronze/cdc/<entity>/event_date=...` and applies only newer events to `mdep.silver.core_customers`, `core_products`, `core_orders`, and `core_payments`. Spark remains restricted to batch reference tables.

`processing/flink/cdc_model.py` is the actual normalized event contract: entity, primary key, `before`/`after`, operation, parsed source LSN, transaction/source time, topic/partition/offset, snapshot marker, and original envelope. Malformed records raise a reason for Quarantine; Kafka null tombstones are classified and ignored for Silver. `flink_cdc_job.py` records the topology contract and explicit 30-second checkpoint configuration; runtime submission is deliberately blocked until documented Kafka/Iceberg connector jars are provided.

Per key, `(source_lsn, kafka_partition, kafka_offset)` is the version tuple. LSN is primary database order; partition/offset are only transport tie-breakers. A lower/equal tuple is ignored, including exact replay. `r`, `c`, and `u` upsert `after`; newer `d` physically deletes. Bronze preserves the original envelope regardless. `preferred_language` is retained in the generic `after` object, so the MDEP-10 additive field can flow into the customer Iceberg schema once the runtime sink is installed.

Docker Compose adds JobManager and TaskManager with local mounted checkpoint/savepoint directories. Checkpoints are automatic recovery state and contain Kafka source/operator state; savepoints are manually triggered upgrade/migration state. Neither has been exercised.
