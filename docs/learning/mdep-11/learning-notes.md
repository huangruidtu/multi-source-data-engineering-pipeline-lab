# MDEP-11 learning notes

- **Keyed state:** Flink keys by source primary key and stores the last applied LSN/partition/offset, preventing replay/stale regression.
- **Checkpoint/savepoint:** checkpoints are automatic Kafka-offset and operator-state recovery; savepoints are deliberate migration/upgrade state.
- **Event time/watermark:** source timestamp drives date/late handling; a 60-second watermark does not replace LSN for current-state ordering.
- **Duplicate/stale/delete/tombstone:** same/lower version tuple is ignored; a newer delete removes state; a Kafka tombstone is a null transport record, not a second database delete.
- **Iceberg:** HadoopCatalog tables provide snapshot current state once the streaming sink runtime is installed. Checkpoint-aligned commits are expected connector behavior, not validated evidence.
- **Failure/scaling:** monitor source/Kafka lag, checkpoint time, backpressure, state growth, sink commits, and PostgreSQL replication-slot WAL retention before increasing parallelism or rescaling state.
