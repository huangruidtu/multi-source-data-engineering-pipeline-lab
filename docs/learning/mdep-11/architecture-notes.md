# MDEP-11 architecture notes

Flink is used for unbounded Kafka CDC because keyed state and checkpoints are native stream-processing concerns; Spark owns bounded reference batch work. Kafka offsets identify transport position, but PostgreSQL LSN is the stronger change-order fact. Keying by PostgreSQL primary key makes last-applied state local to that key; it does not create global order across partitions or transactions.

The job configures 30-second checkpoints in a local volume. On a successful checkpoint, Flink persists source offsets and keyed operator state together; a checkpoint-aware Iceberg sink should commit only with the corresponding completed checkpoint. This design targets recoverable at-least-once transport and logically idempotent state—not an unvalidated exactly-once claim. A savepoint is intentionally separate: it is a user-triggered state capture for upgrade/rescaling.

Event time uses Debezium source time for event-date partitioning and a bounded 60-second watermark/late classification exercise. It cannot decide whether a CDC mutation is newer: a late event with a higher LSN may still be valid, while an on-time event with a lower LSN is stale. Backpressure, lag, checkpoint duration, state size, Iceberg commit failures, and slot/WAL retention are runtime concerns to inspect before scaling parallelism.
