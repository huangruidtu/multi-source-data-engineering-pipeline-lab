# MDEP-11 interview Q&A

## Why Flink rather than Spark for CDC?

**Direct answer:** Flink handles unbounded keyed streams and durable checkpoints; Spark remains the batch reference writer. **Project example:** four Debezium topics key state by entity plus primary key. **Follow-up:** “Exactly once?” The code requests checkpointed processing, but end-to-end guarantees are unvalidated until Kafka and Iceberg commits are observed. **Senior extension:** align Kafka offsets, state snapshots, and sink commits.

## How do you reject stale and duplicate CDC events?

**Direct answer:** compare source LSN per `entity:primary_key`, then same-transaction Debezium total order when available. **Project example:** lower LSN is ignored; a higher `transaction.total_order` can apply at equal LSN; identical known topic/partition/offset is replay only. **Follow-up:** “Why not partition number?” A Kafka partition is not a global database clock. **Senior extension:** retain source position for audit/replay and validate behavior under failover.

## Can two CDC events with the same LSN still need ordering?

**Direct answer:** Yes. When Debezium reports both events in the same transaction and supplies `transaction.total_order`, that order distinguishes them. **Project example:** MDEP-11 applies the larger valid total order at equal LSN, but rejects a lower one. **Follow-up:** “What if metadata is absent?” A known equal topic/partition/offset is an exact replay; otherwise the event is an ambiguous equal-position conflict and does not mutate Silver. **Senior extension:** PostgreSQL LSN is database-log order, Debezium total order is intra-transaction order, Kafka coordinates are transport identity, and watermarks only express time/lateness.

## What are checkpoint, savepoint, watermark, and tombstone?

**Direct answer:** checkpoint is automatic recovery state; savepoint is manual migration state; watermark controls time-based lateness; tombstone is a null Kafka record. **Project example:** 30-second checkpoint and 60-second watermark; tombstone does not delete Silver. **Follow-up:** “What deletes it?” a newer Debezium `d` event. **Senior extension:** test sink commit and rescaling recovery in a real cluster.
