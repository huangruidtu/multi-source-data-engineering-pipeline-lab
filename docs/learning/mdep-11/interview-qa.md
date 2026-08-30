# MDEP-11 interview Q&A

## Why Flink rather than Spark for CDC?

**Direct answer:** Flink handles unbounded keyed streams and durable checkpoints; Spark remains the batch reference writer. **Project example:** four Debezium topics key state by primary key. **Follow-up:** “Exactly once?” Not claimed; state is logically idempotent. **Senior extension:** align Kafka offsets, state snapshots, and sink commits.

## How do you reject stale and duplicate CDC events?

**Direct answer:** compare `(source_lsn, partition, offset)` per key and apply only a greater tuple. **Project example:** lower LSN is ignored even if it arrives later; identical replay is ignored. **Follow-up:** “Why not timestamp?” source LSN is database order; timestamp is secondary evidence. **Senior extension:** retain source position for audit/replay and validate behavior under failover.

## What are checkpoint, savepoint, watermark, and tombstone?

**Direct answer:** checkpoint is automatic recovery state; savepoint is manual migration state; watermark controls time-based lateness; tombstone is a null Kafka record. **Project example:** 30-second checkpoint and 60-second watermark; tombstone does not delete Silver. **Follow-up:** “What deletes it?” a newer Debezium `d` event. **Senior extension:** test sink commit and rescaling recovery in a real cluster.
