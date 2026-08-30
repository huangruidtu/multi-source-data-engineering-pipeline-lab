# MDEP-11 interview Q&A

## Why Flink rather than Spark for CDC?

**Direct answer:** Flink handles unbounded keyed streams and durable checkpoints; Spark remains the batch reference writer. **Project example:** four Debezium topics key state by entity plus primary key. **Follow-up:** “Exactly once?” The code requests checkpointed processing, but end-to-end guarantees are unvalidated until Kafka and Iceberg commits are observed. **Senior extension:** align Kafka offsets, state snapshots, and sink commits.

## How do you reject stale and duplicate CDC events?

**Direct answer:** compare source LSN per `entity:primary_key` and apply only a higher LSN. **Project example:** lower LSN is ignored even if it arrives later; the same topic/partition/offset is an exact replay; the same LSN from another transport coordinate is not considered newer. **Follow-up:** “Why not partition number?” A Kafka partition is not a global database clock. **Senior extension:** retain source position for audit/replay and validate behavior under failover.

## What are checkpoint, savepoint, watermark, and tombstone?

**Direct answer:** checkpoint is automatic recovery state; savepoint is manual migration state; watermark controls time-based lateness; tombstone is a null Kafka record. **Project example:** 30-second checkpoint and 60-second watermark; tombstone does not delete Silver. **Follow-up:** “What deletes it?” a newer Debezium `d` event. **Senior extension:** test sink commit and rescaling recovery in a real cluster.
