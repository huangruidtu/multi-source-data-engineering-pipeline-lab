# Module 05 — Core concepts

## Keyed state and checkpoints
1. **Definition:** `keyBy` partitions records; `ValueState` persists per-key data; checkpoints persist recoverable job state. 2. **Why:** current state needs one decision history per source key. 3. **MDEP files:** `flink_cdc_job.py`, `cdc_model.py`. 4. **How:** `CdcStateApplier` stores last version/current row. 5. **Misunderstanding:** checkpoint settings prove exactly once. 6. **Failure:** missing/failed checkpoint changes recovery semantics. 7. **Production:** observe checkpoint duration/failures/state size. 8. **Interview:** savepoints are operationally managed snapshots; checkpoints are recovery mechanism.

## CDC ordering versus event time
`version_decision` accepts higher LSN, then higher `transaction.total_order` for the same transaction, then only exact known topic/partition/offset as replay. Equal unknown positions are conservatively rejected. Watermarks use source event time for lateness analysis, not correctness; Kafka partition number is never freshness.

## Delivery semantics
The job configures exactly-once checkpoints and fixed-delay restart. End-to-end exactly-once depends on source offsets, checkpoint completion and sink commit behavior, which is **RUNTIME UNVALIDATED**. At-least-once replay must still converge through state rules.

## Flink application, JobManager, TaskManager, slots, and operators

### Definition

A Flink application is a submitted job graph. The JobManager coordinates scheduling, checkpoints, recovery and job lifecycle. TaskManagers provide execution slots, and operators—source, process, `keyBy`, and sink—run in those slots. A `DataStream` is the logical stream flowing through that graph.

### Why it exists

An unbounded CDC pipeline needs a long-lived runtime that can retain keyed state and coordinate recovery. A bounded batch engine can recompute a known input, but a stateful consumer must remember what it already applied after a restart.

### MDEP mapping

`processing/flink/flink_cdc_job.py:run` creates `StreamExecutionEnvironment`, a `KafkaSource` for each topic, parser/stateful operators, and Table API sinks. `env.set_parallelism(1)` is a lab simplification for inspectability; it is not a production capacity claim. Production parallelism should follow measured key distribution, checkpoint duration and sink throughput.

### Common misunderstanding

More slots or partitions may improve throughput but cannot decide business freshness. The correctness invariant remains the per-key version decision.

## Keyed partitioning and managed state

### Definition and mechanism

`key_by(lambda value: key_identity(...))` routes all records for one logical key to the same state owner. `key_identity` returns `entity:primary_key`, so `customers:1` and `products:1` cannot collide. `CdcStateApplier.open` creates two `ValueState` values: `last-applied-cdc-version` protects ordering, while `current-cdc-row` makes a delete capable of emitting a previous row before it clears state.

### MDEP mapping

The pure dictionary implementation in `apply_current_state` is a test oracle, not a runtime state backend. The PyFlink job uses `ValueStateDescriptor` and serialised events. That separation is deliberate: the rule is unit-testable without a JVM, while the topology owns durable managed state.

### General concept — NOT IMPLEMENTED IN MDEP

`ListState` retains an ordered collection; `MapState` retains multiple entries within one key. They suit sessions or per-key collections. MDEP needs one current row and one last version, so `ValueState` is the narrowest state shape. RocksDB tuning, state TTL, and state-backend selection are general production concepts and not configured or runtime-tested here.

## CDC event contract and source position

`CdcEvent` retains the information needed to make state decisions rather than only the latest payload. `entity` and `primary_key` identify the state owner; `operation`, `before`, and `after` describe the mutation; `source_lsn` is the first ordering key; `source_tx_id`, Debezium `transaction_id`, `transaction_total_order`, and `transaction_data_collection_order` retain distinct transaction context; event time supports time analysis; Kafka coordinates describe transport identity; and the original envelope remains evidence.

PostgreSQL LSN is hexadecimal `X/Y`. `parse_lsn` shifts the high 32-bit component and adds the low component, producing an orderable integer. It is not interchangeable with Kafka offset: the former describes database source position, while the latter describes position only in one Kafka partition.

## The MDEP CDC version decision

`version_decision` has five deliberate outcomes. No stored event or a higher LSN is `NEWER`; lower LSN is stale. With equal LSN, it compares `transaction.total_order` only when transaction IDs match and both values exist. Identical known topic/partition/offset becomes `EXACT_REPLAY`. Every other equal-position situation is `EQUAL_POSITION_CONFLICT` and does not mutate Silver.

This is conservative by design. Missing partition/offset cannot prove two messages are the same replay, and Kafka partition *number* is never a database freshness order. Rejecting ambiguity may require an operational investigation; accepting it could silently corrupt current state. The equal-LSN transaction rule is a real MDEP-11 review correction, protected by `tests/test_flink_cdc_model.py`.

## Snapshot, delete, tombstone, and event time

Debezium `r` records are initial snapshot reads and may lack transaction metadata. MDEP accepts a first snapshot state. `d` requires `after=null`, emits a DELETE row for an existing current row, and clears state. A null Kafka tombstone is a compaction marker—not another business delete—so it is preserved in Bronze and ignored for current-state mutation.

`SourceEventTimestampAssigner` extracts source event time and uses a 60-second bounded-out-of-orderness watermark. Event time supports late-data analysis; processing time is when Flink handles the record; ingestion time is a runtime-assigned intermediary notion. A watermark is never used to accept CDC state: a late higher-LSN event can be valid, and an on-time lower-LSN event can be stale. Idleness and event-time windows are **GENERAL / NOT IMPLEMENTED IN MDEP**.

## Checkpoints, savepoints, restart, and delivery

The job configures exactly-once checkpoint *mode* every 30 seconds, two-minute timeout, five-second minimum pause, three tolerable failures, and fixed-delay restart. Checkpoint barriers form a consistent recoverable cut across operators. A savepoint is an intentionally triggered operational snapshot; a checkpoint is recurring fault-recovery state. Aligned versus unaligned checkpoint tuning, max parallelism, rescaling, state TTL, backpressure metrics and RocksDB configuration are useful **GENERAL / NOT IMPLEMENTED** topics.

Exactly-once is not a label earned by `CheckpointingMode.EXACTLY_ONCE`. It would require observed successful checkpoints, Kafka offset coordination and Iceberg sink commit behaviour. Those are **RUNTIME UNVALIDATED**. MDEP nevertheless designs for at-least-once replay convergence through the deterministic state rule.

## Interview framing

Say: “MDEP keys state by entity and primary key, accepts source LSN first, applies same-transaction total order only for equal LSN, and uses Kafka coordinates only to prove an exact replay.” Do not say Flink globally orders Kafka or that checkpoint configuration proves end-to-end exactly once.
