# Module 05 — Core concepts

## Keyed state and checkpoints
1. **Definition:** `keyBy` partitions records; `ValueState` persists per-key data; checkpoints persist recoverable job state. 2. **Why:** current state needs one decision history per source key. 3. **MDEP files:** `flink_cdc_job.py`, `cdc_model.py`. 4. **How:** `CdcStateApplier` stores last version/current row. 5. **Misunderstanding:** checkpoint settings prove exactly once. 6. **Failure:** missing/failed checkpoint changes recovery semantics. 7. **Production:** observe checkpoint duration/failures/state size. 8. **Interview:** savepoints are operationally managed snapshots; checkpoints are recovery mechanism.

## CDC ordering versus event time
`version_decision` accepts higher LSN, then higher `transaction.total_order` for the same transaction, then only exact known topic/partition/offset as replay. Equal unknown positions are conservatively rejected. Watermarks use source event time for lateness analysis, not correctness; Kafka partition number is never freshness.

## Delivery semantics
The job configures exactly-once checkpoints and fixed-delay restart. End-to-end exactly-once depends on source offsets, checkpoint completion and sink commit behavior, which is **RUNTIME UNVALIDATED**. At-least-once replay must still converge through state rules.
