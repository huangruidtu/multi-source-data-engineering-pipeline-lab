# Solutions — 05 Flink streaming workbook

## FS-01
**Correct answer:** Kafka source/raw-message branch first receives value. `_bronze_values` catches malformed JSON and marks/archive-shapes it as `unparsed`; `DebeziumParser` catches the error and sends a reasoned quarantine side output. No `CdcEvent`, watermark/keyBy/ValueState transition, or Silver changelog mutation occurs.

**Relevant logic/invariant:** raw archive is before semantic parse; malformed is parser failure, not stale/replay `VersionDecision`. Invariant: unparseable bytes remain evidence but cannot become current state.

**Common wrong answer:** Say quarantine means discard, or say stale decision rejects malformed JSON. **Production consequence:** losing raw bytes prevents incident diagnosis; accepting them risks corrupt state.

**Test suggestion:** static test verifies topology contains source/parser/side output/filesystem wiring; pure parser test asserts malformed input raises/gets rejected. **Interview answer:** “We archive raw CDC evidence before semantic parsing. A malformed message is quarantined with evidence and never reaches keyed state or Silver.”

**Senior follow-up:** actual raw-file and side-output delivery is **MDEP RUNTIME DEFERRED** and needs a submitted-job malformed-message exercise.

## FS-02
**Correct answer:** both textual `1001` values enter one Flink key, so one entity can read/replace another entity’s `last_version` and `current_row`; an order LSN can incorrectly compare with customer state and deletes can retract the wrong current row shape.

**Relevant logic/invariant:** `key_identity(event)` returns `entity:primary_key`; job keys encoded events using that identity. Invariant: managed state is isolated per entity/business key.

**Common wrong answer:** primary keys are globally unique across tables. **Production consequence:** cross-entity state corruption is subtle and may survive until a delete/replay arrives.

**Test suggestion:** build customer/product or customer/order events with same textual key and assert distinct identity; inspect job source for `key_identity`. **Interview answer:** “A database primary key is table-local. I key Flink state by entity plus primary key so two tables cannot share version or row state.”

**Senior follow-up:** **GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED:** validate input topic/entity consistency and monitor unexpected key cardinality/collisions.

## FS-03
**Correct answer:** reject the strong claim. **MDEP IMPLEMENTED:** exact-once checkpoint mode is requested, interval/timeout/min-pause and restart/topology configuration exist. **MDEP OFFLINE TESTED:** static topology contract and pure CDC decision tests. **MDEP RUNTIME DEFERRED:** Kafka consumption, checkpoint completion, restore, source progress plus ValueState recovery, filesystem/Iceberg commits, and end-to-end duplicate/loss proof.

**Relevant logic/invariant:** configuration expresses intended recovery/delivery behavior; it is not execution evidence. **Common wrong answer:** `CheckpointingMode.EXACTLY_ONCE` proves every source/sink path exactly once.

**Production consequence:** an unsupported claim can hide duplicate/loss behavior after failure. **Test experiment:** inject failure after records flow, wait for checkpoint, restart, compare source offsets, current state, raw archive, and Iceberg commits for no loss/duplicate. **Interview answer:** “The job is configured for exactly-once checkpointing, but I would not claim end-to-end exactly-once until I validate failure recovery across Kafka, state, and Iceberg sinks.”

**Senior follow-up:** define how you would correlate checkpoint IDs, Kafka offsets, sink commits, and duplicate business keys during a recovery drill.
