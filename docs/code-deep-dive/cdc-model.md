# Code Deep-Dive: `processing/flink/cdc_model.py`

**Source of truth:** [`processing/flink/cdc_model.py`](../../processing/flink/cdc_model.py) on `main`.  This is a code walkthrough, not evidence that the Flink runtime has been executed.

## Read beside

- **Source file:** [`processing/flink/cdc_model.py`](../../processing/flink/cdc_model.py)
- **Test file:** [`tests/test_flink_cdc_model.py`](../../tests/test_flink_cdc_model.py)
- **Related architecture:** [`docs/finalization/end-to-end-data-flow.md`](../finalization/end-to-end-data-flow.md)
- **Related interview topic:** [`docs/finalization/interview-qa.md`](../finalization/interview-qa.md) — CDC ordering, replay, and delete semantics

## 1. Why this file exists

The Flink job needs a small set of correctness rules that are too important to hide inside a JVM-dependent streaming topology. This module turns a Debezium message into a typed `CdcEvent`, decides whether it can replace the current version for one business key, and offers an in-memory oracle for tests. Keeping those rules pure means a reviewer can test CDC ordering without Kafka, PyFlink, Iceberg, or Docker.

**MDEP IMPLEMENTED / OFFLINE TESTED:** parsing, LSN ordering, same-transaction ordering, replay detection, delete behavior, and state transition rules.  **MDEP RUNTIME DEFERRED:** consuming actual Kafka records and persisting Flink state/checkpoints.

## 2. Where it sits in the architecture

`PostgreSQL WAL -> Debezium envelope -> Kafka topic -> parse_debezium -> keyed Flink state -> Iceberg current-state table`.

This file occupies the semantic boundary between an external CDC envelope and MDEP's internal current-state rule. `flink_cdc_job.py` owns the physical source, side outputs, state descriptors, and sinks; this file owns the decision that says whether a candidate event is newer. It deliberately does **not** own batch-owned `locations` or `exchange_rates`—those use timestamp/hash ordering in `processing/spark/contracts.py`.

## 3. Inputs, outputs, and state

| Item | Meaning |
|---|---|
| Input | Debezium JSON key/value, Kafka topic, optional partition and offset. |
| Normalized output | Immutable `CdcEvent`, or `None` for a Kafka tombstone. |
| Decision output | `VersionDecision`: newer, stale by LSN/order, exact replay, or unresolved equal-position conflict. |
| State model | `state[key]` is the current business row; `versions[key]` is the last accepted `CdcEvent`. The production equivalent uses two Flink `ValueState`s. |

The event contains `before`, `after`, `source_lsn`, source and Debezium transaction metadata, snapshot flag, and optional Kafka coordinates. The raw JSON is retained as `envelope`; that supports raw CDC Bronze evidence downstream.

## 4. Important symbols

| Symbol | Job in the contract |
|---|---|
| `CDC_ENTITIES`, `PRIMARY_KEY` | Explicit ownership and table-specific primary-key field. |
| `CdcEvent` | Typed normalized contract; frozen to prevent accidental mutation after comparison. |
| `VersionDecision` | Makes ignored events explainable instead of returning only `False`. |
| `parse_lsn` | Converts PostgreSQL's hexadecimal `X/Y` location into one orderable integer. |
| `parse_debezium` | Validates/normalizes a Debezium event. |
| `key_identity` | Uses `entity:primary_key`, avoiding cross-table textual-ID collisions. |
| `version_decision` | The source-of-truth current-state winner rule. |
| `apply_current_state` | Pure test oracle, not the actual Flink state backend. |

## 5. Execution flow

1. A topic suffix identifies an allowed entity, such as `mdep.commerce.orders`.
2. `parse_debezium` parses JSON, validates `op`, `source.lsn`, and delete shape, then resolves the primary key from Kafka key, `after`, or `before`.
3. A caller keys the event with `orders:1001`.
4. `version_decision(candidate, last)` compares source position before modifying state.
5. Only `NEWER` writes the version. A delete removes the current row; an insert/update replaces it.
6. Tombstones are deliberately ignored for current state, because a Kafka tombstone is a compaction marker, not a second business delete.

## 6. Function-by-function walkthrough

### `parse_lsn(value)`

PostgreSQL logical positions are commonly represented as two hexadecimal halves such as `0/16B6C50`. The expression `(high << 32) + low` makes the high half dominate numerically and preserves WAL position ordering. Passing an integer supports a record that has already been normalized. A malformed string raises rather than guessing; accepting a wrong source position could regress current state.

### `_primary_from_payload(...)`

The lookup order is Kafka key, then `after`, then `before`. That is significant for deletes: Debezium deletes normally have `after=null`, so the primary key must still be recoverable from the key or before-image. If every source is missing it, the event cannot be safely keyed and is rejected.

### `_optional_order(value, field)`

Debezium transaction metadata may be absent. When supplied, `total_order` must be integer-like; silently treating an invalid value as absent would turn corrupt ordering metadata into an arbitrary equal-position outcome.

### `parse_debezium(...)`

This is intentionally strict:

- `None` value returns `None`, marking a tombstone.
- Only `r`, `c`, `u`, and `d` are accepted. `r` is a snapshot-read event.
- A delete must have `after=null`; otherwise the producer contract is inconsistent.
- `source.lsn` is mandatory because MDEP's CDC freshness rule is WAL-first.
- The snapshot field accepts Debezium's boolean/string forms used by the project.

The function does not invent a Kafka key when the PyFlink source is value-only. In the current runtime topology, partition/offset are `None` and the primary key is recovered from payload. That limitation is documented, not concealed.

### `key_identity(event)`

`f"{event.entity}:{event.primary_key}"` prevents `customers:42` and `products:42` from sharing a state cell. A bare identifier might look sufficient in sample data but is unsafe once table-local IDs overlap.

### `version_decision(event, last)`

This is the critical block. Its precedence is:

1. No previous event, or a strictly higher `source_lsn`: `NEWER`.
2. Lower LSN: `LOWER_LSN`—ignore it even if Kafka metadata differs.
3. Same LSN and same known Debezium transaction: compare `transaction_total_order`.
4. Only when both topic/partition/offset triples are present and equal: `EXACT_REPLAY`.
5. Otherwise: `EQUAL_POSITION_CONFLICT`.

Two subtleties matter in interview discussion. First, a Kafka **partition number** is never compared as a freshness clock; partitions are transport topology, not a global source order. Second, missing transport coordinates cannot prove exact replay. MDEP chooses conservative non-application for an equal source position that it cannot identify safely.

### `apply_current_state(state, versions, event)`

This mirrors the production transition in plain dictionaries. It records a version before applying a row, so a later stale delete cannot remove a newer current row. On `d`, it removes only the current-state payload and returns `deleted`. It returns a descriptive ignored outcome for stale/replayed/conflicting candidates, which tests can assert directly.

## 7. Critical code-block reasoning

`same_transaction` requires non-null matching `transaction_id` values before `total_order` has authority. A numeric order by itself is not globally meaningful across transactions. The code then uses `total_order`, not `data_collection_order`, as the implemented second comparison. `data_collection_order` is preserved for evidence and future analysis but is not silently promoted to a rule the project has not defined.

`_same_known_transport` requires **both** events to have both coordinates. This avoids a dangerous shortcut: treating two `None` metadata fields as equal would declare distinct value-only messages idempotent replays. The result is an explicit conflict instead.

`event_to_json` uses `asdict` and sorted JSON because the physical Flink operator serializes the pure contract between stages. `event_from_json` reconstructs it at the stateful boundary. This is a deliberately inspectable bridge; it is not a claim of a schema-registry-based production event contract.

## 8. Correctness invariants

- Only the four commerce entities can enter this CDC current-state path.
- Every accepted event has a primary key and a `source_lsn`.
- A lower WAL position cannot overwrite or delete a higher WAL position.
- A same-transaction lower total order cannot overwrite a higher total order.
- An exact known transport replay does not mutate state.
- A tombstone does not itself delete Silver state.
- Key identity includes entity and primary key.

## 9. Failure behavior

Malformed JSON is not handled in this file beyond standard JSON exceptions; the Flink topology catches parse failures and sends evidence to quarantine. Unknown topic entities, unsupported operations, absent LSNs, invalid transaction orders, and malformed deletes raise `ValueError` so they cannot become plausible current state. An equal-position event with unknown identity is **ignored**, not guessed to be newer; this favors non-regression over availability.

## 10. Tests that protect this behavior

[`tests/test_flink_cdc_model.py`](../../tests/test_flink_cdc_model.py) covers snapshot normalization, higher/lower LSNs, transaction ordering, exact replay, missing transport conflict, partition-number non-freshness, deletes, tombstones, composite keys, metadata preservation, and malformed inputs. The tests do not execute Kafka, Debezium, or PyFlink.

## 11. What is not implemented / runtime deferred

**MDEP RUNTIME DEFERRED:** actual Debezium payload observation, Kafka headers/keys/offsets from the current source, Flink checkpoint restoration, Iceberg commits, and a production resolution process for equal-position conflict. There is no claim that an actual PostgreSQL transaction has been consumed.

## 12. Production concepts beyond current code

**GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED:** a metadata-preserving Kafka deserializer could pass real key/partition/offset into `parse_debezium`; schema registry compatibility rules could guard envelope evolution; metrics and a durable dead-letter process could alert on equal-position conflict. Those would enhance operations but must not be represented as existing MDEP behavior.

## 13. Common misunderstandings

- "Kafka offset is globally ordered." No: it is ordered only within one partition.
- "A tombstone means delete the business row now." No: Debezium's delete event conveys the business delete; the tombstone supports compaction.
- "A higher partition means newer." No: partition assignment is not source chronology.
- "Same LSN always means duplicate." No: only known identical transport identity proves exact replay in this contract.

## 14. Interview questions

**How do you prevent a replayed old CDC record from regressing Silver?** I compare PostgreSQL LSN first and only apply a strictly higher source position. For same-transaction events at the same LSN I use Debezium `total_order`; exact known replays and unresolved equal-position candidates do not mutate state.

**Why keep a pure CDC model outside the Flink job?** It makes the most consequential ordering logic unit-testable without a cluster and lets the physical topology use exactly the same rule rather than duplicating it in opaque operator code.

**What happens when Kafka metadata is unavailable?** The implementation does not falsely identify a replay. It emits an equal-position conflict decision and refuses to update current state.

## 15. 30-second spoken explanation

“`cdc_model.py` is the pure correctness layer for the commerce CDC path. It normalizes Debezium events, turns PostgreSQL LSN into an orderable value, and decides whether an event is allowed to change one entity-plus-primary-key state. LSN is the primary clock; Debezium transaction order resolves same-transaction ties; Kafka identity only detects a known replay. This is unit-tested offline, while live Kafka and Flink execution are runtime deferred.”

## 16. Senior follow-up discussion

Ask what should happen for `EQUAL_POSITION_CONFLICT`. A senior answer should not claim a universal fix: first preserve raw evidence and quarantine/metric the ambiguity, then determine whether the source can provide a stable event ID, real transport metadata, or a stronger source-order contract. The correct choice depends on whether preserving stale state is safer than potentially applying an incorrect update.
