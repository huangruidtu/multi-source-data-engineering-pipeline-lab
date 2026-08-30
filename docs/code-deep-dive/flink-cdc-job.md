# Code Deep-Dive: `processing/flink/flink_cdc_job.py`

**Source of truth:** [`processing/flink/flink_cdc_job.py`](../../processing/flink/flink_cdc_job.py), read with [`cdc_model.py`](cdc-model.md). The file defines a runnable PyFlink topology, but its Kafka/Flink/Iceberg runtime behavior is **MDEP RUNTIME DEFERRED — not runtime validated in V1**.

## 1. Why this file exists

This MDEP-11 job is the physical streaming counterpart to the pure CDC model. It describes a Kafka source for the four commerce topics, archives raw CDC evidence to Bronze, parses valid events, routes invalid/stale events to side outputs, maintains keyed current state in managed Flink state, and writes changelog rows to Iceberg Silver tables.

## 2. Where it sits in the architecture

`PostgreSQL logical replication -> Debezium -> Kafka -> KafkaSource -> raw CDC Bronze -> parser -> keyed state -> Iceberg Silver`.

The raw archive and current state are separate outputs: raw Bronze is evidence-first, while Silver is the latest accepted current row for each `entity:primary_key`. The job does not transform batch REST/file data; that boundary belongs to Spark.

## 3. Inputs, outputs, and state

| Layer | Contract |
|---|---|
| Source | Topics `mdep.commerce.{customers,products,orders,payments}`, earliest offsets, explicit consumer group. |
| Raw Bronze | Filesystem Parquet under `bronze/cdc/<entity>/event_date=...`. |
| Quarantine | Filesystem Parquet under `quarantine/cdc/<entity>/event_date=...`. |
| Silver | Four Iceberg upsert-enabled tables `mdep.silver.core_*`. |
| State | `ValueState` for last accepted version plus `ValueState` for current row, keyed by `entity:primary_key`. |

Environment variables parameterize Kafka bootstrap, Bronze root, quarantine root, and Iceberg warehouse; documented defaults support the intended Docker network but do not prove it runs there.

## 4. Important symbols

| Symbol | Meaning |
|---|---|
| `TOPICS`, `SILVER_TABLES` | Explicit owned inputs and outputs. |
| checkpoint constants | 30s interval, 120s timeout, 5s minimum pause. |
| `topology_spec` | Purely inspectable summary used by tests/documentation. |
| `silver_table_ddls` | Concrete Iceberg schemas and upsert properties. |
| `_bronze_values` | Archive-first parser: malformed content still lands as unparsed evidence. |
| `DebeziumParser` | Converts valid raw messages into normalized events; emits quarantine side output on failure. |
| `CdcStateApplier` | Stateful operator that uses `version_decision` before producing a changelog row. |
| `SourceEventTimestampAssigner` | Event-time metadata/watermark support, not the state freshness rule. |

## 5. Execution flow

1. `run` imports PyFlink lazily so tests can import module-level contracts without JVM dependencies.
2. `KafkaSource` subscribes to the four explicit topics, starts at earliest, and emits value strings.
3. Each value is wrapped with topic and processing time, then immediately mapped to raw Bronze values. This archives tombstones and malformed payloads as evidence.
4. `DebeziumParser` turns non-tombstone valid values into encoded `CdcEvent`s. Parse failures go to the quarantine side output.
5. A bounded-out-of-orderness watermark is assigned for event-time concerns, then events are keyed by `entity:primary_key`.
6. `CdcStateApplier` compares against last version, emits stale/replay/conflict events to a side output, and emits insert/update/delete changelog rows for accepted transitions.
7. Table environment DDLs configure filesystem Bronze/quarantine sinks and Iceberg catalog/tables. Changelog rows become dynamic tables and execute with the table statements.

## 6. Function-by-function walkthrough

### `topology_spec` and `silver_table_ddls`

`topology_spec` makes operational choices reviewable: earliest starting offsets, consumer group, checkpoint values, layouts, state layout, source-order rule, and a key warning that watermarks never decide CDC state acceptance. `silver_table_ddls` provides one primary-key-not-enforced table per entity, includes source/transaction/transport evidence, and enables Iceberg upsert write behavior.

### Raw-message helpers

`_raw_message` attaches `processed_at` before parsing. `_bronze_values` tries `parse_debezium`: if valid, it writes normalized metadata and original envelope; if the value is a tombstone, it writes a tombstone marker; if malformed, it writes `unparsed` with original payload. This ordering is deliberate: an operational parse failure must not erase raw evidence.

### `DebeziumParser`

Tombstones are valid compaction markers, so the parser sends a `tombstone_ignored` record to stale/duplicate output rather than quarantine. Invalid JSON, invalid contract shape, or unknown entities are quarantined with the concrete exception reason. Valid normalized events are encoded for the stateful step.

### `CdcStateApplier`

`open` declares two managed `ValueStateDescriptor`s. In `process_element`, it unpickles prior version, delegates freshness to `version_decision`, and emits an explainable side-output record if not newer. On accepted delete it emits `RowKind.DELETE` only when there was a prior current row, then clears current state. On accepted non-delete it writes the event and emits `INSERT` for a first row or `UPDATE_AFTER` for a replacement.

### `_to_silver_row`

The function flattens table-specific `after` fields and carries source LSN, transaction identity/order, source event time, optional transport metadata, and applied-at time. For deletes it receives the **previous** event because the delete event has no after-image; this is what lets the sink identify the row to retract.

### Checkpoint, watermark, Kafka, and table wiring

`env.enable_checkpointing(..., EXACTLY_ONCE)` requests exactly-once checkpointing, while timeout/min-pause/restart strategy express a recoverable job configuration. `KafkaSource` uses `SimpleStringSchema`; current code therefore has value-only deserialization and passes `None` for key/partition/offset into the parser. The source event timestamp/watermark is useful for event-time observation but never replaces LSN for state acceptance. The table environment receives raw/quarantine filesystem DDLs and a Hadoop Iceberg catalog before executing sink statements.

## 7. Critical code-block reasoning

The source-to-Bronze branch occurs before the parser/state branch. That means malformed values are still observable as raw `unparsed` Bronze records; routing a parse failure directly to quarantine would lose the raw CDC archive's evidence role.

`key_by(lambda encoded: key_identity(event_from_json(encoded)))` ensures state is isolated by entity and primary key. The code does not key by Kafka partition, topic alone, or a bare ID—all would create incorrect state sharing or ordering assumptions.

In `CdcStateApplier`, the `decision.value != "newer"` guard appears before any `ValueState` update. That one placement enforces replay safety: stale deletes cannot clear a newer row, and a known replay cannot emit an extra upsert. The actual comparison is intentionally imported from `cdc_model.py`, avoiding duplicate ordering implementations.

`SourceEventTimestampAssigner` must not be confused with freshness. Late event time can be a reason to observe/window a stream, but a CDC current-state record is accepted according to source WAL position and transaction ordering. The constants and `topology_spec` make this distinction explicit.

## 8. Correctness invariants

- Only four approved commerce topics/tables enter this topology.
- Raw CDC evidence is archived even when parsing fails.
- Invalid payloads are quarantined; tombstones are not mislabeled malformed.
- State is one cell per entity plus primary key.
- Every state mutation passes the pure LSN/transaction/replay decision.
- Delete emits a changelog retraction and clears current row only if it is newer.
- Watermark/event time does not override source-order acceptance.

## 9. Failure behavior

Malformed messages produce quarantine records with original payload/reason. Stale, duplicate, tombstone, and equal-position-conflict events go to the stale side output and do not mutate state. Connector/sink exceptions are not swallowed; a real runtime would rely on checkpoint/restart behavior, but V1 has not physically tested recovery, checkpoint commit alignment, or Iceberg sink commit behavior.

## 10. Tests that protect the behavior

[`tests/test_flink_topology.py`](../../tests/test_flink_topology.py) checks concrete topics, start offset, state declaration, checkpoint/watermark constants, paths, Iceberg DDL properties, and real-wiring constructs rather than an intentional runtime stub. [`tests/test_flink_cdc_model.py`](../../tests/test_flink_cdc_model.py) protects the decision logic the stateful operator calls. These are **MDEP OFFLINE TESTED** static/unit checks, not a submitted Flink job.

## 11. What is not implemented / runtime deferred

**MDEP RUNTIME DEFERRED:** Kafka broker connectivity, actual Debezium envelopes, consumer offsets, checkpoint restoration, restart behavior, Kafka-to-Iceberg exactly-once effects, S3 filesystem sink writes, Iceberg catalog commits, and measuring watermark/lag behavior. The current value-only `SimpleStringSchema` source also means runtime Kafka key/partition/offset are not preserved in the event object.

## 12. Production concepts beyond current code

**GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED:** metadata-aware Kafka deserialization, schema registry compatibility, observability/alerts, scalable checkpoint storage, autoscaling, secret management, and a production policy for conflict triage. A physical job should also demonstrate end-to-end delivery semantics rather than inferring them from `EXACTLY_ONCE` configuration.

## 13. Common misunderstandings

- "Exactly-once configuration proves exactly-once output." No; it requests a runtime mode and needs real source/sink/checkpoint validation.
- "The watermark decides newest current state." No; source LSN/transaction order does.
- "Tombstones are bad data." No; they are valid Kafka compaction signals.
- "A primary key declared `NOT ENFORCED` is an engine-enforced uniqueness guarantee." No; the keyed state and upsert/changelog contract are responsible for semantics.

## 14. Interview questions

**Why archive raw CDC before parsing?** To retain evidence for malformed and unexpected inputs. Bronze records what arrived; quarantine records why it could not satisfy the normalized contract.

**How do you handle a delete with no after-image?** The stateful operator uses its previously stored current event to build a delete changelog row, then clears current state—only after the delete passes source-version ordering.

**What is the practical limitation of this Kafka source?** It uses a value-only schema, so it cannot currently prove exact replay from actual Kafka key/partition/offset metadata. The pure model treats absent identity conservatively rather than pretending it is available.

## 15. 30-second spoken explanation

“`flink_cdc_job.py` defines the commerce streaming topology: Kafka input, raw CDC Bronze archive, validated parser with quarantine side output, LSN-ordered keyed state, and Iceberg current-state changelog sinks. The job delegates freshness to a pure model, so stale replays and stale deletes cannot mutate state. The topology and its contracts are statically/unit tested, while all physical Kafka, Flink, S3, and Iceberg execution remains runtime deferred.”

## 16. Senior follow-up discussion

The right senior discussion is delivery semantics across the whole chain. Checkpointing plus Iceberg upsert configuration is not sufficient evidence by itself. Ask how offsets, managed state, raw files, and Iceberg commits are coordinated on restart; then identify which pieces MDEP configured versus which require an actual fault-injection runtime lab before making an exactly-once claim.
