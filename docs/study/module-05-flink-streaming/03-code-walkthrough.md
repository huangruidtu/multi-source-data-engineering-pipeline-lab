# Module 05 — Code walkthrough

Reading order: `processing/flink/cdc_model.py` → `tests/test_flink_cdc_model.py` → `processing/flink/flink_cdc_job.py` → `tests/test_flink_topology.py` → `scripts/validate-mdep-11-flink-cdc.ps1`.

`parse_debezium` validates entity/envelope/LSN and produces `CdcEvent`. `key_identity` prevents cross-entity key collision. `version_decision` is the pure test oracle. The topology creates a `KafkaSource` per topic, writes Bronze before parsing, emits malformed input to a side-output quarantine, assigns watermarks, keys valid events, and uses `CdcStateApplier`. It handles `d` by emitting a delete and clearing current state; tombstones are archive/compaction markers and do not mutate Silver. Earlier topology/config-only work was replaced by this actual wiring, but physical job submission remains unvalidated.

## `processing/flink/cdc_model.py` — executable ordering specification

### Purpose and boundaries

This file is intentionally ordinary Python. It is not a mock of the topology: it defines the same parsing and acceptance rule that the PyFlink job invokes. The absence of PyFlink imports is a design choice that makes correctness reviewable with `tests/test_flink_cdc_model.py` even when a JVM cluster is unavailable.

### Entity, key, and event structure

`CDC_ENTITIES` is the fixed Flink ownership boundary. `PRIMARY_KEY` maps customers/products/orders/payments to their business identifiers. The frozen `CdcEvent` keeps each fact needed for a later decision: entity/key, operation, before/after images, source LSN, source transaction id, Debezium transaction id and orders, event timestamp, Kafka topic/partition/offset, snapshot flag and raw envelope. Retaining all of them avoids the common mistake of reducing a CDC event to only `after` before ordering has been decided.

### Parsing helpers

`parse_lsn` translates PostgreSQL `X/Y` hexadecimal into a sortable integer. `_primary_from_payload` tries Kafka key, `after`, then `before`; that fallback lets a delete identify its key even though its after-image is null. `_optional_order` rejects non-numeric transaction orders. `parse_debezium` turns a null Kafka value into `None` (tombstone); otherwise it rejects out-of-bound entities, invalid `r/c/u/d`, malformed deletes and missing LSN before constructing `CdcEvent`.

### Ordering and mutation helpers

`version_decision` is intentionally lexicographic: source LSN, then `transaction.total_order` only for the same transaction, then exact known transport identity. `_same_known_transport` requires full topic/partition/offset on both events. `apply_current_state` is a dictionary **test oracle**; newer delete removes state, newer after-image upserts, and non-newer candidates leave state untouched. Runtime state lives in PyFlink `ValueState`, not this dictionary.

## `processing/flink/flink_cdc_job.py` — topology wiring

### Constants and DDLs

`TOPICS` names the four `mdep.commerce.*` inputs, `CONSUMER_GROUP` gives a stable group identity, and checkpoint/watermark constants make lab settings inspectable. `silver_table_ddls` produces four Iceberg v2 tables with `PRIMARY KEY ... NOT ENFORCED` plus `write.upsert.enabled=true`. The keys document intended changelog semantics; they are not an independent uniqueness validator.

### Evidence-first branches

`_raw_message` adds processing provenance. `_bronze_values` tries to parse only to enrich Bronze; malformed bytes are still represented as `unparsed`. `DebeziumParser` sends malformed non-null values to quarantine but treats tombstones as valid compaction evidence. This order is important: a parser failure cannot erase raw CDC evidence.

### Stateful operator

`CdcStateApplier.open` allocates last-version and current-row state. `process_element` restores the last event, calls the pure rule, emits rejected decisions to a stale/duplicate side output, updates version state only for `NEWER`, clears state after a newer delete, and chooses INSERT versus UPDATE_AFTER from whether current state existed. `_to_silver_row` carries business fields and ordering/transport metadata into the changelog row.

### Source through sink

`run` builds a source per topic from earliest offsets, wraps and unions raw strings, derives Bronze before semantic parsing, creates quarantine side output, assigns source-event watermarks, keys valid events, and bridges streams into Table API temporary views. `_add_file_sink_definitions` uses event-date partitions for Bronze/Quarantine. `_submit_writes` emits all entity history, quarantine and Silver inserts through one statement set.

The configured `SimpleStringSchema` is value-only. Therefore physical parsing currently receives no Kafka key/partition/offset even though the pure contract can accept them. That limitation is documented rather than hidden. Kafka connection, checkpoint completion, S3 writes and Iceberg commits are **RUNTIME DEFERRED — not runtime validated**.

## Tests as executable specification

`tests/test_flink_cdc_model.py` protects higher/lower LSN, same-transaction order, exact replay, missing-transport conflict, partition-number non-freshness, stale delete, tombstone, key collision and malformed-envelope invariants. `tests/test_flink_topology.py` protects against returning to the historical configuration-only stub by asserting real source/state/sink wiring and concrete DDL/checkpoint/watermark settings. Neither test proves a running broker, completed checkpoint or committed Iceberg snapshot.

## Recommended reading order

1. `tests/test_flink_cdc_model.py`
2. `processing/flink/cdc_model.py`
3. `tests/test_flink_topology.py`
4. `processing/flink/flink_cdc_job.py`
5. `scripts/validate-mdep-11-flink-cdc.ps1`
6. `validation/mdep-13-validation-matrix.yml`
