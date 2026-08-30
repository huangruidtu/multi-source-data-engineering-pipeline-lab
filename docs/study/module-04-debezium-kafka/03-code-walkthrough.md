# Module 04 — Code walkthrough

Reading order: `source-data/postgres/cdc-init.sql` → `ingestion/cdc/debezium-postgres-connector.json` → `ingestion/cdc/contracts.py` → `tests/test_cdc_contracts.py` → `scripts/validate-mdep-10-cdc.ps1`.

The JSON is the actual connector contract: class, `pgoutput`, publication/slot, `snapshot.mode=initial`, tombstones, exact four-table include list and `topic.prefix=mdep`. `contracts.py` defines expected entity/topic shape without duplicating JSON constants. Its tests read the JSON, protecting the reviewed transaction metadata correction. The script describes registration/mutation evidence but is not a completed run. Notice that MDEP-10 stops at Kafka; MDEP-11 owns applying the envelope.

## `source-data/postgres/cdc-init.sql`

This is the source-side prerequisite, not a Debezium convenience script. It grants the local `lab` role replication and creates `mdep_publication` for the four CDC-owned commerce tables. The publication intentionally excludes REST/file references because CDC ownership is only for relational current-state entities. Compose supplies `wal_level=logical`; without that setting, logical decoding cannot supply the connector.

## `ingestion/cdc/debezium-postgres-connector.json`

The `name` identifies the connector. `connector.class` selects PostgreSQL Debezium. `database.*` identifies the Compose source, while `plugin.name=pgoutput`, `publication.name`, and `slot.name` bind capture to the PostgreSQL prerequisites. `slot.drop.on.stop=false` retains source-resume state; it also means stalled capture can retain WAL. `topic.prefix=mdep` and `table.include.list` produce the four explicitly owned topics. `snapshot.mode=initial` produces bootstrap `r` records before streaming changes. `tombstones.on.delete=true` retains Kafka compaction semantics.

The critical review correction is `provide.transaction.metadata=true`. The old `include.transaction` key must not return: `tests/test_cdc_contracts.py` parses this JSON and asserts the actual expected setting. Configuration is not observation—transaction events and metadata are runtime-unvalidated.

## `ingestion/cdc/contracts.py` and tests

`topic_name` makes the source ownership boundary executable: unsupported `locations` is rejected. `primary_key` refuses a key missing the contracted PK; it documents why Kafka keys must carry business identity. `classify_envelope` maps `r/c/u/d` and rejects a delete with non-null `after`. The tests protect topic derivation, key contract, envelope semantics and the connector JSON. They do not prove Kafka assigned a key to a partition or that Debezium emitted a real envelope.

## `scripts/validate-mdep-10-cdc.ps1`

The runtime exercise first requires Docker, optionally resets volumes, starts Compose, registers the connector through Connect REST, checks connector status, then queries `wal_level`, publication, slot `confirmed_flush_lsn`, current WAL position and a Kafka topic. It performs insert/update/delete, additive schema change, a multi-row transaction, Connect restart and keyed topic consumption. Every step is an acceptance-evidence recipe; on the current host the matrix records it as BLOCKED rather than successful.

## Recommended reading order

1. `tests/test_cdc_contracts.py`
2. `source-data/postgres/cdc-init.sql` and Compose PostgreSQL settings
3. `ingestion/cdc/debezium-postgres-connector.json`
4. `ingestion/cdc/contracts.py`
5. `scripts/validate-mdep-10-cdc.ps1`
6. Module 05 `cdc_model.py`, which consumes the resulting contract
