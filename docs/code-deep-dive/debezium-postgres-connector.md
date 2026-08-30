# Code Deep-Dive: `ingestion/cdc/debezium-postgres-connector.json`

**Source of truth:** [`ingestion/cdc/debezium-postgres-connector.json`](../../ingestion/cdc/debezium-postgres-connector.json). Configuration is **MDEP IMPLEMENTED / OFFLINE TESTED**; connector behavior is **MDEP RUNTIME DEFERRED**.

## Read beside
- **Source:** [`debezium-postgres-connector.json`](../../ingestion/cdc/debezium-postgres-connector.json)
- **Tests:** [`tests/test_cdc_contracts.py`](../../tests/test_cdc_contracts.py)
- **Architecture:** [`docs/finalization/end-to-end-data-flow.md`](../finalization/end-to-end-data-flow.md)
- **Interview topics:** [`cdc-transport-contracts.md`](cdc-transport-contracts.md), [`cdc-model.md`](cdc-model.md)

## 1. Why this file exists
It is the explicit Kafka Connect/Debezium PostgreSQL connector contract for the commerce CDC transport path.
## 2. Where it sits in the architecture
PostgreSQL logical replication and its publication/slot feed this connector; it emits the configured `mdep.commerce.*` topics that Flink later consumes.
## 3. Inputs / outputs / state
Database host `postgres:5432`, `commerce`, local credentials, publication and slot enter. Kafka Connect persists connector operational state externally; the JSON itself has no state. Output topics use prefix `mdep`.
## 4. Important symbols
Connector name `mdep-postgres-cdc`; `connector.class`; `pgoutput`; publication/slot settings; four-table include list; snapshot, tombstone, and transaction-metadata settings.
## 5. Execution flow
Connect loads this configuration, connects to PostgreSQL, uses the existing publication and slot, takes an initial snapshot, then streams `pgoutput` changes for only the included tables into prefixed Kafka topics.
## 6. Property-by-property walkthrough
`connector.class` selects Debezium PostgreSQL; `tasks.max=1` keeps this lab connector compact. Database properties identify the Docker-oriented source. `plugin.name=pgoutput` selects PostgreSQL logical decoding output.

`publication.name=mdep_publication` plus `publication.autocreate.mode=disabled` means publication creation is an explicit source prerequisite (`cdc-init.sql`), not a connector side effect. `slot.name=mdep_debezium_slot` establishes resume position; `slot.drop.on.stop=false` preserves it across connector stops. `topic.prefix=mdep` and `table.include.list` constrain the four exact commerce topics/tables.

`snapshot.mode=initial` establishes a baseline then streams future changes. `tombstones.on.delete=true` enables a Kafka compaction tombstone after the business delete event. `provide.transaction.metadata=true` configures Debezium 3.0 transaction metadata capability.
## 7. Critical code-block reasoning
The publication/slot combination is intentionally explicit. Disabled publication auto-create prevents a connector from silently changing source replication scope. Keeping a slot supports resume semantics, but an inactive/lagging consumer can make PostgreSQL retain WAL; resilience and source disk pressure are inseparable trade-offs.

Tombstones are transport/compaction semantics, distinct from the preceding delete record whose `after` is null. Transaction metadata is configured capability—not proof that transaction fields or boundary events were observed. Existing project documentation records the correction from the incorrect `include.transaction` switch to `provide.transaction.metadata`; `test_cdc_contracts.py` reads the actual JSON.
## 8. Correctness invariants
- PostgreSQL Debezium connector with `pgoutput` is selected.
- Publication, slot, prefix, snapshot mode, tombstone setting, and transaction setting are explicit.
- Exactly four approved CDC tables are included.
- Publication cannot be auto-created by the connector.
## 9. Failure behavior
Missing publication/replication privileges/slot prevents correct connector startup rather than expanding scope silently. A stopped connector can leave the preserved slot and retain WAL. Registration, snapshot failure, Kafka connectivity, and schema failures are runtime concerns not swallowed by JSON.
## 10. Tests that protect the behavior
[`tests/test_cdc_contracts.py`](../../tests/test_cdc_contracts.py) loads the actual JSON and asserts connector class, `pgoutput`, publication, slot, initial snapshot, tombstones, transaction metadata, prefix, and exact table list. **MDEP OFFLINE TESTED.**
## 11. What is not implemented / runtime deferred
**MDEP RUNTIME DEFERRED:** PostgreSQL replication prerequisites, Connect registration, initial snapshot, actual `r/c/u/d`/tombstone messages, transaction boundary observation, offset/slot restart, Kafka delivery, and WAL-lag measurement.
## 12. Production concepts beyond current code
**GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED:** production secret handling, connector HA, monitoring/alerts for connector and slot lag, capacity policy, schema registry, and recovery drills.
## 13. Common misunderstandings
`slot.drop.on.stop=false` is not free recovery; it has WAL retention risk. Kafka tombstone is not the business delete itself. Transaction metadata does not create global ordering across Kafka partitions. `snapshot.mode=initial` is not runtime evidence that a snapshot ran.
## 14. Interview questions
**Why disable publication auto-creation?** Source replication scope is a database ownership prerequisite. Requiring it explicitly makes permissions/table scope reviewable and avoids a connector silently changing the source.

**How do you enable transaction metadata?** In this Debezium PostgreSQL 3.0 configuration, `provide.transaction.metadata=true`. It can enrich/emit transaction information where supported, but PostgreSQL boundaries and Kafka partition ordering remain distinct, and MDEP has not observed it at runtime.
## 15. 30-second spoken explanation
“This JSON explicitly configures a Debezium PostgreSQL connector with pgoutput, a pre-created publication, a durable replication slot, initial snapshot, four-table scope, tombstones, and Debezium 3.0 transaction metadata. The important trade-off is that preserving the slot helps resume but can retain WAL and pressure source disk. The configuration is statically tested; no live connector behavior is claimed.”
## 16. Senior follow-up discussion
Explain your operational response to connector lag: measure slot/WAL position and disk growth, establish alert thresholds, decide whether to restore/scale/fix the connector before slot retention threatens the source, and validate recovery with real drills rather than relying on configuration intent.
