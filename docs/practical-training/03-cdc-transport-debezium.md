# 03 — CDC transport and Debezium workbook

Attempt this file before [the matching solutions](solutions/03-cdc-transport-debezium-solutions.md). Record work in a copy of [the session template](training-records/03-cdc-transport-debezium-session-template.md). This workbook is grounded in checked-in configuration and pure/static tests, not a live PostgreSQL, Debezium, or Kafka run.

## CT-01 — Connector configuration and slot-ownership review

- **Difficulty:** Intermediate
- **Task type:** CONFIGURATION REVIEW / FAILURE ANALYSIS / INTERVIEW EXPLANATION
- **Source files/config to inspect:** `ingestion/cdc/debezium-postgres-connector.json`; `ingestion/cdc/contracts.py`; `tests/test_cdc_contracts.py`; [connector deep-dive](../code-deep-dive/debezium-postgres-connector.md); [transport-contract deep-dive](../code-deep-dive/cdc-transport-contracts.md).
- **Scenario:** A reviewer asks whether the checked-in Debezium connector can silently change PostgreSQL replication scope, and whether its recovery choice has a source-disk cost. Review the configuration before any connector registration.
- **Concrete connector/topic/envelope facts:** The connector class is `io.debezium.connector.postgresql.PostgresConnector`; `plugin.name` is `pgoutput`; database is `commerce`; `publication.name` is `mdep_publication`; `publication.autocreate.mode` is `disabled`; `slot.name` is `mdep_debezium_slot`; and `slot.drop.on.stop` is `false`. It includes exactly `commerce.customers,commerce.products,commerce.orders,commerce.payments`, uses topic prefix `mdep`, `snapshot.mode=initial`, `tombstones.on.delete=true`, and `provide.transaction.metadata=true`. The pure transport contract maps `orders` to `mdep.commerce.orders` and accepts only `r/c/u/d`.
- **Engineering deliverables:**
  1. State the PostgreSQL publication prerequisite and replication-slot prerequisite/ownership implied by this configuration. Explain whether Debezium will auto-create the publication.
  2. Explain every listed setting’s relevant boundary: decoding plugin, exact source scope, topic naming, initial snapshot, transaction-metadata capability, and tombstone setting.
  3. Explain why retaining the slot after connector stop helps resume/recovery, then trace how an unadvanced confirmed slot position can retain WAL and create disk pressure.
  4. Identify what an operator should monitor: connector health/lag, slot active state, slot progress such as `restart_lsn` and `confirmed_flush_lsn`, current WAL position, and WAL/disk growth. Distinguish configuration intent from observed metrics.
  5. Propose one static/config regression test that reads the JSON rather than repeating constants, and write a 30–60 second interview explanation.
  6. Add the historical correctness note: the former `include.transaction=true` setting was corrected to `provide.transaction.metadata=true`. Explain why this is a checked-in configuration fix, not evidence that transaction metadata or boundaries were observed at runtime.
- **Constraints:** `publication.autocreate.mode=disabled` means source publication setup is an explicit prerequisite, not a connector side effect. Do not invent a publication definition, a live slot position, or a running connector. `MDEP IMPLEMENTED` covers configuration/contracts; `MDEP OFFLINE TESTED` covers the test loading the actual JSON; PostgreSQL logical replication, Connect registration, Kafka delivery, snapshot and resume are `MDEP RUNTIME DEFERRED`.
- **Competency trained:** source-side CDC ownership, recovery-versus-WAL-retention trade-offs, and configuration evidence.
- **Learner workspace/template:**

```text
Publication prerequisite / auto-create verdict: ___
Slot ownership and recovery benefit: ___
WAL retention mechanism: ___
Settings and boundaries: ___
Monitor: ___
Static test: ___
Historical transaction-metadata note: ___
Interview explanation: ___
```

## CT-02 — Debezium delete versus Kafka tombstone trace

- **Difficulty:** Foundation
- **Task type:** MESSAGE TRACE / SEMANTIC BOUNDARY REVIEW / TEST DESIGN
- **Source files/config to inspect:** `ingestion/cdc/contracts.py`; `ingestion/cdc/debezium-postgres-connector.json`; `processing/flink/cdc_model.py` (`parse_debezium`, `apply_current_state`); `tests/test_cdc_contracts.py`; `tests/test_flink_cdc_model.py`; [CDC-model deep-dive](../code-deep-dive/cdc-model.md).
- **Scenario:** A consumer sees two records for the same key after an order is deleted. Do not collapse transport compaction behavior into business-current-state semantics.
- **Concrete connector/topic/envelope facts:**

```text
Topic: mdep.commerce.orders
Kafka key: {"order_id":"1001"}

Message A — Debezium delete envelope:
  value = {"op":"d", "before":{"order_id":"1001", "order_status":"created"},
           "after":null, "source":{"lsn":"0/500"}}

Message B — Kafka tombstone for the same key:
  value = null
```

The actual connector has `tombstones.on.delete=true`. `contracts.primary_key("orders", {"order_id":"1001"})` resolves the business key, and `classify_envelope` requires a delete’s `after` to be null. `parse_debezium(..., value_json=None, ...)` returns `None`; `apply_current_state(..., None)` returns `tombstone_ignored`.
- **Engineering deliverables:**
  1. Derive the topic/entity and validate the Kafka-key contract for Message A.
  2. Classify `op=d`; explain why the delete must find its identity from Kafka key or `before` when `after` is null.
  3. Trace Message A through the transport boundary and downstream semantic boundary. State the condition that still governs whether it can delete current state: source-version ordering in `cdc_model.py`.
  4. Trace Message B: state Kafka log-compaction meaning, the `value_json=None` behavior, and why it is not a second business deletion.
  5. Separate transport evidence (topic, key, tombstone) from business semantics (delete operation, source LSN, version decision), propose one regression test, and write a 30–60 second interview explanation.
- **Constraints:** Preserve this invariant: a Debezium delete event and a Kafka tombstone are not the same semantic event. Do not claim actual Kafka records were observed. The value-only PyFlink source has runtime limitations around Kafka key/transport metadata, so do not invent metadata availability.
- **Competency trained:** delete-envelope contracts, compaction semantics, and transport-versus-current-state separation.
- **Learner workspace/template:**

```text
Topic/entity/key: ___
Message A classification and identity: ___
Message A current-state condition: ___
Message B transport meaning / model result: ___
Why not a second delete: ___
Regression test: ___
Interview explanation: ___
```

## CT-03 — WAL-retention incident

- **Difficulty:** Senior
- **Task type:** INCIDENT RESPONSE / OPERATIONAL DESIGN / INTERVIEW EXPLANATION
- **Source files/config to inspect:** `ingestion/cdc/debezium-postgres-connector.json`; `tests/test_cdc_contracts.py`; [connector deep-dive](../code-deep-dive/debezium-postgres-connector.md); `processing/flink/cdc_model.py` only to identify the downstream boundary.
- **Scenario:** The Debezium connector has been stopped for several days. PostgreSQL disk usage is rising and WAL storage is growing. The team notices `slot.drop.on.stop=false` in the connector configuration. Produce a safe incident assessment without pretending the MDEP lab experienced this incident.
- **Concrete connector/topic/envelope facts:** The configuration preserves `mdep_debezium_slot` when the connector stops and disables automatic publication creation. Assume an operations dashboard reports a stopped connector and increasing WAL/disk usage, but provides no confirmed live PostgreSQL query result yet. The relevant conceptual chain is `PostgreSQL WAL -> logical replication slot -> Debezium progress -> retained WAL -> disk pressure`.
- **Engineering deliverables:**
  1. State the most likely mechanism and why a stopped or lagging logical-replication consumer can prevent WAL reclamation.
  2. Explain precisely why `slot.drop.on.stop=false` is both a recovery feature and an operational risk.
  3. Give a first-response checklist: connector status/error, publication/scope, PostgreSQL slot existence and active/inactive status, `restart_lsn`, `confirmed_flush_lsn` or equivalent evidence, current WAL position/retained-byte estimate, filesystem/disk capacity, and rate of growth.
  4. Explain why connector health alone is insufficient and what each PostgreSQL slot signal can help establish.
  5. List safe remediation questions before dropping/recreating a slot: can the consumer resume, what data is still retained, can capacity be safely extended, what snapshot/reconciliation impact follows, and who owns source-data risk.
  6. Explain the risk of slot recreation: loss of resume position and possible new snapshot/recovery work; do not say it is a harmless cleanup. Propose monitoring/alerting and write an incident-response checklist plus a 30–60 second interview answer.
- **Constraints:** This is `GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED` for live observability/remediation. MDEP supplies `MDEP IMPLEMENTED` configuration and `MDEP OFFLINE TESTED` static assertions only. Actual logical replication, connector status, slot progress, WAL growth, restart recovery, and snapshots are `MDEP RUNTIME DEFERRED`. Do not let Kafka topic offsets or downstream Flink ordering substitute for PostgreSQL source/slot evidence.
- **Competency trained:** incident diagnosis, operational trade-off reasoning, and safe recovery planning.
- **Learner workspace/template:**

```text
Likely mechanism: ___
Why retained slot helps / hurts: ___
First checks: ___
Connector versus slot evidence: ___
Before slot drop/recreate: ___
Recovery/snapshot risk: ___
Monitoring and alerting: ___
Incident checklist: ___
Interview explanation: ___
```
