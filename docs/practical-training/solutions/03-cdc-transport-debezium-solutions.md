# Solutions — 03 CDC transport and Debezium workbook

Read these only after attempting [the workbook](../03-cdc-transport-debezium.md). The answers describe actual checked-in configuration and static contracts, not a completed PostgreSQL/Debezium/Kafka runtime.

## CT-01 — Connector configuration and slot-ownership review

**Correct answer:** `publication.autocreate.mode=disabled` requires the PostgreSQL publication `mdep_publication` to already exist with the approved source-table scope; Debezium is not configured to create it. The connector uses Debezium’s PostgreSQL connector with `plugin.name=pgoutput`, the explicit `mdep_debezium_slot`, four table include entries, topic prefix `mdep`, and `snapshot.mode=initial`. Its expected topics follow the `mdep.commerce.<table>` transport contract. `tombstones.on.delete=true` configures later Kafka compaction tombstones, and `provide.transaction.metadata=true` is the current Debezium transaction-metadata setting.

**Step-by-step transport/config trace:** PostgreSQL writes WAL; a publication provides the selected logical-decoding scope; the named logical replication slot tracks the consumer’s acknowledged progress; Debezium can take its configured initial snapshot and stream decoded `pgoutput` changes to the four included topic families. With `slot.drop.on.stop=false`, stopping the connector preserves the slot position so a recovered connector can resume from retained WAL rather than automatically discarding that progress.

That recovery benefit creates a source-side cost: if the slot cannot advance its confirmed position, PostgreSQL may retain WAL needed by the slot. Retained WAL can consume disk until the connector recovers or an explicit, safe operational decision changes the slot/recovery plan. The configuration does not show a current LSN, lag, slot state, or disk reading.

**Relevant actual configuration/contract logic:** `tests/test_cdc_contracts.py` loads the JSON itself and asserts connector class, `pgoutput`, publication, slot, initial snapshot, tombstones, `provide.transaction.metadata`, prefix, and the exact four-table set. `contracts.py` separately rejects out-of-scope tables and maps approved tables to topic/key semantics.

**Correctness invariant:** CDC source scope, publication/slot ownership, topic naming, and recovery behavior are explicit rather than connector side effects; retaining a resume position never removes the need to manage retained WAL.

**Common wrong answer:** “The connector will create the publication,” “a stopped connector cannot affect PostgreSQL,” or “transaction metadata is observed because the property exists.” The historical correction from `include.transaction=true` to `provide.transaction.metadata=true` fixes checked-in Debezium configuration intent; it is not live transaction-boundary evidence.

**Production consequence:** An unmonitored stopped/lagging preserved slot can pressure the source database disk; auto-created scope can silently broaden or conflict with source ownership.

**Suggested regression/config test:** Parse the actual connector JSON and assert all named properties plus `publication.autocreate.mode == 'disabled'` and `slot.drop.on.stop == 'false'`; also assert the include list has exactly the approved tables. Static configuration is `MDEP IMPLEMENTED` and `MDEP OFFLINE TESTED`. Connector registration, snapshot, streaming, transaction metadata, and resume are `MDEP RUNTIME DEFERRED`.

**Interview-ready English answer:** “We require a pre-created PostgreSQL publication and preserve a named replication slot. That makes source scope explicit and lets Debezium resume, but it also means a stopped or lagging consumer can retain WAL and pressure the source disk. I monitor connector health together with slot progress and WAL/disk growth; the configuration is statically tested, while live replication is deferred.”

**Senior follow-up discussion:** Define source owner, disk headroom, alert thresholds based on retained WAL and growth rate, recovery runbooks, and a tested decision tree for resume versus re-snapshot. These operational controls are **GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED** in MDEP.

## CT-02 — Debezium delete versus Kafka tombstone trace

**Correct answer:** The topic suffix identifies `orders`, and `contracts.primary_key` validates `order_id='1001'` from the Kafka key. Message A’s `op='d'` is a business delete envelope; `classify_envelope` returns `delete` only when `after` is null. Because deletes have no after-image, the stable identity must come from the Kafka key when present or, at the downstream parser boundary, from `before` when needed.

**Step-by-step transport/config trace:** Message A carries a business transition plus source evidence, including LSN. `processing/flink/cdc_model.py` normalizes it into a `CdcEvent` only after validating its entity, delete shape, key fallback, and source LSN. It is merely a delete candidate at that point: `version_decision` must still classify it as `NEWER` before `apply_current_state` removes the current row. A stale delete cannot bypass source-version semantics.

Message B has a null Kafka value. Since the connector is configured with `tombstones.on.delete=true`, it represents a Kafka log-compaction marker for the same key, not a second business delete. In the MDEP model, `parse_debezium` returns `None` and `apply_current_state` returns `tombstone_ignored`. This deliberately keeps business deletion semantics separate from transport-log maintenance semantics.

**Relevant actual configuration/contract logic:** The actual JSON enables tombstones. `contracts.py` enforces legal `r/c/u/d` operations and `after=null` for delete; `cdc_model.py` treats `value_json=None` as a tombstone. Its current-state layer owns LSN/transaction/replay decisions, so topic/key/offset facts must not replace source freshness.

**Correctness invariant:** A Debezium delete envelope and a Kafka tombstone are distinct events with distinct evidence and effects; only a source-version-winning business delete may remove current state.

**Common wrong answer:** Treat the tombstone as another delete, or delete state immediately based solely on `op='d'`. Both conflate compaction transport behavior with business-current-state mutation.

**Production consequence:** Collapsing the two can cause duplicate/deletion errors, loss of replay reasoning, and inconsistent recovery logic.

**Suggested regression/config test:** Assert `classify_envelope({'op':'d','after':None}) == 'delete'` and reject a delete with an after-image; separately assert `parse_debezium(..., value_json=None, ...) is None` and `apply_current_state(..., None) == 'tombstone_ignored'`. These pure/static paths are `MDEP OFFLINE TESTED`; actual Kafka delivery/compaction is `MDEP RUNTIME DEFERRED`.

**Interview-ready English answer:** “The Debezium delete record is the business event: it has `op=d`, no after-image, and still must win source-version ordering before it deletes state. The following Kafka tombstone is a compaction marker for the key. In our model a null value becomes `None` and is ignored for business state, so the two semantics are never collapsed.”

**Senior follow-up discussion:** With a metadata-preserving source, retain topic/partition/offset and key for diagnostics and replay identity, but continue to use PostgreSQL source/version evidence for current-state ordering. Metadata availability in the current value-only source is a runtime limitation, not an excuse to infer freshness from offsets.

## CT-03 — WAL-retention incident

**Correct answer:** The likely causal chain is a stopped/lagging Debezium logical-replication consumer whose retained `mdep_debezium_slot` cannot advance acknowledgment. PostgreSQL therefore retains WAL that may still be needed from the slot’s restart position, and the retained WAL consumes disk. `slot.drop.on.stop=false` intentionally preserves recovery/resume state, but it does not make the retained WAL free.

**Step-by-step transport/config trace:** First confirm the incident rather than acting on configuration alone: inspect connector state/errors and the actual slot’s existence, active/inactive status, `restart_lsn`, `confirmed_flush_lsn`, current WAL position, retained bytes or equivalent lag indication, filesystem capacity, and growth rate. Connector status identifies whether the consumer is functioning; PostgreSQL slot state establishes whether the source can reclaim the relevant WAL. Neither Kafka offset nor downstream Flink ordering proves slot advancement.

Before dropping or recreating the slot, ask whether the connector can safely resume, whether credentials/network/publication issues are repairable, how much disk headroom remains, whether capacity can be extended, and whether the source owner accepts a recovery change. Dropping/recreating a slot loses its resume position. Depending on available history and connector configuration, recovery may require a new snapshot and then reconciliation; it is not harmless disk cleanup.

**Relevant actual configuration/contract logic:** MDEP config explicitly chooses preserved slot `mdep_debezium_slot` and disables publication auto-create. Its test only establishes those configuration facts. No repository evidence contains actual slot query results, WAL volume, or a connector incident.

**Correctness invariant:** Never destroy source replication recovery state merely to reclaim disk without establishing data-loss/recovery implications and an approved recovery path.

**Common wrong answer:** “Restart Debezium and disk will certainly recover,” “drop the slot immediately,” or “Kafka has the data, so PostgreSQL slot state is irrelevant.” These skip source evidence and may turn a recoverable lag condition into lost continuity.

**Production consequence:** Uncontrolled WAL retention can exhaust source disk and affect PostgreSQL availability; careless slot removal can require broad recovery/snapshot/reconciliation work.

**Suggested regression/config test:** Statically parse the connector JSON for `slot.name='mdep_debezium_slot'` and `slot.drop.on.stop='false'`; a production runtime drill would stop/lag a connector in an isolated environment, capture PostgreSQL slot/WAL evidence, resume or re-snapshot using an approved runbook, and reconcile source-to-consumer state. The first is `MDEP OFFLINE TESTED`; the drill is `GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED` and `MDEP RUNTIME DEFERRED`.

**Interview-ready English answer:** “A logical replication slot can retain WAL until its consumer advances the confirmed position. We preserve the slot to resume Debezium safely, but a stopped connector can therefore create source disk pressure. I inspect both connector errors and PostgreSQL slot LSN/state plus WAL growth before deciding whether to restore the connector, add capacity, or accept a re-snapshot plan; I never drop a slot as a blind cleanup.”

**Senior follow-up discussion:** Define alerts for disk headroom, retained-WAL growth, slot lag, inactive critical slots, and connector failure duration. Couple those alerts to clear source-owner escalation and a rehearsed re-snapshot/reconciliation process. MDEP has no live monitoring or incident evidence; that remains `MDEP RUNTIME DEFERRED`.
