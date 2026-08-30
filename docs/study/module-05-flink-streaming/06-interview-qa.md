# Module 05 — Interview Q&A

### How do you prevent stale CDC events from changing current state?
**Direct answer:** compare source LSN, then same-transaction order, then exact known transport identity. **Deep explanation:** Kafka offset/partition is not a database version. **MDEP example:** `version_decision` and its tests. **Why:** replay must converge. **Follow-up:** same LSN unknown identity? reject as conflict. **Senior extension:** source transaction ordering contract. **Weak answer:** use event time or partition number.

### Checkpoint versus savepoint?
**Direct answer:** checkpoint is automated fault recovery state; savepoint is an intentional operational snapshot. **MDEP example:** checkpoint interval/config exists in `flink_cdc_job.py`. **Follow-up:** exactly once? configured, not proven. **Senior extension:** upgrade/rescale procedure. **Weak answer:** treating both as backups.

### Why do tombstones not delete Silver?
**Direct answer:** the preceding Debezium delete changes state; null tombstone is a Kafka compaction marker. **MDEP example:** `apply_current_state(None)` returns ignored. **Follow-up:** archive it? yes, as Bronze evidence. **Senior extension:** compaction/retention policy. **Weak answer:** parse null as malformed business delete.
