# MDEP-10 learning notes

- **CDC/WAL/logical decoding:** PostgreSQL writes mutations to WAL; logical decoding makes changes consumable. Here `wal_level=logical` enables Debezium rather than polling `updated_at`.
- **Publication, slot, LSN:** `mdep_publication` scopes tables; `mdep_debezium_slot` retains the resume position; an LSN is WAL progress. A failed connector can make retained WAL grow.
- **Debezium/Kafka Connect:** Connect runs the connector and persists config/status/source offsets in Kafka internal topics. Debezium snapshots then streams `pgoutput` changes.
- **Topic/partition/key/offset:** topics are one per table. Primary-key messages stay in one partition and are ordered there; an offset is the position inside that partition. A consumer group can replay with a new group ID.
- **Envelope:** `before`/`after` show row states and `op` means snapshot read/create/update/delete. A delete record has `after=null`; a tombstone is a following null-value record used by compacted consumers.
- **At least once:** restarts may repeat delivered changes. MDEP-11 must use key/source-position semantics for idempotent Silver application; MDEP-10 does not claim exactly once.
- **Transaction metadata/schema/recovery:** PostgreSQL transactions provide commit boundaries but not global Kafka order. This connector is **CONFIGURED** with Debezium 3.0 `provide.transaction.metadata=true`; Debezium is **expected** to enrich change events with transaction identity/order metadata and emit boundary events where supported. It is **runtime unvalidated** here. `preferred_language` is additive schema change. Inspect status, slot, LSN, logs, offsets, and actual transaction messages after restarts before declaring recovery successful.
