# MDEP-10 interview Q&A

## What is CDC and why not polling?

**Direct answer:** CDC converts committed database mutations into replayable change records with lower latency than repeated table scans. **Project example:** Debezium reads PostgreSQL WAL for four `commerce` tables. **Follow-up:** “What is in WAL?” PostgreSQL's durability/change log. **Senior extension:** choose CDC when freshness and complete mutation semantics outweigh operational slot/lag management.

## How do PostgreSQL logical replication, publication, slot, and LSN work?

**Direct answer:** `wal_level=logical` permits decoding; a publication selects tables, a slot retains progress, and LSN identifies WAL position. **Project example:** `mdep_publication` and `mdep_debezium_slot`. **Follow-up:** “Risk?” an inactive slot retains WAL. **Senior extension:** alert on slot lag/WAL disk and have monitored failover/runbook procedures.

## What happens when Debezium starts and restarts?

**Direct answer:** `snapshot.mode=initial` emits `r` records from a consistent snapshot then streams WAL from its recorded position. Connect stores offsets, so a normal restart resumes rather than intentionally resnapshotting. **Project example:** offsets use Kafka topic `mdep_connect_offsets`. **Follow-up:** “If offsets are lost?” recovery can require resnapshot/replay and duplication handling. **Senior extension:** treat source positions as idempotency/audit data.

## How are changes, keys, and deletes represented?

**Direct answer:** Debezium values contain `before`, `after`, `source`, and `op`: `c`, `u`, `d`, or snapshot `r`; the Kafka key is the table primary key. Delete has `after=null` and this lab enables a tombstone. **Project example:** `mdep.commerce.orders` keys by `order_id`. **Follow-up:** “Ordering?” only within the same key's partition. **Senior extension:** never assume global ordering; future Flink keys state by primary key.

## Is Debezium exactly once and why does MDEP-10 stop at Kafka?

**Direct answer:** treat delivery as at least once; failures around offsets/acks can duplicate records. MDEP-10 captures transport, while MDEP-11 owns idempotent current-state Silver application. **Project example:** pure helpers classify envelopes but write no Silver state. **Follow-up:** “Kafka down?” connector pauses/fails and slot lag can grow. **Senior extension:** monitor connector/Kafka/slot lag, capacity, retries, DLQ policy, and recovery drills.

## How do you enable and use Debezium transaction metadata?

**Direct answer:** For this Debezium 3.0 PostgreSQL connector I configure `provide.transaction.metadata=true`. PostgreSQL supplies commit boundaries; Debezium is expected to enrich data-change envelopes with transaction metadata and emit transaction boundary events where its event contract supports them.

**Project example:** the connector JSON enables the setting and the validator performs two product updates inside one PostgreSQL transaction. This is configuration, not observed runtime evidence. **Likely follow-up:** “Does that provide global ordering?” No. Transaction metadata helps identify/order transaction-related records, while Kafka ordering remains only within each partition; records across multiple partitions have no global order. **Senior-level extension:** consume and validate the actual boundary/data event contract, retain source LSN/transaction identifiers for audit/idempotency, and design downstream state application for at-least-once delivery.
