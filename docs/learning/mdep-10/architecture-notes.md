# MDEP-10 architecture notes

CDC reads PostgreSQL's write-ahead log through logical decoding rather than repeatedly polling tables. A publication specifies the four exported tables; a replication slot holds the consumer position, expressed as an LSN, so Debezium can resume. The initial snapshot establishes a consistent bootstrap and records snapshot `r` events, then Debezium streams later WAL changes from the captured position.

Kafka Connect hosts and manages the Debezium connector; Kafka stores table-per-topic change records. The broker is deliberately a single KRaft node—no ZooKeeper or schema registry is required for this lab. Kafka partitions scale consumption but only order records within a partition. Keys are mandatory because MDEP-11 will need per-primary-key order to apply current state safely.

This is at-least-once transport. A crash near offset/producers acknowledgements can cause a duplicate on resume; an offset loss can force snapshot/recovery choices. Consumers must compare source positions/version semantics and be idempotent. A stalled connector can leave a replication slot behind and retain WAL, risking disk pressure; monitor slot lag, current LSN, broker/topic lag, connector state, and failures. A transaction groups source changes, but consumers must not infer global ordering across keys/partitions.

An additive `preferred_language` column is intentionally exercised. Debezium exposes changed row structure in subsequent payloads; future Flink/Iceberg jobs must handle additive fields deliberately. MDEP-10 stops at Kafka: MDEP-11 owns applying CDC events into canonical Silver current state.
