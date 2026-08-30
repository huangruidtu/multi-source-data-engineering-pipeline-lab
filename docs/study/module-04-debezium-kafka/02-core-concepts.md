# Module 04 — Core concepts

## WAL, publication and slot
1. **Definition:** WAL records database changes; publication selects tables; a replication slot retains a consumer position. 2. **Why:** capture mutations without polling. 3. **MDEP file:** `source-data/postgres/cdc-init.sql`. 4. **Why selected:** CDC is a core V1 capability. 5. **Misunderstanding:** a slot stores downstream state. 6. **Failure:** lag retains WAL and risks disk pressure. 7. **Production:** monitor slot lag/retained bytes. 8. **Interview:** a slot is a retention responsibility.

## Debezium envelope and transactions
`debezium-postgres-connector.json` uses `pgoutput`, `mdep_publication`, `mdep_debezium_slot`, initial snapshot, tombstones and `provide.transaction.metadata=true`. `r/c/u/d` describe snapshot/create/update/delete; a Kafka tombstone is a compaction marker. The earlier `include.transaction` property was wrong; configuration was corrected. Transaction metadata is **CONFIGURED/expected by Debezium 3.0**, but not observed here.

## Kafka ordering and replay
Topics, keys, partitions, offsets and consumer groups provide durable at-least-once transport. Ordering is per partition, not globally across partitions; a partition number is never freshness. Consumers must use source semantics and idempotency.
