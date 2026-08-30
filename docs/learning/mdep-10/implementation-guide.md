# MDEP-10 implementation guide

MDEP-10 implements transport only: `PostgreSQL WAL -> Debezium PostgreSQL connector -> Kafka`. It captures `commerce.customers`, `products`, `orders`, and `payments`, but creates no Silver table and contains no Flink logic.

`docker-compose.yml` retains the existing PostgreSQL source and adds a one-node KRaft Kafka broker plus Debezium 3.0 Kafka Connect. `source-data/postgres/cdc-init.sql` grants the local `lab` role replication and creates `mdep_publication`; PostgreSQL already starts with `wal_level=logical`, replication slots, and WAL senders. `ingestion/cdc/debezium-postgres-connector.json` uses `pgoutput`, slot `mdep_debezium_slot`, publication `mdep_publication`, initial snapshot, JSON converters, and the four-table include list.

Topic derivation is Debezium `topic.prefix` + PostgreSQL schema + table: `mdep.commerce.customers`, `mdep.commerce.products`, `mdep.commerce.orders`, and `mdep.commerce.payments`. Records use the PostgreSQL primary key as Kafka key. A key therefore selects one partition and preserves order for that key only; Kafka provides no global ordering. One partition is sufficient for the small lab and topic retention is the broker default; a new consumer group can replay retained records.

The JSON Debezium envelope contains `before`, `after`, `source`, `op`, and source timing fields. For this connector version, consumers must inspect actual messages for optional `ts_ms`/higher precision, `source.lsn`, `source.txId`, schema/table, snapshot marker, and transaction metadata rather than assume every optional field is present. `r` is initial snapshot read; `c`, `u`, and `d` are insert, update, delete. Delete has `after: null`; `tombstones.on.delete=true` also emits a Kafka tombstone after the delete record.

`scripts/validate-mdep-10-cdc.ps1` starts the stack, registers the connector, queries WAL/publication/slot state, applies CRUD, schema addition, multi-row transaction, restarts Connect, and consumes messages with keys. It is a runtime exercise, not evidence that those steps ran on this host.
