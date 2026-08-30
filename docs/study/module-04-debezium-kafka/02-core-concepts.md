# Module 04 — Core concepts

## WAL, publication and slot
1. **Definition:** WAL records database changes; publication selects tables; a replication slot retains a consumer position. 2. **Why:** capture mutations without polling. 3. **MDEP file:** `source-data/postgres/cdc-init.sql`. 4. **Why selected:** CDC is a core V1 capability. 5. **Misunderstanding:** a slot stores downstream state. 6. **Failure:** lag retains WAL and risks disk pressure. 7. **Production:** monitor slot lag/retained bytes. 8. **Interview:** a slot is a retention responsibility.

## Debezium envelope and transactions
`debezium-postgres-connector.json` uses `pgoutput`, `mdep_publication`, `mdep_debezium_slot`, initial snapshot, tombstones and `provide.transaction.metadata=true`. `r/c/u/d` describe snapshot/create/update/delete; a Kafka tombstone is a compaction marker. The earlier `include.transaction` property was wrong; configuration was corrected. Transaction metadata is **CONFIGURED/expected by Debezium 3.0**, but not observed here.

## Kafka ordering and replay
Topics, keys, partitions, offsets and consumer groups provide durable at-least-once transport. Ordering is per partition, not globally across partitions; a partition number is never freshness. Consumers must use source semantics and idempotency.

## PostgreSQL WAL, logical decoding, publication, and slot

### Definition

PostgreSQL writes changes to the write-ahead log (WAL) before they become durable database state. Physical replication replays storage-level WAL to a replica; logical decoding interprets relevant WAL changes as row-level change records for consumers. MDEP uses logical decoding through the `pgoutput` plugin, not physical replica replication.

### MDEP mapping

`docker-compose.yml` starts PostgreSQL with `wal_level=logical`, four replication slots and four WAL senders. `source-data/postgres/cdc-init.sql` grants the lab role replication and creates `mdep_publication` for exactly `commerce.customers`, `products`, `orders`, and `payments`. The connector JSON sets `publication.autocreate.mode=disabled`: this makes publication ownership explicit rather than allowing a connector to silently change source configuration.

### Replication slots and retained-WAL risk

A slot retains enough WAL for a consumer to resume. Its `restart_lsn` is conceptually the oldest LSN PostgreSQL must retain; `confirmed_flush_lsn` is the position the consumer has confirmed. If Debezium or Kafka Connect is unhealthy, retained WAL can grow until source disk is at risk. MDEP’s validator is designed to inspect `confirmed_flush_lsn` and `pg_current_wal_lsn`; no such runtime observation has occurred. Dropping/recreating a slot can force a resnapshot or create a gap depending on recovery design, so it is not a casual repair action.

## Kafka Connect and Debezium connector responsibilities

Kafka Connect is a runtime for connectors. A worker hosts connectors; a source connector reads an external system and produces Kafka records; tasks are units of connector work. MDEP configures `tasks.max=1` for a small source, and the Compose stack has one Debezium Connect instance. This is a lab topology, not a replicated Connect cluster.

The actual connector is `io.debezium.connector.postgresql.PostgresConnector`, database endpoint `postgres:5432`, plugin `pgoutput`, slot `mdep_debezium_slot`, initial snapshot mode, topic prefix `mdep`, the four-table include list, and `tombstones.on.delete=true`. `tests/test_cdc_contracts.py` reads the JSON itself, so changing the configuration without updating the source contract breaks a static invariant.

## Snapshot, streaming, and the Debezium envelope

With `snapshot.mode=initial`, Debezium first emits baseline row records with `op=r`, then streams committed changes as `c`, `u`, and `d`. A normal envelope contains `before`, `after`, `source`, `op`, timing information and, when configured/supported, transaction fields. A delete has `after=null`; because tombstones are enabled, a subsequent null-valued Kafka record may appear for compaction. The tombstone is transport lifecycle information, not a second logical delete.

`source.lsn` is database source position. `source.txId` is PostgreSQL source transaction information. Debezium `transaction.id`, `transaction.total_order`, and `transaction.data_collection_order` are connector transaction metadata. They are related but not interchangeable; Module 05 uses LSN first and total order only under the documented equal-LSN/same-transaction condition.

## Transaction metadata: corrected configuration and honest claim

The original connector used `include.transaction`, which does not reliably enable the intended Debezium PostgreSQL 3.0 transaction metadata. The reviewed correction is `provide.transaction.metadata=true`. It is **CONFIGURED** in `ingestion/cdc/debezium-postgres-connector.json`; Debezium 3.0 is expected to enrich events and emit transaction boundary information where its event contract supports it; it is **RUNTIME DEFERRED — not runtime validated** because no BEGIN/END or transaction-topic record has been observed in this repository.

## Kafka broker, topic, partition, key, offset, and consumer group

MDEP derives topics as `mdep.commerce.<table>`. The PostgreSQL primary key is the Kafka key (`ingestion/cdc/contracts.py`), which routes all records for that key to one partition and gives order for that key within that partition. Offset is an append position within that partition. Neither a partition number nor an offset across different partitions is a global PostgreSQL freshness value.

The Compose broker is one-node KRaft with replication factor 1 for offsets and transaction state. Leader/follower replicas, ISR, producer acknowledgements, replication factor, multi-broker failure tolerance and partition-count planning are important **GENERAL / NOT IMPLEMENTED IN MDEP** topics. This lab intentionally exposes their limitation: losing the only broker is not a high-availability design.

Consumer groups co-ordinate ownership of partitions. A restart or rebalance can cause replay around an acknowledged boundary, so consumers must be idempotent. Retention permits a new group to read old records; compaction retains latest-key semantics plus tombstones. Exact-once across database capture, Kafka transport and a downstream sink requires more than a connector setting and is not claimed here.

## Schema evolution and operational recovery

The validator includes `ALTER TABLE ... ADD COLUMN IF NOT EXISTS preferred_language` followed by an update. This is an additive-evolution learning exercise. Consumers must tolerate an evolving `after` shape or apply an explicit compatibility policy. Restarting Connect should be assessed by connector status, slot position and duplicate/replay semantics; it must not be assumed harmless. The validator can reset with `docker compose down -v`, but that deliberately destroys offsets and source data—a useful failure-lab action, not a normal production recovery.

## Interview framing

State the path precisely: “PostgreSQL logical WAL → publication/slot → Debezium Connect → keyed `mdep.commerce.*` topics.” Then add: “The connector is configured for transaction metadata, but no runtime event has been observed; Kafka ordering remains per partition, while downstream current-state ordering uses source LSN.”

## PostgreSQL write-ahead log (WAL)

### Definition

WAL is PostgreSQL's append-oriented durability record. Before a database page change is considered durable, the change is described in WAL so recovery and replication can reconstruct committed state.

### Why it exists

It protects database recovery and supplies an ordered source of committed changes. CDC uses the latter capability; it does not query application tables repeatedly to guess what changed.

### How it works internally

Each committed transaction advances a log sequence position. PostgreSQL can retain enough segments for a replication consumer. Logical decoding interprets relevant WAL records as row changes instead of replaying storage blocks.

### MDEP mapping

The MDEP PostgreSQL container starts with `wal_level=logical`. Debezium reads the permitted commerce-table changes through `pgoutput`.

### Actual file/config reference

`docker-compose.yml` sets `wal_level=logical`, `max_replication_slots=4`, and `max_wal_senders=4`. `source-data/postgres/cdc-init.sql` grants replication and creates the publication.

### Failure implications

If logical WAL is unavailable, the connector cannot capture row changes. If a slot stops advancing, retained WAL may consume source disk.

### Production considerations

Monitor WAL generation, slot lag, retained bytes, disk headroom and database failover behaviour. A CDC pipeline's source database must be part of the operational design.

### Common misunderstanding

WAL is not a Kafka topic and it is not automatically an application-level audit trail with stable consumer contracts.

### Interview framing

“WAL is the ordered database durability log. MDEP uses logical decoding to turn selected committed row changes into CDC events.”

## Physical replication versus logical replication

### Definition

Physical replication copies PostgreSQL storage-level WAL to another PostgreSQL server. Logical replication/decoding exposes changes in a logical, row-oriented form for a subscriber or connector.

### Why it exists

Physical replication is primarily availability/read-scale technology. Logical decoding is appropriate when a non-PostgreSQL consumer such as Debezium needs row changes.

### How it works internally

Physical standby replays WAL blocks to reproduce database storage. Logical decoding uses an output plugin to decode WAL into records with relation and row information.

### MDEP mapping

MDEP does not create a physical standby. It configures logical WAL and `pgoutput` for Debezium.

### Actual file/config reference

`ingestion/cdc/debezium-postgres-connector.json` sets `plugin.name` to `pgoutput`.

### Failure implications

Confusing the modes can lead to missing connector prerequisites or an incorrect claim that a replica is a CDC feed.

### Production considerations

Physical HA and logical CDC may coexist, but their slots, failover and retention plans must be tested together.

### Common misunderstanding

Logical replication does not mean every consumer gets an independently ordered global event stream without operational limits.

### Interview framing

“Physical replication copies database storage; logical decoding exposes row changes. MDEP needs the latter because Debezium is not a PostgreSQL standby.”

## Publication and replication slot

### Definition

A publication declares which PostgreSQL tables publish logical changes. A replication slot stores a consumer-resume point and prevents PostgreSQL from discarding required WAL too early.

### Why it exists

Publication narrows CDC ownership; slot state enables resume across connector restart.

### How it works internally

The publication contains four tables. Debezium attaches to `mdep_debezium_slot`; as capture confirms progress, the database can advance retained-WAL eligibility. `restart_lsn` is conceptually the oldest WAL location still needed by a slot; `confirmed_flush_lsn` is the position a consumer has confirmed processed.

### MDEP mapping

Only customers, products, orders and payments belong to CDC. REST locations/rates remain batch-owned and are not added to the publication.

### Actual file/config reference

`cdc-init.sql` creates `mdep_publication`; the connector names it and `mdep_debezium_slot`; `scripts/validate-mdep-10-cdc.ps1` queries `confirmed_flush_lsn` and current WAL position.

### Failure implications

A missing publication table creates a silent data gap. A missing slot can force rebootstrap. A lagging slot retains WAL and causes disk pressure.

### Production considerations

Alert on retained WAL bytes, slot inactivity and source disk. Do not drop/recreate a slot without deciding whether replay, re-snapshot, or reconciliation is required.

### Common misunderstanding

`confirmed_flush_lsn` is not a Kafka consumer offset; it belongs to the PostgreSQL replication boundary.

### Interview framing

“The slot is both recovery state and a liability: if Debezium is down, PostgreSQL may retain WAL until disk pressure becomes the incident.”

## CDC versus polling

### Definition

CDC consumes committed change history. Polling periodically queries current table state, usually via an `updated_at` watermark.

### Why it exists

CDC captures insert/update/delete semantics and source position with lower latency. Polling is simpler and remains suitable for MDEP's bounded reference batch path.

### How it works internally

Polling compares an interval/watermark and can miss ambiguous deletes or intermediate changes. CDC receives changes from WAL in source order subject to connector and downstream delivery semantics.

### MDEP mapping

MDEP-8 uses optional bounded PostgreSQL extraction for batch learning, while MDEP-10 captures the four current-state entities from WAL. The batch path must not become a second Silver writer for CDC entities.

### Actual file/config reference

Compare `ingestion/batch/extractors.py:postgres_rows` with `ingestion/cdc/debezium-postgres-connector.json`.

### Failure implications

Polling can produce duplicate/overlapping interval records; CDC can stall, retain WAL, or replay. Both need idempotent downstream handling.

### Production considerations

Choose based on source privileges, volume, deletion requirements, latency, recovery window and operational maturity—not tool popularity.

### Common misunderstanding

CDC does not automatically provide exactly-once end-to-end processing.

### Interview framing

“MDEP uses both patterns deliberately: polling/batch for references and CDC for relational mutations. They have different ownership and replay semantics.”

## Kafka Connect architecture

### Definition

Kafka Connect is a framework for running reusable source and sink connectors. A worker process hosts one or more connector instances; a connector creates one or more tasks; a source task reads an external system and produces Kafka records.

### Why it exists

It separates connector lifecycle, configuration and offset management from a custom application. Debezium is a Kafka Connect source connector rather than a Python polling loop.

### How it works internally

The worker loads connector plugins, validates connector configuration, creates tasks, stores connector configuration/status/offset state in Kafka Connect internal topics in distributed deployments, and supervises task lifecycle. A task obtains changes from the source and serializes records through configured converters.

### MDEP mapping

Compose runs one Debezium 3.0 `connect` service. Its environment declares `GROUP_ID=mdep-connect` and the config/offset/status topic names. The connector JSON is registered through the Connect REST endpoint in `scripts/validate-mdep-10-cdc.ps1`.

### Failure implications

Connector failure can stop one capture job while a worker remains healthy. Worker failure can stop all hosted tasks. Either can increase slot lag and retained WAL.

### Production considerations

Distributed workers provide assignment and fault tolerance, but internal topic replication, worker compatibility, connector version rollout and offset durability need explicit operational design.

### Common misunderstanding

Kafka Connect is not Kafka itself and does not make a connector exactly once merely by storing offsets.

### Interview framing

“Connect is the runtime; Debezium is the source connector plugin; its task reads PostgreSQL and emits Kafka records.”

## Worker, connector, task, and lifecycle

### Definition

A worker is the Connect process. A connector is a configured integration instance. A task is the runnable unit assigned by the connector. MDEP sets `tasks.max=1`, so its small lab connector has one capture task.

### Why it exists

The separation lets a production Connect cluster distribute connectors/tasks without making every connector author implement scheduling and offset storage.

### How it works internally

In standalone mode one process owns configuration and tasks locally. In distributed mode workers join a group, co-ordinate assignment, and use internal Kafka topics for shared configuration/status/offset state. A connector restart recreates its task; a worker restart affects all hosted tasks.

### MDEP mapping

The Compose topology is a single Connect container and single Kafka broker. It is a focused lab, not a distributed Connect demonstration.

### GENERAL CONCEPT — NOT IMPLEMENTED/EXERCISED IN MDEP

Worker rebalancing, multiple tasks for a connector, rolling worker upgrade and replicated internal-topic recovery are not exercised here.

### Failure implications

Lost Connect offsets can alter resume/re-snapshot decisions even if the PostgreSQL slot still exists. Slot state and Connect offsets are related recovery evidence, not one interchangeable checkpoint.

### Interview framing

“I distinguish worker failure from connector/task failure because their blast radius and recovery evidence differ.”

## MDEP Debezium connector configuration groups

### Infrastructure and location settings

`connector.class` selects `io.debezium.connector.postgresql.PostgresConnector`. `database.hostname=postgres`, `database.port=5432`, `database.user=lab`, and `database.dbname=commerce` identify the Compose source. The password is a local lab credential in connector configuration; production would inject a secret rather than commit a usable credential.

### Logical-replication settings

`plugin.name=pgoutput` chooses PostgreSQL's standard logical-decoding output plugin. `publication.name=mdep_publication` and `slot.name=mdep_debezium_slot` bind Debezium to the source prerequisites. `slot.drop.on.stop=false` preserves resume state but makes lag monitoring mandatory.

### Routing and serialization settings

`topic.prefix=mdep` and `table.include.list=commerce.customers,commerce.products,commerce.orders,commerce.payments` derive the four MDEP topics. Compose configures JSON key/value converters with schemas disabled, so consumers receive schemaless JSON values. This reduces local setup but moves schema compatibility discipline into contracts/tests; it is not a schema-registry design.

### Change-event settings

`snapshot.mode=initial` requests baseline `r` records before WAL streaming. `tombstones.on.delete=true` requests a null Kafka tombstone after a delete envelope. `provide.transaction.metadata=true` is the corrected Debezium 3.0 setting for transaction metadata.

## Initial snapshot semantics

### Definition and purpose

An initial snapshot supplies a baseline for existing source rows before the connector streams later WAL mutations. Without it, a downstream current-state consumer could see only changes occurring after connector startup.

### Internal mechanism

Conceptually, Debezium establishes a consistent source boundary, reads table rows, emits `op=r` records with `before=null` and `after` equal to the captured row, then transitions to WAL streaming from the corresponding position. Snapshot source metadata/flag tells consumers that a record is bootstrap evidence rather than a normal update.

### MDEP mapping

`snapshot.mode=initial` is present in the JSON and asserted by `tests/test_cdc_contracts.py`. MDEP-11 accepts a first snapshot event as initial keyed state because snapshot records may lack normal transaction metadata.

### Failure and restart implication

An interrupted snapshot can require resume or re-snapshot decisions. Re-snapshot may replay baseline rows, so downstream must be idempotent and reconciliation-aware. No physical snapshot has run in this repository; this is **CONFIGURED / RUNTIME DEFERRED — not runtime validated**.

### Common misunderstanding

Debezium snapshot is not the same as MDEP's separate scheduled batch ingestion. Batch also teaches REST/file landing, deterministic object paths and bounded backfill; it is not replaced by CDC bootstrap.

## Change-event envelope and operations

`before` is the prior row image where emitted; `after` is the new row image for create/update; `source` identifies source metadata including LSN and transaction-related facts; `op` identifies the operation; timestamps such as `ts_ms` represent connector/source timing fields whose exact presence must be observed rather than assumed.

For `order_id=1001`, initial snapshot is `r` with `before=null` and a current `after`. An INSERT is `c`; an update from `created` to `paid` is `u` with old/new images; a delete is `d` with `after=null` and an identifying key/before image. The following null tombstone is a separate Kafka compaction record, not the delete event itself.

## Transaction metadata and the MDEP correction

`source.txId` is source-side PostgreSQL transaction information. Top-level Debezium `transaction.id` identifies transaction metadata; `transaction.total_order` can order events in the transaction context; `transaction.data_collection_order` supplies collection-order context. They are not synonyms for Kafka offsets and not replacements for source LSN.

MDEP originally used `include.transaction`; review found it was not the intended Debezium PostgreSQL 3.0 enablement property. The JSON now uses `provide.transaction.metadata=true`, and the test reads the actual JSON to prevent regression. This configuration can enable enrichment and transaction boundary information where supported, but it does **not** prove any BEGIN/END event was observed. Module 05 uses transaction total order only for relevant equal-LSN/same-transaction state decisions.
