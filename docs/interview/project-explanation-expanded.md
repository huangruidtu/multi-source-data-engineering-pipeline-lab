# MDEP Expanded Interview Explanation

This is a project-specific understanding reference, not a new source of truth. Read it with the [Code Deep-Dive Master Map](../code-deep-dive/master-map.md) and [Practical Training](../practical-training/README.md). It distinguishes implemented source/configuration, offline evidence, and runtime-deferred work.

## 1. What this project is

MDEP is an interview-oriented Commerce and Operations Data Engineering implementation. It demonstrates a coherent path from multiple source types through batch and CDC processing into Bronze, Silver, and Gold.

V1 is not a production deployment claim. Its acceptance model is real implementation, architecture correctness, offline/static and fixture-driven tests, reconciliation logic, documented failures/trade-offs, and interview readiness. Physical Airflow, Spark, Flink, Kafka, PostgreSQL CDC, S3, Snowflake, and dbt integration is **MDEP RUNTIME DEFERRED** for a later V1.x lab.

## 2. Architecture at a glance

```text
PostgreSQL snapshots + REST + CSV/JSON -> Airflow -> Bronze Parquet -> Spark -> reference Silver Iceberg
PostgreSQL WAL -> Debezium -> Kafka -> Flink -> CDC Bronze archive + CDC current-state Silver Iceberg
Canonical Silver Iceberg -> Snowflake external access -> dbt -> Gold dimensions, facts, marts
                                                     -> validation and reconciliation
```

| Layer | What/why | Owner | Enters / leaves |
| --- | --- | --- | --- |
| Sources | operational and reference truth | source systems | rows, API pages, files, WAL changes |
| Bronze | replayable source/rejection evidence | batch/Flink landing paths | source-aligned records -> Spark/Flink input |
| Silver | validated/conformed current or reference state | Spark for reference; Flink for CDC | Bronze/CDC -> Iceberg tables |
| Gold | analytical dimensions, facts, marts | dbt | external Silver -> Snowflake-native analytics |
| Validation | prove bounded behavior and semantic agreement | `validation/` | tests/templates -> evidence decisions |

## 3. Source systems and ingestion strategy

PostgreSQL supplies customers, products, orders, and payments. Batch snapshots demonstrate bounded extraction; Debezium is the canonical CDC path for the CDC-owned commerce tables. REST supplies paginated exchange-rate/location reference data. CSV/JSON files exercise file schema, duplicate-content, and malformed-input handling.

Batch and CDC coexist because they answer different needs. Batch is suited to reference data and bounded replay; CDC is suited to current-state change propagation. They must not independently write the same canonical current-state Silver dataset.

## 4. Batch ingestion and Bronze

Airflow is the control plane: daily UTC scheduling, retries, and explicit backfill. `catchup=False` stops automatic scheduler catchup; it does not prohibit an intentional historical run. A `BatchContext` has logical date, `[start,end)`, source, and entity. Its deterministic ingestion ID creates one deterministic Bronze key for one logical operation.

`fetch_paginated_json` reads all pages before it returns. If the final page fails after retries, earlier successful pages remain in memory and no canonical object is published. This separates extraction completeness from publication. File identity is SHA-256 of bytes: a renamed equal-byte file is duplicate evidence, not a different business entity. Quarantine keeps original payload, reason, locator, and context.

Local and S3 publishers share the same conditional-publication intent: retries converge on the same object identity rather than inventing another canonical output. **Common misunderstanding:** Airflow retry is not data idempotency; stable context plus conditional publication provides that property. **Interview interpretation:** explain the all-or-fail boundary before naming Airflow.

## 5. CDC transport

PostgreSQL WAL is the change log; logical decoding makes selected changes consumable. The checked-in Debezium PostgreSQL connector uses `pgoutput`, a named `mdep_publication`, `mdep_debezium_slot`, exact four-table include list, topic prefix `mdep`, initial snapshot, tombstones, and `provide.transaction.metadata=true`.

`publication.autocreate.mode=disabled` means the publication is a source prerequisite rather than a connector side effect. `slot.drop.on.stop=false` preserves resume position, but an inactive/lagging slot can retain WAL and pressure source disk. Monitor connector state plus PostgreSQL slot state, `restart_lsn`, `confirmed_flush_lsn`, current WAL position, and disk growth. Monitoring drills are **GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED**.

Topics follow `mdep.commerce.<table>` and Kafka keys carry the business key. `r/c/u/d` are snapshot read/create/update/delete. A `d` envelope has `after=null`; a following Kafka tombstone is compaction transport semantics, not a second business delete. Configuration/contracts are **MDEP IMPLEMENTED** and **MDEP OFFLINE TESTED**; observed WAL, topics, snapshots, and metadata are **MDEP RUNTIME DEFERRED**.

## 6. CDC current-state semantics

A raw CDC record is not automatically current business state. `CdcEvent` normalizes entity, primary key, operation, before/after, source LSN, transaction metadata, optional transport coordinates, and snapshot evidence. `key_identity` is `entity:primary_key`, preventing cross-entity textual-ID collisions.

`version_decision` is the core rule:

| Decision | Meaning |
| --- | --- |
| `NEWER` | may mutate accepted state |
| `LOWER_LSN` | older PostgreSQL source position; ignore |
| `LOWER_TRANSACTION_ORDER` | older change within same transaction; ignore |
| `EXACT_REPLAY` | same known transport identity; ignore |
| `EQUAL_POSITION_CONFLICT` | evidence cannot safely distinguish candidate/replay; reject conservatively |

Ordering is source LSN first. Same LSN can be resolved by `transaction.total_order` only when same transaction evidence exists. Same LSN alone is not duplicate, and missing partition/offset cannot prove replay just because `None == None`.

Example: accepted order update at LSN `0/500`; later valid delete envelope at `0/420`. The delete loses as `LOWER_LSN`; `apply_current_state` returns `lower_lsn_ignored`, retaining both current row and accepted version. A Kafka null tombstone parses to `None` and is `tombstone_ignored`; it is not business state mutation.

`apply_current_state` is a pure in-memory semantic oracle for tests. Flink managed state is the durable runtime mechanism. **Common misunderstanding:** a Python dictionary can represent state but cannot provide distributed keyed state, checkpointing, or restart recovery. The current model is **MDEP IMPLEMENTED/OFFLINE TESTED**; physical consumer behavior is runtime deferred.

## 7. Flink streaming layer

The Flink topology is source -> raw Bronze archive -> parser -> rejection side output -> `keyBy(entity:primary_key)` -> `ValueState` -> current-state/event Iceberg sinks. Parser failures retain raw evidence via quarantine/side output. Row kinds represent insert/update/delete behavior at the sink boundary.

The pure model and runtime job coexist so semantic ordering can be unit tested without a JVM, while `ValueState` provides runtime partitioning/recovery. Checkpoint and exactly-once configuration are implementation facts, not end-to-end delivery proof. Current limitations include a value-only PyFlink Kafka source, so complete key/topic/partition/offset retention is not claimed, and lab parallelism is not a production scalability demonstration.

## 8. Spark Silver

Spark owns batch reference Silver—exchange rates and locations—not CDC commerce current state. It reads Bronze Parquet, validates business/schema constraints, quarantines invalid records, deterministically chooses a window winner, and MERGEs into Iceberg.

The exact freshness tuple is lexicographic. Locations use `updated_at -> source_extract_ts -> ingested_at -> record_hash`; rates use `retrieved_at -> source_extract_ts -> ingested_at -> record_hash`. The target MERGE mirrors the in-batch order. Example: existing location updated 2026-08-20 beats a replay updated 2026-07-01 even if the replay has a later landing timestamp and different hash. `record_hash` is only the final deterministic tie-breaker; it is not a freshness signal. This corrects the historical unsafe idea that “different hash means update.” Additive schema evolution and quarantine are implemented design paths; physical Spark/Iceberg execution is deferred.

## 9. Why Iceberg and ownership

Iceberg is canonical Silver storage because MDEP needs open-table semantics for Spark/Flink interoperability intent, schema evolution, current-state MERGE/upsert behavior, storage/compute separation, and downstream Snowflake external access. The selected shared catalog/storage intent is configured, but no cross-engine runtime proof is claimed.

One canonical Silver writer per dataset is critical: Spark owns batch references; Flink owns CDC commerce state; dbt owns Gold only. A Snowflake-managed Silver copy may be a valid different architecture, but in MDEP it would introduce synchronization, freshness, lineage, and “which copy is canonical?” questions.

## 10. Snowflake and dbt

Snowflake creates external access objects for six Silver Iceberg tables under `MDEP.SILVER_EXT`; it does not own/write Silver. dbt sources read that layer, staging shapes fields, intermediate models enrich, and Gold contains dimensions, facts, and marts. Grains are explicit: one current order for `fct_orders`, one current payment for `fct_payments`, and order-date plus source currency for `mart_daily_sales`.

`fct_orders` is incremental with `unique_key='order_id'`. Its selection predicate considers incoming `applied_at` or `updated_at` against target maxima. Incremental selection is not delete synchronization: its post-hook anti-join deletes Gold targets absent from `int_orders_enriched`. `fct_payments` intentionally uses a full table rebuild, which is simpler for delete/relink correctness at this scale; incremental is not automatically more advanced.

`safe_currency_conversion` returns null for unavailable/non-positive rate, while `missing_dkk_rate` preserves the uncertainty. Payment/order joins are left joins: orphan evidence stays visible and the relationship test is warning severity, not a row-deletion operation. Historically, model schema override `schema: gold` plus profile schema `GOLD` could resolve as `GOLD_GOLD`; the override was removed. This was schema-resolution risk, not a literal setup-SQL typo.

## 11. Validation versus reconciliation

Testing asks, “Did this check execute and pass?” Reconciliation asks, “Do these layers represent the same intended business truth despite different semantics and grain?”

The evidence runner preserves stdout/stderr, `$LASTEXITCODE`, working directory, run ID, and stage status. A native nonzero exit must be `FAILED`, even when PowerShell does not throw; this avoids false PASS. The matrix status vocabulary is `PASSED`, `FAILED`, `BLOCKED`, and `NOT_RUN`. Historical `BLOCKED` reflects unavailable physical capability; it is not changed to PASS. Final V1 scope calls those physical exercises runtime deferred.

R01–R10 cover source/Silver and Silver/Gold keys/attributes, missing FX, orphan facts, accepted deletes, duplicate current-state keys, required nulls, and fact-to-mart aggregation. The essential rule is:

```text
count equality != key-set equality != attribute equality != aggregate correctness
```

Use anti-joins for keys, compare attributes at matching grain, and aggregate facts to the mart grain. Templates are implemented; cross-system execution is deferred.

## 12. Failure and recovery examples

| Symptom | Root mechanism | Correct behavior | Verify |
| --- | --- | --- | --- |
| REST final page fails | incomplete bounded extract | abort before canonical Bronze; retry same context | one key, complete source/Silver key set |
| renamed same file | equal bytes, different locator | preserve duplicate quarantine evidence | content identity/disposition |
| old Spark replay | lower business version | no MERGE update despite hash difference | version tuple/current row |
| stale CDC delete | lower LSN | ignore before state mutation | accepted version/current key |
| equal CDC position | incomplete identity/order evidence | quarantine/conservative conflict | metadata and conflict evidence |
| WAL growth | retained inactive/lagging slot | inspect source/slot before destructive action | LSNs, disk, connector state |
| malformed Flink event | invalid envelope/source fields | side output/quarantine | payload and reason |
| Silver/Gold key mismatch | selection/delete/scope/quality defect | preserve keys; isolate before repair | R03/R08/R09/R10/R05 |
| missing FX/orphan | unknown reference/integrity | retain null/flag/orphan evidence | flags, warning, exceptions |

## 13. Major decisions and trade-offs

| Decision | Why | Trade-off / alternative |
| --- | --- | --- |
| batch plus CDC | references differ from current-state changes | more paths, but explicit ownership |
| Bronze evidence | replay/audit/quarantine | storage and metadata discipline |
| Spark vs Flink ownership | match bounded references vs stateful CDC | must prevent overlapping writers |
| Iceberg Silver | open table semantics and external access intent | needs catalog/runtime validation |
| external Snowflake Silver | avoid duplicate canonical state | physical cross-engine access deferred |
| incremental orders, rebuild payments | choose complexity where needed | rebuild costs more at scale |
| preserve uncertainty | avoid fake zeros/hidden orphans | downstream consumers need quality awareness |
| offline V1 acceptance | implementation depth and honest evidence | no full runtime acceptance claim |

## 14. What I can truthfully claim in an interview

### MDEP IMPLEMENTED

Batch/Bronze code, Spark and Flink job/model code, Debezium configuration/contracts, Iceberg/Snowflake/dbt definitions, validation runner, reconciliation templates, and documentation exist in the repository.

### MDEP OFFLINE TESTED

Python/unit/static contracts cover batch idempotency patterns, Spark version ordering, actual connector JSON, CDC current-state decisions/topology, warehouse/dbt contracts, closure policy, and validation runner safeguards.

### MDEP RUNTIME DEFERRED

No claim of physical Docker/Airflow, PostgreSQL replication/Debezium/Kafka, Spark/Iceberg/S3, Flink checkpoints, Snowflake/dbt, or cross-system reconciliation execution. These are future V1.x hands-on exercises with evidence matrix/runbook support.

### GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED

Production HA, secret/RBAC hardening, schema registry, scalable connector operations, monitoring/alerts, disaster recovery, and fully automated integration CI are discussion extensions, not MDEP features.

## 15. How to use this document with ChatGPT

Paste the relevant section and state: “Use only source-supported MDEP facts; mark general explanations separately and do not claim runtime execution.” Then try prompts such as:

1. “Using this MDEP context, explain replication slots in simpler terms.”
2. “Explain why stale delete LSN 420 cannot remove state at LSN 500.”
3. “Teach me `fct_orders` incremental selection versus delete synchronization.”
4. “Quiz me on the ownership boundary between Spark, Flink, Iceberg, Snowflake, and dbt.”
5. “Turn the REST partial-extraction incident into a senior interview question.”
6. “Challenge my explanation of delete versus Kafka tombstone.”
7. “Give me a reconciliation incident where counts match but business keys differ.”
8. “Review this answer for overclaiming runtime evidence.”
