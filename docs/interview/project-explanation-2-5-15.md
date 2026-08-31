# MDEP Project Explanation — 2, 5, and 15 Minutes

Use these as adaptable spoken narratives, not scripts to recite word-for-word. Each version follows the same story: problem, architecture, correctness decisions, and truthful V1 boundary.

## 2-minute version — “Briefly walk me through this project”

I built MDEP as an interview-oriented Commerce and Operations data-engineering project. The goal was to practice one coherent architecture that handles several source types rather than collecting unrelated tools.

On the batch side, PostgreSQL snapshots, a paginated REST API, and CSV/JSON files are orchestrated into source-aligned Bronze Parquet. Bronze is the replayable evidence layer. The important design choice is that a batch run has a deterministic identity based on its logical date, UTC interval, source, and entity. For example, REST pages are all collected before publishing, so a late page failure does not create a partial canonical Bronze object.

Spark owns the batch reference-data path from Bronze into Iceberg Silver. It validates records, quarantines invalid data, deduplicates deterministically, and uses a lexicographic freshness order. A changed hash alone cannot make an old replay newer.

For PostgreSQL changes, Debezium reads logical changes through a declared publication and replication slot, then Kafka carries the CDC messages. Flink owns CDC current-state Silver. Its key rule is that source LSN wins first, transaction order resolves defined ties, and a stale delete cannot remove newer state. Spark and Flink therefore do not compete to write the same Silver dataset.

Downstream, Snowflake exposes Silver Iceberg externally and dbt builds Gold dimensions, facts, and marts. dbt owns Gold, not Silver. I also implemented validation contracts and reconciliation templates, because tests alone do not prove that different layers represent the same business truth.

For V1, the code, configuration, offline tests, reconciliation logic, and failure reasoning are complete. I do not claim that the full Airflow, Kafka, Flink, Spark, Iceberg, Snowflake, and dbt stack ran together; that physical validation is explicitly runtime deferred to a V1.x lab.

### Memory anchors

1. One project, multiple source types, one ownership model.
2. Bronze is replayable evidence; retry is not data idempotency by itself.
3. Spark owns batch/reference Silver; Flink owns CDC current state.
4. Freshness comes from source/version evidence, not payload difference.
5. Snowflake reads Silver; dbt owns Gold.
6. Offline proof is real, but runtime proof is deferred.

## 5-minute version — “Walk me through the architecture and main decisions”

MDEP is a Commerce and Operations portfolio project designed to demonstrate practical Data Engineering judgment across batch ingestion, CDC, lakehouse processing, warehouse modeling, and reliability. I deliberately kept the technology set bounded and focused on correctness boundaries rather than trying to demonstrate every possible tool.

The sources are PostgreSQL commerce tables, paginated REST reference data, and CSV/JSON files. PostgreSQL has two different roles: bounded batch snapshots are one batch input, while Debezium is the canonical change path for CDC-owned commerce tables. That distinction avoids having a batch job and a CDC job both publish competing current state.

For batch ingestion, Airflow provides scheduling, retry, and backfill control. The data identity is separate from Airflow itself: `BatchContext` uses a logical date, UTC `[start,end)` interval, source name, and entity to derive a deterministic ingestion ID and Bronze key. REST extraction accumulates every page before it returns; if page three exhausts retries, pages one and two stay in memory rather than becoming partial canonical Bronze. File ingestion uses content SHA-256 to identify renamed duplicate files, while the downstream Silver natural key remains a separate business concept. Bronze and quarantine preserve source and rejection evidence.

Once raw evidence is landed in Bronze, Spark owns the reference-data Silver path, specifically exchange rates and locations. It validates schema/business rules, sends invalid/non-winning records to quarantine, and selects a deterministic winner per business key. Its Iceberg MERGE uses the same version ordering as in-batch dedup: business time, source extract time, ingestion time, then `record_hash`. That last field is a deterministic tie-breaker, not proof of business recency, which prevents an older replay with a different payload from regressing Silver.

For current-state CDC, the important boundary is PostgreSQL WAL through Debezium and Kafka, then Flink. The connector configuration uses `pgoutput`, an explicit publication and replication slot, initial snapshot, tombstones, and Debezium transaction metadata configuration. A publication is a source prerequisite; preserving a slot supports resume but can retain WAL if the consumer stops. Kafka topic/key rules stay at the transport boundary. Then the Flink CDC model normalizes an event and decides whether it may change current state. It compares source LSN first, then same-transaction total order when available, then known transport identity for exact replay. Equal position with missing evidence is a conflict, not a guessed replay. A delete has no special privilege: it must win the same ordering before it removes state.

Flink’s runtime topology uses a parser, side output for rejected messages, keyed state by `entity:primary_key`, and Iceberg sinks for event evidence/current state. The pure Python CDC model exists so the semantic rules can be unit tested separately from a distributed runtime. Checkpoint and exactly-once settings are present as implementation/configuration, but I do not overclaim end-to-end delivery proof.

Downstream, Snowflake does not become another Silver writer. It has external Iceberg access to canonical Silver, while dbt reads those sources through staging and intermediate models and owns native Gold dimensions, facts, and marts. For example, `fct_orders` is incremental by order key, but merge alone does not remove rows absent upstream, so the model includes an anti-join delete post-hook. `fct_payments` intentionally rebuilds as a table because current scale favors clear delete/relink correctness over another incremental contract. Missing DKK rates remain explicit null/flag evidence, and orphan payments are preserved by left joins with warning-level quality signaling.

Finally, I separate validation from reconciliation. The evidence runner records native exit code, stdout, stderr, and working directory so a nonzero native command cannot be falsely reported as PASS. Reconciliation compares business keys, attributes, deletes, duplicates, nulls, and aggregates at declared grain; matching row counts alone is weak evidence.

V1 is complete as an implemented, offline/static-validated, documented, interview-ready baseline. Physical cross-system execution remains runtime deferred, and I would treat that as a focused V1.x validation lab rather than saying it has already passed.

### Memory anchors

1. Multiple sources, two distinct ingestion responsibilities.
2. Deterministic Bronze makes bounded retries/backfills safe.
3. Spark reference Silver and Flink CDC Silver have separate ownership.
4. Version evidence beats hash/offset intuition.
5. Iceberg is canonical Silver; Snowflake/dbt is Gold analytics.
6. Delete propagation is designed explicitly at each layer.
7. Tests execute checks; reconciliation compares business truth.
8. V1 runtime is deferred, never claimed as passed.

### Places where an interviewer may interrupt

- Why use batch and CDC for PostgreSQL?
- Airflow retry versus data idempotency.
- Spark MERGE/version ordering and stale replay.
- Publication, slot, tombstone, and transaction metadata.
- LSN ordering, stale delete, or equal-position conflict.
- Flink state/checkpoint/exactly-once boundary.
- Iceberg writer ownership and Snowflake external access.
- dbt incremental deletes, FX, or orphan payments.
- Validation runner and semantic reconciliation.
- What remains runtime deferred.

## 15-minute version — deep architecture walkthrough

### 1. Problem and objectives

I built MDEP around a Commerce and Operations domain so I could practice a realistic data platform without tying the project to one industry. The objective was not tool coverage. It was to implement and explain the boundaries that make a multi-source system trustworthy: source ownership, replayable ingestion, versioned current state, analytical modeling, and evidence-based validation.

### 2. High-level architecture

There are two ingestion paths that meet at canonical Iceberg Silver. The batch path is PostgreSQL snapshots, REST, and files through Airflow to Bronze Parquet and Spark. The CDC path is PostgreSQL WAL through Debezium and Kafka into Flink, which writes a CDC Bronze archive and CDC current-state Silver. Snowflake accesses canonical Silver externally, and dbt owns Gold dimensions, facts, and marts. Validation and reconciliation span all paths.

### 3. Batch path

The batch path exists for reference/API/file ingestion and bounded replay. Airflow is the control plane: it schedules a daily UTC interval, retries failures, and supports explicit historical backfill. It is not the data identity mechanism. `BatchContext` represents one logical landing operation and deterministically derives the Bronze object identity.

REST extraction makes a deliberate all-or-fail choice. It follows pages, retries bounded retryable failures, and only returns a record set when every page has succeeded. This prevents a partial page set from looking like canonical source evidence. File ingestion uses byte-content identity, so a renamed file with the same bytes is quarantined as duplicate content rather than re-landed as a new source fact.

### 4. Bronze design

Bronze is source-aligned, replayable evidence with metadata such as ingestion identity, source locator, extract time, ingestion time, and record hash. It is not business-current-state truth. Deterministic keys plus conditional publication make the same logical retry converge rather than produce arbitrary duplicate canonical objects. Quarantine preserves rejected payload and reason rather than silently losing data.

### 5. Spark/reference Silver

Spark owns the batch/reference Silver datasets, currently exchange rates and locations. It validates values, types, and reference constraints; invalid records become quarantine evidence. Within a batch it uses a deterministic window winner; against existing Iceberg state it uses a matching MERGE predicate. For locations the ordering is `updated_at`, then source extract timestamp, ingestion time, and only then record hash. For exchange rates, `retrieved_at` is the first field. The reason is to prevent an older Bronze replay from overwriting newer state simply because it has a different hash.

### 6. CDC transport

CDC is deliberately different from periodic extraction. PostgreSQL changes are exposed through logical decoding using a named publication and replication slot. Debezium uses `pgoutput`, the exact four commerce tables, an initial snapshot, topic prefix `mdep`, tombstones on delete, and `provide.transaction.metadata=true`. The publication is intentionally not auto-created by the connector, so source replication scope remains explicit. A preserved slot allows resuming, but it can retain WAL and pressure source disk when a connector lags; that is a recoverability-versus-capacity trade-off.

At the transport boundary, topic and Kafka key contracts restrict scope and identity. A Debezium `op=d` event has `after=null`; it is the business delete candidate. A following Kafka null-value tombstone is a log-compaction message, not another business delete.

### 7. CDC current-state semantics

Raw change events are not automatically the current business state. MDEP normalizes them into `CdcEvent` and maintains state by `entity:primary_key`, preventing collisions between equal textual IDs in different entities. `version_decision` uses PostgreSQL source LSN first. If LSN ties within the same transaction, total order can resolve it. Known identical topic/partition/offset can prove exact replay. If position is equal but evidence is insufficient, the model returns an equal-position conflict rather than guessing.

This protects the key invariant: a stale delete cannot remove a row updated by a higher LSN. A valid delete envelope with LSN 420 loses to accepted state LSN 500, so it is ignored. The pure `apply_current_state` dictionary is a semantic test oracle; it is not the production state mechanism.

### 8. Flink implementation

The Flink job turns the same semantic rule into a streaming topology: source, raw Bronze archive, parser, rejection side output, `keyBy(entity:primary_key)`, managed `ValueState`, and Iceberg sinks. ValueState is necessary because a plain Python dictionary has no distributed keyed-state, checkpoint, or restart semantics. Checkpoint/exactly-once configuration belongs to the implementation, but successful end-to-end checkpoint recovery has not been observed in V1. The current PyFlink Kafka source is value-only, so complete transport metadata is an explicit limitation rather than an invented guarantee.

### 9. Iceberg and ownership model

Iceberg is the canonical Silver storage/table format because it supports the intended Spark/Flink interoperability, schema evolution, and current-state merge patterns while keeping storage separate from processing engines. The most important rule is one canonical Silver writer per dataset: Spark owns batch reference entities, Flink owns CDC commerce current state, and dbt does not write Silver. That avoids conflicting upserts and unclear source of truth.

### 10. Snowflake integration and dbt analytics

Snowflake has external access to Iceberg Silver; it does not copy or take over Silver. dbt declares the six Silver sources, shapes them in staging, enriches them in intermediate models, and owns Gold. Gold models have declared grains: dimensions represent current entities, `fct_orders` one current order, `fct_payments` one current payment, and `mart_daily_sales` one order date plus source currency.

`fct_orders` is incremental with `order_id` merge key and selects candidates by `applied_at` or `updated_at` against target maxima. That selection does not imply delete synchronization, so a post-hook anti-join removes target orders absent from enriched source. `fct_payments` is intentionally a full table rebuild because this makes delete/relink behavior clearer at project scale. Missing FX uses safe conversion and `missing_dkk_rate`; orphan payment evidence is retained through left joins and warning-level relationship tests.

### 11. Data quality and reconciliation

Tests verify individual behavior; reconciliation asks whether layers represent the intended same business truth despite different semantics and grain. MDEP records a validation matrix and uses a runner that captures native exit codes, stdout, stderr, and working directory. A native exit code 7 must be FAILED even if PowerShell itself does not throw.

R01–R10 cover source/Silver and Silver/Gold key/attribute checks, missing FX, orphan facts, accepted deletes, duplicate keys, required nulls, and fact-to-mart aggregation. Count equality is not key-set equality; key-set equality is not attribute equality; attribute equality is not aggregate correctness.

### 12. Failure and recovery examples

If REST page three fails, the safe action is to abort before Bronze, retry the same logical operation, then reconcile the bounded source set. If an old Spark record replays, it cannot win based on changed payload/hash. If a lower-LSN CDC delete arrives, it is ignored before current state changes. If evidence cannot establish an equal-position replay, the event is conservatively rejected as conflict. If a slot lags, operators must inspect slot/WAL evidence before dropping recovery state. Missing FX and orphan payments remain visible rather than being silently normalized away.

### 13. Trade-offs

The design accepts some deliberate lab limitations. Deterministic evidence and offline semantic tests improve explainability, but do not replace live integration. External Silver avoids a second managed copy, but requires cross-engine access validation later. Incremental facts reduce processing work, but need explicit delete behavior; a rebuild may be simpler and more correct at smaller scale. Conservative CDC conflicts protect state but require later operational treatment.

### 14. What V1 proves and what it does not

V1 proves real implementation, configuration, source-grounded contracts, pure/static tests, reconciliation design, failure reasoning, and an interview-ready architecture story. It does not prove that Docker services, Airflow, PostgreSQL replication, Debezium, Kafka, Flink checkpoints, Spark/Iceberg, S3, Snowflake, and dbt ran together. Those physical exercises are explicitly runtime deferred to a future V1.x hands-on lab.

### 15-minute whiteboard anchors

```text
PostgreSQL snapshots + REST + CSV/JSON
                    -> Airflow / BatchContext
                    -> Bronze Parquet (evidence + quarantine)
                    -> Spark -> reference Silver Iceberg

PostgreSQL WAL -> Debezium -> Kafka
                    -> Flink parser / side output / ValueState
                    -> CDC Bronze archive + CDC Silver Iceberg

Canonical Silver Iceberg -> Snowflake external access -> dbt
                    -> Gold dimensions / facts / marts
                    -> validation + reconciliation
```

Annotate the drawing with: one Silver writer per dataset; batch freshness tuple; CDC LSN-first ordering; Gold-only dbt ownership; and “implemented/offline tested, runtime deferred.”
