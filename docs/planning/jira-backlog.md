# Jira Backlog — Multi-Source Data Engineering Pipeline Lab

**Status:** PLANNED. Issue keys below are proposed identifiers for later Jira creation only. No Jira issues have been created.

## Epic MDEP-E01 — Data Contracts and Reproducible Sources

### Story MDEP-S01 — Define Commerce & Operations data contracts and ownership

- **Context:** V1 needs consistent entities across PostgreSQL, REST, files, Kafka, and downstream layers before implementation begins.
- **Goal:** Define a small domain contract for customers, products, locations, orders, payments, device events, exchange rates, and reference files, including which path owns each canonical Silver dataset.
- **Why this matters in Data Engineering:** Reliable pipelines start with explicit schemas, keys, grain, and ownership—not transformations.
- **Learning objectives:** Translate business entities into data contracts; identify grain, natural keys, event identifiers, mutation semantics, and quality rules.
- **Concepts to understand before implementation:** Source-of-truth ownership, schema versus data contract, event key, idempotency key, ingestion metadata, fact grain, late data.
- **Architecture context:** Establishes contracts for PostgreSQL/REST/files → Bronze and PostgreSQL → Debezium → Kafka → Flink; all feed the shared Silver/Gold design.
- **Implementation scope:** Produce a source-to-layer mapping, schemas, key rules, quality expectations, partitioning candidates, and a compact data dictionary. Decide the CDC-owned tables and batch-owned tables. No executable pipeline.
- **Sub-tasks:**
  - Map entities, relationships, grains, and source systems.
  - Define batch record and Kafka event envelopes, including metadata fields.
  - Specify insert/update/delete and duplicate semantics for one CDC entity.
  - Define Bronze, Silver, quarantine, and Gold ownership boundaries.
  - Record the HadoopCatalog/S3 and Snowflake object-store integration decision.
- **Hands-on validation:** Walk through an order update, a payment failure event, a duplicate file row, and a late device event; confirm every record has one canonical destination and one quality disposition.
- **Failure scenarios:** Ambiguous ownership, missing primary key, event without an idempotency key, incompatible source datatype, and a dimension arriving after a fact.
- **Acceptance criteria:** Contracts name fields/types/keys and quality rules; each canonical Silver table has one writer path; dimensions and facts have stated grain; unresolved choices are recorded as decisions.
- **Interview questions:** What makes a data contract useful? How do you prevent dual-write inconsistencies? What is the difference between an event id and a business key? How do you define fact-table grain?
- **Documentation requirements:** Add planned data dictionary and ownership matrix; mark all content `PLANNED`.
- **Estimated hands-on time:** 6–8 hours.
- **Priority:** MUST HAVE.

### Story MDEP-S02 — Build reproducible source systems and intentionally imperfect data

- **Context:** The charter requires PostgreSQL, REST, CSV/JSON, and Kafka event patterns, including bad data and source mutations.
- **Goal:** Prepare small, resettable source datasets and a minimal REST behavior definition that can support both normal and failure exercises.
- **Why this matters in Data Engineering:** Source behavior—not just clean tables—is what drives ingestion, quality, and recovery design.
- **Learning objectives:** Model relational constraints, formulate API pagination/rate-limit behavior, distinguish file identity from content, and seed controlled defects.
- **Concepts to understand before implementation:** Primary/foreign keys, WAL prerequisites, pagination cursor, rate limiting, CSV/JSON schema validation, malformed records, deterministic fixtures.
- **Architecture context:** Supplies the upstream PostgreSQL, REST, and files inputs for Airflow/Spark and PostgreSQL mutations for Debezium/Kafka.
- **Implementation scope:** Manually create source PostgreSQL schema/data, a small REST source behavior or stable public-equivalent fixture, CSV/JSON reference files, and a reset procedure. Seed duplicates, nulls, malformed records, a missing-file case, and source update/delete examples. Do not build platform processing.
- **Sub-tasks:**
  - Create relational source entities and deterministic seed data.
  - Define and exercise REST pagination, retryable failure, and rate-limit behavior.
  - Create CSV/JSON fixtures with valid and invalid records plus duplicate/late files.
  - Write a manual reset and mutation checklist.
  - Confirm PostgreSQL logical replication prerequisites without starting CDC processing.
- **Hands-on validation:** Reset sources twice; compare expected counts; page through API data; inject a retryable API error; locate a malformed file row and a source delete.
- **Failure scenarios:** Missing file, duplicate filename/content, malformed JSON, API timeout/429, foreign-key violation, and source schema field addition.
- **Acceptance criteria:** A developer can reset sources, reproduce the defect set, perform insert/update/delete mutations, and identify all data against S01 contracts.
- **Interview questions:** How do you make integration data repeatable? How do you distinguish a duplicate file from a retry? What are API ingestion checkpoints? Why use synthetic bad data?
- **Documentation requirements:** Document source setup, reset steps, expected defects, and mutation examples as `PLANNED` until actually performed.
- **Estimated hands-on time:** 8–10 hours.
- **Priority:** MUST HAVE.

## Epic MDEP-E02 — Reliable Batch Lakehouse Path

### Story MDEP-S03 — Orchestrate idempotent batch ingestion into S3 Bronze

- **Context:** Batch sources require one repeatable, observable ingestion path before Spark can build trusted data.
- **Goal:** Manually implement an Airflow DAG that extracts PostgreSQL snapshots, REST pages, and files into source-aligned S3 Bronze Parquet with ingestion metadata and quarantine handling.
- **Why this matters in Data Engineering:** Orchestration turns one-off scripts into replayable, scheduled, and recoverable data workflows.
- **Learning objectives:** Practice DAG/task boundaries, logical dates, retries, idempotent writes, backfills, partitions, and extraction checkpoints.
- **Concepts to understand before implementation:** Airflow scheduling versus execution date, retry policy, task idempotency, atomic publish pattern, Parquet, S3 prefixes, pagination state, full versus incremental load.
- **Architecture context:** Implements `PostgreSQL / REST API / Files → Airflow → S3 Bronze / Parquet`; it does not publish CDC-owned current state to Silver.
- **Implementation scope:** One compact DAG and small task units for all three source types; deterministic paths, run metadata, and quarantine records. PostgreSQL batch is snapshot/backfill learning input, while CDC remains canonical for identified CDC tables.
- **Sub-tasks:**
  - Define Bronze and quarantine path/naming conventions.
  - Implement PostgreSQL snapshot and incremental extraction exercises.
  - Implement REST pagination/retry and file discovery/identity checks.
  - Write valid records as Parquet and rejected records with reasons.
  - Create the Airflow DAG, retry policy, and backfill parameters.
- **Hands-on validation:** Manually run a DAG; rerun one interval; fail/retry a task; backfill a historical date; inspect Parquet schema and partitions; prove no duplicate canonical Bronze publication.
- **Failure scenarios:** API failure mid-page, duplicate file, missing file, partial write, rerun same interval, source record retry, and malformed type.
- **Acceptance criteria:** All three sources land traceable Bronze data; a rerun is idempotent; retries/backfill behave as documented; invalid data is isolated; the developer can explain logical-date behavior.
- **Interview questions:** How do you make an Airflow task idempotent? What is a backfill? How would you resume paginated API extraction? What metadata makes Bronze auditable?
- **Documentation requirements:** Document DAG purpose, dependencies, run parameters, Bronze layout, and actual validation result/status.
- **Estimated hands-on time:** 12–16 hours.
- **Priority:** MUST HAVE.

### Story MDEP-S04 — Transform batch Bronze data to trusted Silver Iceberg with PySpark

- **Context:** Bronze is intentionally imperfect; analytics cannot consume it directly.
- **Goal:** Build a PySpark transformation that validates, deduplicates, normalizes, enriches, and incrementally writes batch-owned Silver tables in Iceberg.
- **Why this matters in Data Engineering:** This is the practical bridge between raw ingestion and reliable, queryable data.
- **Learning objectives:** Inspect Spark execution, schema, partitions, shuffles, skew, joins, incremental processing, Iceberg snapshots, and bad-data quarantine.
- **Concepts to understand before implementation:** Spark DataFrame plan, partition versus file, shuffle, repartition, skew, watermark distinction, Iceberg snapshots/merge, schema evolution, null policy.
- **Architecture context:** Implements `S3 Bronze / Parquet → PySpark → Silver / S3 + Iceberg` for batch-owned reference/API/file datasets.
- **Implementation scope:** One focused job family and one or two representative Silver tables; use the shared S3 HadoopCatalog. Include a controlled schema evolution and an incremental rerun. Avoid duplicating Flink CDC application logic.
- **Sub-tasks:**
  - Configure the shared Iceberg catalog and table namespaces.
  - Read Bronze Parquet and apply contract validation/type normalization.
  - Deduplicate and quarantine invalid rows with reasons.
  - Join/enrich a representative dataset and write Silver Iceberg.
  - Perform incremental rerun, schema evolution, and a small skew/shuffle exercise.
- **Hands-on validation:** Inspect DataFrame schema/partitions; trigger and inspect a shuffle; change repartitioning; create a skewed key; rerun processing; inspect Iceberg snapshots and valid/quarantine counts.
- **Failure scenarios:** Duplicate records, null keys, bad types, evolving field, small-file/partition mistake, skew, and rerun after partial failure.
- **Acceptance criteria:** Silver data conforms to S01; every rejected record is traceable; reruns do not duplicate logical records; a schema evolution path is demonstrated; Spark observations are recorded.
- **Interview questions:** Why Parquet? When does Spark shuffle? How do you address skew? What does Iceberg add beyond files? How do you make incremental processing idempotent?
- **Documentation requirements:** Document transformation rules, table schema, write semantics, observed Spark behavior, and actual validation evidence.
- **Estimated hands-on time:** 12–16 hours.
- **Priority:** MUST HAVE.

## Epic MDEP-E03 — CDC and Streaming Lakehouse Path

### Story MDEP-S05 — Capture PostgreSQL changes with Debezium and Kafka

- **Context:** CDC is a core V1 capability and must show how source mutations become replayable change events.
- **Goal:** Configure PostgreSQL logical replication, Debezium, and Kafka so a selected source table produces observable insert, update, and delete events.
- **Why this matters in Data Engineering:** CDC enables low-latency change propagation while preserving mutation semantics that periodic extracts can hide.
- **Learning objectives:** Understand WAL, initial snapshot, connector offsets, before/after state, operation types, partition keys, ordering, consumer groups, replay, and schema evolution.
- **Concepts to understand before implementation:** PostgreSQL logical decoding, Debezium envelope, Kafka topic/partition/offset, tombstone versus delete event, at-least-once delivery, key-based ordering.
- **Architecture context:** Implements `PostgreSQL → Debezium → Kafka`; this is the prerequisite for Flink’s CDC current-state application.
- **Implementation scope:** One or two carefully chosen PostgreSQL tables and their topics. Configure initial snapshot and changes; record topic/key conventions and retention/replay decision. Do not introduce a schema registry unless required by a later justified decision.
- **Sub-tasks:**
  - Enable and validate PostgreSQL logical replication prerequisites.
  - Configure Debezium connector and snapshot behavior.
  - Create/verify Kafka topics, key strategy, partitions, and retention.
  - Perform source insert/update/delete and inspect envelopes/offsets.
  - Replay with a separate consumer group and record current-state reconstruction expectations.
- **Hands-on validation:** Insert, update, and delete a record; inspect corresponding events; compare snapshot and stream records; replay from earliest with a new group; introduce a duplicate delivery and explain handling.
- **Failure scenarios:** Connector restart, invalid connector configuration, duplicate event delivery, delete/tombstone confusion, out-of-order keys across partitions, and source schema addition.
- **Acceptance criteria:** The developer can show source mutations and corresponding Kafka events; initial snapshot is understood; a consumer can replay; topic/key choices are documented; no claims of global ordering are made.
- **Interview questions:** CDC versus polling? What is stored in the WAL? How does Debezium represent deletes? What ordering does Kafka guarantee? How would you recover a consumer?
- **Documentation requirements:** Document connector assumptions, topic contracts, sample event semantics, replay process, and actual validation results.
- **Estimated hands-on time:** 10–14 hours.
- **Priority:** MUST HAVE.

### Story MDEP-S06 — Process Kafka events and CDC with Flink into Bronze and Silver Iceberg

- **Context:** Kafka retains records but does not produce trusted event-time tables or current state on its own.
- **Goal:** Use Flink to archive raw Kafka records to Bronze and build one event-time table plus one CDC-derived current-state Silver Iceberg table with recovery semantics.
- **Why this matters in Data Engineering:** Streaming systems require explicit decisions about time, state, duplicates, late data, and failure recovery.
- **Learning objectives:** Practice event time, watermarks, windows, state, checkpoints, backpressure, replay, CDC upsert/delete application, and Iceberg streaming sinks.
- **Concepts to understand before implementation:** Processing versus event time, watermark, allowed lateness, keyed state, checkpoint, checkpoint-to-sink consistency, upsert, tombstone/delete, backpressure.
- **Architecture context:** Implements `Kafka → Flink → Bronze archive + Silver Iceberg`; CDC tables are canonical current state here, while event topics become append/event Silver tables.
- **Implementation scope:** One `order` or `payment` event-time stream and one CDC table. Set a modest checkpoint strategy; use the shared S3 HadoopCatalog; define late/bad event handling. No second streaming engine or duplicate Spark CDC process.
- **Sub-tasks:**
  - Configure Kafka source offsets, event parsing, and raw Bronze archive.
  - Define event time, watermark, and a representative window/aggregation.
  - Apply CDC upserts/deletes to a keyed current-state Iceberg table.
  - Configure checkpoint location/restart behavior and sink commit semantics.
  - Route invalid/late-beyond-policy records to a documented disposition.
- **Hands-on validation:** Process normal events; produce late, duplicate, and out-of-order events; restart the job; validate checkpoint recovery; create processing lag/backpressure; compare current state against PostgreSQL.
- **Failure scenarios:** Checkpoint failure, job restart, duplicate event, late/out-of-order event, malformed Kafka payload, source delete, lag/backpressure, and Iceberg commit conflict.
- **Acceptance criteria:** Raw stream records are replayable; event-time behavior matches policy; CDC state correctly reflects insert/update/delete; restart resumes according to configured offsets/checkpoints; failure results are documented.
- **Interview questions:** What is a watermark? How do checkpoints differ from Kafka offsets? How do you apply CDC deletes? What does exactly-once mean at a sink? How do you diagnose backpressure?
- **Documentation requirements:** Document topology, state/checkpoint design, watermark/late-data policy, sink semantics, and measured validation outcomes.
- **Estimated hands-on time:** 14–18 hours.
- **Priority:** MUST HAVE.

## Epic MDEP-E04 — Analytics Warehouse and Gold Dimensional Models

### Story MDEP-S07 — Expose Silver Iceberg in Snowflake and build tested dbt Gold marts

- **Context:** Trusted Silver datasets need a consumer-oriented warehouse layer that demonstrates ELT and dimensional modeling.
- **Goal:** Configure Snowflake access to the S3/Iceberg Silver data and build a compact dbt project with dimensions, facts, one mart, tests, and a bounded SCD exercise.
- **Why this matters in Data Engineering:** Warehouse modeling connects reliable pipeline outputs to understandable business analytics.
- **Learning objectives:** Practice external Iceberg access, ELT boundaries, dbt sources/ref/tests, surrogate keys, star schema, incremental models, snapshots, and lineage.
- **Concepts to understand before implementation:** External volume, Iceberg metadata refresh, dbt DAG, source/staging/intermediate/mart layers, natural/surrogate key, SCD Type 1/2, fact grain, referential integrity.
- **Architecture context:** Implements `Silver Iceberg → Snowflake → dbt → Gold`. Snowflake/dbt owns Gold only; it does not write back into externally managed Silver tables.
- **Implementation scope:** Create Snowflake external access/configuration, register representative Silver Iceberg tables, and build `dim_customer`, `dim_product`, `dim_date`, `fct_orders`, and `mart_daily_sales`. Use tests and one SCD Type 2 demonstration via snapshot or one bounded dimension. Keep the number of marts small.
- **Sub-tasks:**
  - Configure external volume/object-store catalog integration and verify read access.
  - Define dbt sources and staging models over Silver.
  - Document grains and create dimensions/fact with surrogate keys.
  - Build one incremental fact/mart and one bounded SCD Type 2/snapshot example.
  - Add dbt tests and intentionally fail then fix a test.
- **Hands-on validation:** Build dbt sources/staging with `ref()`; run tests; intentionally fail a uniqueness or relationship test; run incremental model twice; full-refresh; inspect lineage; reconcile fact count/value with Silver.
- **Failure scenarios:** Stale Iceberg metadata, inaccessible S3 path, broken relationship, duplicate business key, late-arriving dimension, schema change, failed incremental run, and dbt test failure.
- **Acceptance criteria:** Snowflake can query representative Silver data; Gold models have documented grain; dbt tests cover required quality checks; one incremental and one SCD behavior are proven; Gold reconciles to Silver for a stated run.
- **Interview questions:** ETL versus ELT? What is the grain of a fact? Why surrogate keys? How do dbt tests differ from runtime validation? How do late-arriving dimensions affect facts? What is an external Iceberg table trade-off?
- **Documentation requirements:** Document Snowflake access boundary, dbt lineage/model descriptions, grains, tests, SCD choice, reconciliation, and actual result status.
- **Estimated hands-on time:** 12–16 hours.
- **Priority:** MUST HAVE.

## Epic MDEP-E05 — Reliability Demonstration and Portfolio Evidence

### Story MDEP-S08 — Execute end-to-end failure, recovery, and interview-readiness lab

- **Context:** The charter defines completion as understanding and validating system behavior, not merely having code.
- **Goal:** Run the coherent V1 path, intentionally break representative batch and streaming cases, recover, validate outcomes, and record evidence grounded in the repository’s actual state.
- **Why this matters in Data Engineering:** Data engineers are evaluated on diagnosis, recovery, and communication as much as normal-path delivery.
- **Learning objectives:** Practice detect → diagnose → handle → recover → validate; distinguish replay from backfill; reconcile layers; explain trade-offs; produce portfolio-quality evidence.
- **Concepts to understand before implementation:** Failure domain, retry versus replay, checkpoint recovery, idempotency, quarantine, freshness/completeness, reconciliation, incident timeline, runbook.
- **Architecture context:** Exercises the complete V1 architecture without adding a separate platform: sources, Airflow/Spark batch, Debezium/Kafka/Flink streaming, Iceberg, Snowflake, and dbt.
- **Implementation scope:** Execute a small failure matrix, recovery steps, and reconciliation for the completed V1. Produce factual runbook/architecture/data-flow documentation and an interview Q&A based on observed behavior. No broad production observability build.
- **Sub-tasks:**
  - Define expected layer counts/checks for one end-to-end run.
  - Run duplicate/bad-type/missing-file/API-retry batch cases.
  - Run source update/delete, duplicate, late, out-of-order, restart, and replay stream cases.
  - Backfill/rerun Airflow and validate idempotency.
  - Reconcile Silver/Gold; update actual-state docs and interview notes.
- **Hands-on validation:** For each selected failure, record detection signal, diagnosis, handling, recovery action, and validation query/check. Demonstrate one batch retry/backfill and one Flink restart/replay.
- **Failure scenarios:** Duplicate records, nulls, malformed data, schema change, late/out-of-order event, duplicate/missing file, API failure, source update/delete, partial batch load, pipeline restart/failure.
- **Acceptance criteria:** Required V1 failures are exercised or explicitly recorded as unimplemented; recoveries preserve stated data semantics; Bronze/Silver/Gold reconciliation is captured; documentation distinguishes `PLANNED`, `IMPLEMENTED`, and `VALIDATED`.
- **Interview questions:** Walk through a failed pipeline incident. How do you know a backfill is safe? How do you reconcile CDC state? When would you quarantine versus reject? How do you explain exactly-once limitations?
- **Documentation requirements:** Update README and completed docs only from observed state; include a runbook, failure-recovery table, architecture/data-flow, data model, and concise interview Q&A. Never represent the planning backlog as implementation evidence.
- **Estimated hands-on time:** 8–12 hours.
- **Priority:** MUST HAVE.

## Backlog size and priority rationale

This is intentionally an eight-story, five-epic backlog. All Stories are MUST HAVE because each directly supports a required V1 capability. Additional marts, extra CDC entities, automation polish, a second API, and CI enhancements are candidates for future SHOULD HAVE/OPTIONAL follow-up only after this coherent V1 has been manually implemented and validated.
