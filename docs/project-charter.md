# Project Charter
## Multi-Source Data Engineering Pipeline Lab

**Status:** PLANNING  
**Repository:** `huangruidtu/multi-source-data-engineering-pipeline-lab`  
**Jira Project:** `Multi-Source Data Engineering Pipeline Lab`  
**Jira Key:** `MDEP`

> **V1 scope amendment — 2026-08-30:** Sections written before this amendment
> described hands-on physical runtime validation as mandatory for important
> Stories. That was the original learning intent and is retained as project
> history. For the final MDEP V1 scope, full infrastructure runtime integration
> is deferred to a separate hands-on lab / V1.x validation phase. The canonical
> V1 completion model is **IMPLEMENTED**, **OFFLINE / STATICALLY VALIDATED**,
> **DOCUMENTED / DESIGNED**, and **RUNTIME DEFERRED**. This is a formal scope
> decision, not a claim that unexecuted infrastructure has passed.

---

## 1. Project Purpose

This repository is a hands-on learning and portfolio project for Data Engineer and Data Platform Engineer job preparation.

The objective is **not** to create the largest possible platform or demonstrate as many technologies as possible.

The objective is to build **one coherent, mainstream, end-to-end Data Engineering implementation** that teaches reusable Data Engineering concepts through real hands-on implementation.

The project should help prepare for Data Engineer and Data Platform Engineer interviews across different industries, including:

- financial services
- pharmaceutical
- logistics
- telecom
- EV / energy
- SaaS
- ecommerce

The architecture should therefore remain broadly domain-neutral and focus on transferable Data Engineering patterns.

---

## 2. Learning Philosophy

This is primarily a **hands-on learning project**.

AI and Codex should assist with:

- architecture planning
- Jira backlog planning
- code skeletons and examples
- reviews
- troubleshooting
- documentation
- architecture diagrams

However, AI must **not** autonomously implement the entire project.

The human developer will manually:

- perform implementation steps
- run commands
- inspect data
- trigger failure scenarios
- debug problems
- validate system behavior
- review the resulting code

A Story is **not complete simply because working code exists**.  
The developer must understand the behavior of the implemented system.

---

## 3. Existing Skills

The developer already has practical experience with:

- AWS
- Kubernetes
- Terraform
- Docker
- Helm
- Argo CD
- GitHub Actions
- Prometheus
- Grafana
- Linux
- production troubleshooting
- observability
- Kafka fundamentals

Therefore:

> Do **not** turn this project primarily into a Kubernetes, Terraform, GitOps, or observability project.

Those technologies may be used where useful, but they are not the primary learning objectives.

---

## 4. Primary Learning Areas

The project must primarily develop hands-on experience with:

- SQL
- Python
- data ingestion
- ETL
- ELT
- CDC
- batch processing
- streaming processing
- Data Lake
- Lakehouse
- Data Warehouse
- Apache Spark
- Apache Kafka
- Apache Flink
- Apache Iceberg
- Snowflake
- dbt
- Apache Airflow
- dimensional modeling
- data quality
- incremental processing
- schema evolution
- idempotency
- retry
- replay
- backfill
- late-arriving data
- data reliability

---

## 5. Business Domain

Use a generic **Commerce & Operations** domain.

The project must **not** be tightly coupled to banking, pharmaceutical, telecom, logistics, or EV charging.

The purpose of the business domain is only to provide realistic Data Engineering problems.

### Master Data

- customers
- products
- locations
- devices

### Transactional Data

- orders
- payments

### Event Data

- order events
- payment events
- device events

### Reference / External Data

- exchange rates
- location metadata
- reference datasets

---

## 6. Source Systems

The project must demonstrate multiple realistic source patterns.

### 6.1 PostgreSQL

Example entities:

- customers
- products
- orders
- payments

Learning objectives:

- relational data
- primary keys
- foreign keys
- SQL
- full and incremental extraction
- source updates and deletes
- CDC

### 6.2 REST API

Example datasets:

- exchange rates
- location/reference metadata

Learning objectives:

- JSON
- pagination
- retries
- rate limiting
- incremental ingestion
- API failures

### 6.3 Files

Use CSV and JSON reference datasets.

Learning objectives:

- file ingestion
- schema validation
- duplicate files
- missing files
- malformed records
- late-arriving files

### 6.4 Kafka

Example events:

- `order.created`
- `order.completed`
- `payment.authorized`
- `payment.completed`
- `payment.failed`
- `device.status_changed`

Learning objectives:

- event streaming
- partitions
- ordering
- offsets
- replay
- consumer groups
- schema evolution

---

## 7. CDC

CDC is a **core V1 capability**.

Target flow:

```text
PostgreSQL
    ↓
Debezium
    ↓
Kafka
    ↓
Downstream Processing
```

The implementation must demonstrate how `INSERT`, `UPDATE`, and `DELETE` operations become change events.

CDC learning must include:

- PostgreSQL WAL / database change-log concepts
- initial snapshot
- change events
- before/after state
- operation type
- ordering
- duplicate handling
- schema evolution
- reconstruction of current state
- CDC versus periodic batch extraction

---

## 8. Medallion Architecture

**Bronze → Silver → Gold** is a core architectural principle and must be implemented rather than merely documented.

### 8.1 Bronze

Purpose:

- raw or minimally transformed data
- source-aligned
- immutable / append-oriented where practical
- replayable
- auditable

Bronze may intentionally contain:

- duplicates
- null values
- malformed records
- old schemas
- late data

### 8.2 Silver

Purpose:

Create technically trustworthy datasets.

Silver processing should include:

- schema validation
- type casting
- deduplication
- null handling
- normalization
- enrichment
- joins
- conformance
- incremental processing
- merge / upsert
- CDC application
- late-arriving data
- schema evolution
- bad-data handling

### 8.3 Gold

Purpose:

Create business-oriented and consumer-ready datasets.

Gold should demonstrate dimensional modeling.

Example dimensions:

- `dim_customer`
- `dim_product`
- `dim_location`
- `dim_date`

Example facts:

- `fct_orders`
- `fct_payments`
- `fct_device_events`

Example marts:

- `mart_daily_sales`
- `mart_customer_activity`
- `mart_payment_performance`
- `mart_operational_activity`

Learning objectives:

- grain
- fact tables
- dimensions
- star schema
- natural keys
- surrogate keys
- SCD Type 1
- SCD Type 2
- late-arriving dimensions
- incremental models
- snapshots

---

## 9. V1 Technology Stack

### Programming

- Python
- SQL

### Operational Source

- PostgreSQL

### CDC

- Debezium

### Streaming

- Apache Kafka
- Apache Flink

### Object Storage

- AWS S3

### Data Format

- Parquet

### Open Table Format

- Apache Iceberg

### Batch Processing

- PySpark

### Warehouse

- Snowflake

### Transformation

- dbt

### Orchestration

- Apache Airflow

### Source Control

- GitHub

### CI

- lightweight GitHub Actions where useful

---

## 10. V1 High-Level Architecture

```text
                               DATA SOURCES
                    ┌──────────────┼──────────────┐
                    │              │              │
               PostgreSQL       REST API       Files
                    │              │           CSV/JSON
                    │
                 Debezium
                    │
                    ▼
                  Kafka ◄──────── Event Generator
                    │
             ┌──────┴───────────────┐
             │                      │
          BATCH PATH           STREAMING PATH
             │                      │
          Airflow                  Flink
             │                      │
             ▼                      │
         S3 BRONZE                  │
       Raw / Parquet                │
             │                      │
             ▼                      │
          PySpark                   │
             │                      │
             └──────────┬───────────┘
                        ▼
                     SILVER
                  S3 + Iceberg
                        │
                        ▼
                    Snowflake
                        │
                       dbt
                        │
                        ▼
                      GOLD
                        │
               Fact / Dimension
                  Data Marts
                        │
                        ▼
                    Analytics
```

---

## 11. Batch Data Flow

Target architecture:

```text
PostgreSQL / REST API / Files
            ↓
         Airflow
            ↓
        S3 Bronze
            ↓
         PySpark
            ↓
     Silver / Iceberg
            ↓
        Snowflake
            ↓
           dbt
            ↓
          Gold
            ↓
        Analytics
```

The batch implementation must teach:

- full loads
- incremental loads
- partitioning
- Parquet
- Spark transformations
- joins
- shuffle
- skew
- idempotency
- retries
- backfills
- warehouse loading
- ETL versus ELT

---

## 12. Streaming Data Flow

Target architecture:

```text
Event Generator
      ↓
    Kafka
      ↓
    Flink
      ↓
Bronze / Silver
      ↓
   Iceberg
```

CDC also feeds Kafka:

```text
PostgreSQL
    ↓
Debezium
    ↓
Kafka
```

Streaming learning objectives:

- processing time
- event time
- watermarks
- windows
- state
- checkpoints
- duplicate events
- late events
- out-of-order events
- backpressure
- replay
- failure recovery
- at-least-once versus exactly-once concepts

---

## 13. Data Quality

Data quality is mandatory.

Implement and validate:

- not-null checks
- uniqueness
- accepted values
- referential integrity
- freshness
- completeness
- schema validation
- duplicate detection
- bad-data quarantine

Use dbt tests where appropriate.

Synthetic source data must intentionally contain bad data.

---

## 14. Failure Lab

The project must intentionally create failure scenarios.

Examples:

- duplicate records
- null values
- bad types
- malformed records
- schema changes
- late events
- out-of-order events
- duplicate files
- missing files
- API failures
- source updates
- source deletes
- partial batch loads
- pipeline failures

Important failures must follow:

```text
Detect
  ↓
Diagnose
  ↓
Handle
  ↓
Recover
  ↓
Validate
```

---

## 15. Hands-On Validation Rule

### Historical rule superseded for final V1

The runtime exercises below remain valuable learning-lab recipes and must not
be rewritten as if they happened. They are no longer final V1 completion
criteria. Physical execution of Airflow, Spark, Flink, Kafka, PostgreSQL CDC,
S3, Snowflake, and dbt is **RUNTIME DEFERRED**. A future V1.x hands-on phase
may execute them and retain evidence.

Every Jira Story that introduces an important technology or concept must include a **HANDS-ON VALIDATION** section.

### Spark

- inspect DataFrame schema
- inspect partitions
- trigger shuffle
- test repartition
- create skew
- deduplicate data
- rerun processing

### dbt

- define source
- build staging model
- use `ref()`
- add tests
- intentionally fail a test
- implement incremental model
- perform full refresh
- inspect lineage

### Airflow

- manually execute DAG
- intentionally fail a task
- retry task
- backfill historical data
- rerun the same interval
- verify idempotency

### Flink

- process normal events
- create late event
- create duplicate event
- create out-of-order event
- restart job
- validate checkpoint recovery
- create processing lag / backpressure

### CDC

- INSERT source record
- UPDATE source record
- DELETE source record
- inspect corresponding Debezium events
- rebuild downstream state
- validate consistency

---

## 16. Jira Story Definition

Every Jira Story must contain:

1. Context
2. Goal
3. Why this matters in Data Engineering
4. Learning objectives
5. Concepts to understand before implementation
6. Architecture context
7. Implementation scope
8. Sub-tasks
9. Hands-on validation
10. Failure scenarios
11. Acceptance criteria
12. Interview questions
13. Documentation requirements

---

## 17. Project Workflow

For every Story:

```text
Jira Story
    ↓
Study Concepts
    ↓
Review Architecture
    ↓
Codex Provides Guidance / Skeletons
    ↓
HUMAN Manually Implements
    ↓
Run
    ↓
Break Intentionally
    ↓
Debug
    ↓
Validate
    ↓
Review Actual Code
    ↓
Generate Documentation
    ↓
Extract Interview Q&A
```

Do not skip the manual implementation stage.

---

## 18. Documentation Principle

Codex should produce high-quality documentation.

However, documentation describing implementation must be based on the **actual repository state**.

Never document planned functionality as if it were implemented.

Use explicit states where useful:

- `PLANNED`
- `IMPLEMENTED`
- `VALIDATED`

For final V1, use the following truth model consistently:

- **IMPLEMENTED** — versioned code or configuration exists.
- **OFFLINE / STATICALLY VALIDATED** — unit, fixture, contract, deterministic,
  reconciliation, or source-inspection evidence exists without requiring a
  physical infrastructure deployment.
- **DOCUMENTED / DESIGNED** — an architecture decision, prerequisite, failure
  behavior, or production improvement is specified but not executed.
- **RUNTIME DEFERRED** — physical integration is intentionally outside V1
  acceptance and must never be described as runtime-validated.

### Final V1 scope amendment and acceptance model

### Why this changed

MDEP V1 is optimized for interview preparation and implementation depth rather
than a local production-like deployment. The project can demonstrate data
engineering judgment more honestly through complete code, deterministic offline
tests, reconciliation logic, failure reasoning, documentation, and interview
readiness than through an incomplete local infrastructure demonstration.

### What changed

Full infrastructure runtime integration moved from mandatory V1 acceptance to
**RUNTIME DEFERRED**. This includes physical Airflow/Spark/Flink/Kafka/
PostgreSQL CDC/S3/Snowflake/dbt execution and full end-to-end validation.

### What did not change

Architecture correctness, implementation and configuration quality, offline
validation, reconciliation logic, failure/trade-off reasoning, documentation,
and interview readiness remain mandatory V1 criteria. The target architecture,
technology scope, canonical-writer rules, and prohibition on unsupported tool
sprawl do not change.

Expected documentation eventually includes:

```text
README.md

docs/
├── project-charter.md
├── architecture.md
├── data-flow.md
├── data-model.md
├── medallion-architecture.md
├── batch-processing.md
├── streaming-processing.md
├── cdc.md
├── data-quality.md
├── failure-recovery.md
├── runbook.md
└── decisions/
```

---

## 19. V1 Scope Boundary

Do **not** include the following in V1:

- Databricks
- Delta Lake
- Redshift
- BigQuery
- Paimon
- Fluss
- StarRocks
- Dagster
- Prefect
- Airbyte
- Fivetran

Do not add technologies simply for tool coverage.

V1 optimizes for:

> **CONCEPT COVERAGE**

not:

> **TOOL COVERAGE**

---

## 20. Future Versions

Future versions should preferably **replace parts of the architecture** rather than continuously add technologies.

### V2 — Databricks / Lakehouse Alternative

Potential stack:

- Databricks
- Spark
- Delta Lake
- Medallion Architecture

Goal:

Compare a Databricks-centered lakehouse implementation against V1.

### V3 — AWS-Native Alternative

Potential stack:

- S3
- Glue
- Spark / Glue
- Redshift
- dbt

Goal:

Compare a Snowflake-centric implementation against an AWS-native Data Engineering architecture.

### V4 — Streaming / Streamhouse Alternative

Potential stack:

- Kafka
- Flink
- Paimon
- Fluss
- StarRocks

Goal:

Explore a more specialized real-time / streamhouse architecture.

All versions should solve substantially similar business and data problems so that architectural trade-offs can be compared meaningfully.

---

## 21. Project Governance Rule

Before adding a new technology to V1, ask:

> Does this introduce a new and important Data Engineering concept, or does it merely add another tool for a concept already covered?

If it only adds another tool, defer it to a future alternative implementation.

Examples:

- Redshift when Snowflake already covers the warehouse role → defer.
- Delta Lake when Iceberg already covers open table-format concepts → defer.
- Dagster when Airflow already covers orchestration concepts → defer.
- Late-arriving dimensions when not otherwise covered → include, because this adds an important Data Engineering pattern.

---

## 22. Current Project Status

### Completed Foundation Steps

- [x] GitHub repository created:
  `huangruidtu/multi-source-data-engineering-pipeline-lab`
- [x] Jira project created:
  `Multi-Source Data Engineering Pipeline Lab`
- [x] Jira key confirmed:
  `MDEP`
- [x] Project Charter finalized

### Current Phase

**BACKLOG DESIGN**

The immediate next tasks are:

1. Design the final repository directory structure.
2. Generate Jira Epics.
3. Generate Jira Stories.
4. Generate Jira Sub-tasks.
5. Define dependency ordering.
6. Estimate hands-on time for each Story.
7. Define learning objectives.
8. Define hands-on validation exercises.
9. Define acceptance criteria.
10. Identify MUST HAVE / SHOULD HAVE / OPTIONAL scope.

> **Do not implement the platform yet.**

---

## 23. Immediate Codex Assignment

Codex must now use this Charter as the source of truth.

The next Codex output should contain:

### A. Final V1 Architecture Review

Validate the coherence of V1 without adding unnecessary tools.

### B. Proposed Repository Structure

Design a repository structure suitable for incremental hands-on implementation and future alternative branches.

### C. Jira Backlog

Create:

```text
Epic
  ↓
Story
  ↓
Sub-task
```

For every Story include all requirements defined in Section 16.

### D. Dependency Order

Identify which Stories block or depend on others.

### E. Time Estimate

Estimate **hands-on learning and implementation time**, not AI generation time.

### F. Prioritization

Classify every Story as:

- MUST HAVE
- SHOULD HAVE
- OPTIONAL

### G. Minimum Complete V1

Recommend the smallest coherent set of Stories that still produces a complete end-to-end V1.

### H. No Implementation Yet

Do **not** generate the full implementation in this phase.

The goal of the next phase is to produce a high-quality implementation plan, not to finish the project automatically.
