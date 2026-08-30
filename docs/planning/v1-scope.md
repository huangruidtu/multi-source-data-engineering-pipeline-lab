# V1 Architecture Review and Minimum Complete Scope

**Status:** PLANNED

## Architecture validation outcome

The charter’s V1 is internally coherent and provides the intended concept coverage: three batch source patterns, PostgreSQL CDC, Kafka/Flink streaming, S3/Parquet Bronze, Spark and Flink transformations, Iceberg Silver, Snowflake/dbt Gold, and Airflow orchestration.

Two boundary rules are necessary for that architecture to remain coherent. They clarify the charter; they do not expand its business scope.

1. **One canonical ownership path per dataset.** PostgreSQL batch extraction demonstrates snapshot/backfill mechanics and lands source-aligned Bronze data. Debezium is the canonical change path for CDC-enabled PostgreSQL tables. A batch job must not independently publish the same current-state table into Silver after CDC starts. This avoids duplicate or conflicting upserts.
2. **A shared Iceberg catalog is required configuration.** Iceberg requires a catalog to atomically track a table’s current metadata. For the smallest V1, use an S3-backed Hadoop catalog shared by Spark and Flink, with deterministic table locations. Snowflake reads the resulting Iceberg metadata through an external volume and an object-storage catalog integration. This uses capabilities already implicit in S3, Iceberg, and Snowflake; it does not introduce Glue, a metastore service, or another data platform.

Snowflake’s documentation confirms that it can query Iceberg metadata files in object storage using an object-store catalog integration and an external volume. Its documentation also explains why a catalog is an architectural requirement. [Snowflake: object-storage catalog integration](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-object-storage) and [Snowflake: Iceberg catalog and external volume](https://docs.snowflake.com/en/user-guide/tables-iceberg) support this boundary. Apache Iceberg documents Hadoop, Glue, REST, and other catalog choices for Spark and Flink; Hadoop is selected here to avoid adding another service. [Apache Iceberg Spark configuration](https://iceberg.apache.org/docs/latest/spark-configuration/) and [Flink configuration](https://iceberg.apache.org/docs/latest/flink-configuration/).

## Explicit scope clarification, not a V1 change

The charter does not name an Iceberg catalog. This document proposes **HadoopCatalog backed by S3** as the V1 implementation choice. It is required configuration, not a new user-facing technology. If the developer instead wants automatic Snowflake discovery, catalog-linked databases, or multi-writer warehouse access, that would require reconsidering an external catalog such as AWS Glue REST; it is not part of this V1 plan.

## End-to-end ownership model

```text
PostgreSQL snapshots ─┐
REST API ─────────────┼─ Airflow → S3 Bronze Parquet → Spark → Silver Iceberg
CSV / JSON files ────┘                                      │
                                                             ├─ Snowflake external Iceberg access → dbt Gold
PostgreSQL WAL → Debezium → Kafka → Flink → Bronze archive ─┘
                                      └─────→ Silver Iceberg (CDC/current state + event tables)
Event generator ─────────────→ Kafka ───────→ Flink
```

### Layer and writer rules

| Layer | Storage/form | Writers | Purpose |
| --- | --- | --- | --- |
| Bronze | S3 / Parquet | Airflow batch ingestion; Flink raw streaming archive | Immutable, replayable, source-aligned records plus metadata |
| Quarantine | S3 / Parquet or JSON evidence | Spark/Flink ingestion paths | Invalid, malformed, or rejected records with reason and source reference |
| Silver | S3 / Iceberg | Spark for batch entities; Flink for CDC/event entities | Validated, deduplicated, conformed, incrementally maintained tables |
| Gold | Snowflake tables/views via dbt | dbt | Dimensional facts, dimensions, and marts for analytics |

The canonical batch Silver candidates are reference and API/file datasets. The canonical streaming Silver candidates are CDC current-state tables and event tables. A deliberate source-to-owner matrix is an acceptance artifact of MDEP-S01.

## Minimum Complete V1

The smallest complete V1 is all eight MUST HAVE Stories in the proposed backlog. Each has a narrow demonstration target:

| Capability | Minimal demonstration |
| --- | --- |
| Multiple source types | PostgreSQL tables, one paginated/retried REST endpoint, and one CSV plus one JSON reference feed |
| Batch + Airflow | One idempotent DAG that lands all three source types and is rerun/backfilled |
| Bronze / S3 / Parquet | Source-aligned Parquet records, ingestion metadata, and at least one quarantined record |
| Spark | One batch Bronze-to-Silver job with schema checks, deduplication, enrichment, and rerun proof |
| CDC / Kafka | Insert, update, delete on one PostgreSQL entity observed as Debezium messages and replayed |
| Flink | One event-time event stream and one CDC state-application flow, with checkpoint recovery proof |
| Iceberg | Shared Silver tables, upsert/current-state semantics, snapshot inspection, and schema evolution exercise |
| Snowflake + dbt | Silver data exposed to Snowflake; `dim_customer`, `dim_product`, `dim_date`, `fct_orders`, and `mart_daily_sales` built and tested |
| Dimensional modeling | Documented grains, natural/surrogate keys, SCD Type 1 plus one bounded SCD Type 2 or dbt snapshot demonstration |
| Data quality + failures | Tests/quarantine plus duplicate, bad type, API failure, late/out-of-order event, and retry/recovery exercises |

## Intentionally out of scope

- Production hardening, high availability, multi-account infrastructure, full CI/CD, Kubernetes deployment, and observability platform work.
- A second way to process the same dataset solely to demonstrate another tool.
- Warehouse write-back into externally managed Silver Iceberg tables.
- The technologies excluded by the charter: Databricks, Delta Lake, Redshift, BigQuery, Paimon, Fluss, StarRocks, Dagster, Prefect, Airbyte, and Fivetran.

## Recommended sequence and effort

The minimum path is MDEP-S01 through MDEP-S08 in dependency order. Estimated hands-on time is **78–106 hours** for a learner who performs the validation and failures manually. This is intentionally not an AI-generation estimate.

## Definition of done for final V1

**Amended 2026-08-30:** the original plan required manual end-to-end execution.
That historical target is now a deferred V1.x hands-on validation phase, not a
V1 blocker. Final V1 is complete when the architecture, implementation,
configuration, deterministic/offline tests, reconciliation logic, failure and
trade-off reasoning, documentation, and interview readiness are complete.

Physical Airflow, Spark/Iceberg, Debezium/Kafka, Flink/S3, Snowflake/dbt, and
cross-system E2E execution are **RUNTIME DEFERRED**. Their absence must remain
visible and must never be represented as runtime validation.
