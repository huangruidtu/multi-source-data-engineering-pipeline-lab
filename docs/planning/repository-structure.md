# Proposed Repository Structure

**Status:** PLANNED — this is a structure proposal, not an implementation checklist.

## Design principles

- Keep source contracts, ingestion, processing, orchestration, and warehouse models easy to discover.
- Add a directory only when its first Story needs it; do not pre-create empty placeholders.
- Keep runnable logic separate from sample data, documentation, and infrastructure configuration.
- Treat `docs/project-charter.md` as the V1 source of truth. Planning documents never claim that a component is implemented.

## Incremental target layout

```text
.
├── README.md
├── docs/
│   ├── project-charter.md
│   ├── planning/
│   │   ├── repository-structure.md
│   │   ├── jira-backlog.md
│   │   ├── jira-dependencies.md
│   │   └── v1-scope.md
│   └── decisions/                 # add when an architecture decision is made
├── source-data/                   # added with source-system stories
│   ├── contracts/
│   └── files/
├── ingestion/                     # added with batch/CDC ingestion stories
│   ├── batch/
│   └── cdc/
├── orchestration/                  # added with Airflow story
│   └── dags/
├── processing/                     # added with Spark/Flink stories
│   ├── spark/
│   └── flink/
├── warehouse/                      # added with Snowflake/dbt story
│   ├── snowflake/
│   └── dbt/
├── tests/                          # added alongside the first automated checks
├── scripts/                        # small, documented developer commands only
├── infra/                          # minimal local/cloud configuration, only when needed
└── docker-compose.yml              # add only if local service orchestration is chosen
```

## Ownership and contents

| Area | Purpose | Introduced by |
| --- | --- | --- |
| `source-data/contracts/` | Entity/event schemas, source-to-target mapping, and data-quality expectations | MDEP-S01 |
| `source-data/files/` | Versioned, deliberately imperfect CSV/JSON fixtures; never production-like secrets | MDEP-S02 |
| `ingestion/batch/` | Small Python extractors for REST/files/PostgreSQL batch snapshots | MDEP-S03 |
| `ingestion/cdc/` | Debezium connector configuration and CDC event notes | MDEP-S05 |
| `orchestration/dags/` | Airflow DAG definitions that call the batch processing units | MDEP-S03 |
| `processing/spark/` | PySpark Bronze-to-Silver jobs and testable transformation helpers | MDEP-S04 |
| `processing/flink/` | Flink jobs/SQL and event-time, checkpoint, and sink configuration | MDEP-S06 |
| `warehouse/snowflake/` | DDL, roles/access notes, and Iceberg exposure configuration | MDEP-S07 |
| `warehouse/dbt/` | dbt project, sources, staging/intermediate/mart models, tests, and snapshots | MDEP-S07 |
| `tests/` | Focused unit/contract tests and reusable validation fixtures | First introduced by the Story that needs them |
| `docs/decisions/` | Short ADRs for decisions such as table ownership, catalog choice, and replay semantics | When each decision is made |

## Naming and data-layout conventions

Use source and layer names in paths rather than creating a generic `utils/` folder. The intended S3 layout is documented, not created by this planning phase:

```text
s3://<bucket>/bronze/<source>/<entity>/ingest_date=YYYY-MM-DD/...
s3://<bucket>/quarantine/<source>/<entity>/ingest_date=YYYY-MM-DD/...
s3://<bucket>/iceberg/<namespace>/<table>/...
```

Bronze keeps immutable, source-aligned Parquet records and ingestion metadata. Silver Iceberg tables are the technically trusted layer. Snowflake/dbt owns the Gold dimensional models. No duplicate `gold/` lakehouse implementation is planned.

## Deliberately deferred structure

Do not add `kubernetes/`, `terraform/`, Helm charts, GitOps manifests, dashboards, or a broad CI framework merely because the developer has prior experience in those areas. Introduce a small `infra/` or `.github/workflows/` item only when a V1 Story has a concrete operational need.
