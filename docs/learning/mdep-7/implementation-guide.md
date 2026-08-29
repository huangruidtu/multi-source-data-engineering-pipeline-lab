# MDEP-7 Implementation Guide — Reproducible Source Systems

## Story goal and actual result

MDEP-7 implements reproducible upstream inputs for later batch and CDC work: a PostgreSQL source schema, deterministic REST reference source, valid and deliberately bad files, and operator scripts. It does not implement Debezium, Kafka, Airflow, Spark, Flink, S3, Iceberg, Snowflake, or dbt.

The implementation is on PR #2 at commit `e59e2ae`; this documentation branch intentionally does not include that application commit. The facts below are based on that branch and its Jira validation record.

## Files created by the Story

```text
docker-compose.yml
source-data/postgres/schema.sql
source-data/postgres/seed.sql
source-data/postgres/examples/{cdc-mutations,foreign-key-violation,add-customer-loyalty-tier}.sql
source-data/rest-api/{Dockerfile,app.py,test_app.py}
source-data/files/{valid,invalid,scenarios}/
scripts/reset-sources.ps1
scripts/validate-sources.ps1
docs/source-systems.md
docs/learning/mdep-7-reproducible-sources.md
```

## Components, configuration, and structures

`docker-compose.yml` defines PostgreSQL 16 and the local REST source. PostgreSQL exposes port 5432, uses database `commerce` and schema `commerce`, and starts with `wal_level=logical`, `max_replication_slots=4`, and `max_wal_senders=4`. The REST source exposes port 8080.

The relational tables are `customers`, `products`, `orders`, and `payments`. They use text primary keys; `orders.customer_id` references customers and `payments.order_id` references orders. Check constraints restrict statuses, non-negative amounts/prices, and uppercase currency. Seed data creates three rows per table and intentionally leaves `cust-300.email` null.

`app.py` is a dependency-free Python `ThreadingHTTPServer`. `paginate()` validates page/page-size input, slices a fixed record list, and returns `items`, `page`, `page_size`, `total_items`, and `next_page`. `SourceHandler.do_GET()` serves `/health`, `/v1/exchange-rates`, and `/v1/locations`, plus deterministic 503, 429, and 3-second delay scenarios.

## Inputs and outputs

| Input | Output / expected behavior |
| --- | --- |
| Compose startup | resettable Postgres source and REST service |
| PostgreSQL seed | three deterministic rows per source table |
| `cdc-mutations.sql` | customer insert, order update, unreferenced-customer delete |
| REST page query | fixed JSON page with `next_page` |
| `scenario=retryable` | 503 plus `Retry-After: 1` |
| `scenario=rate_limit` | 429 plus `Retry-After: 1` |
| `scenario=timeout` | normal response delayed by 3 seconds |
| invalid fixtures | controlled parse/schema/duplicate/missing-file exercises |

## Important commands

```powershell
docker compose up --build --wait
.\scripts\reset-sources.ps1
.\scripts\validate-sources.ps1
docker compose exec postgres psql -U lab -d commerce -c "SELECT * FROM commerce.orders ORDER BY order_id;"
python -m unittest discover -s source-data/rest-api -p test_*.py
docker compose down
```

The reset script intentionally destroys the local Compose volume before rebuilding it. It must only be used for this local lab.

## Boundaries and deferred work

The source mutations are examples for a later Debezium CDC Story, not observed CDC events. Logical replication is configured but not runtime-confirmed on the implementation host. The full validation script is present, but its Compose/PostgreSQL execution was not run because Docker and `psql` were unavailable. REST unit/live checks and fixture inspection were performed.
