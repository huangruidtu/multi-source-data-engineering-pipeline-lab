# Architecture ↔ Implementation Mapping

This is the authoritative final-V1 map between the intended architecture and
the repository. It records implemented behavior, not a claim that the full
multi-service stack has run.

## V1 truth model

| State | Meaning in this repository |
| --- | --- |
| Implemented | Code, SQL, configuration, or contract is version controlled. |
| Offline tested | A unit/contract test can exercise the behavior without infrastructure. |
| Statically checked | Source/configuration is compiled, parsed, or inspected by a test. |
| Runtime deferred | Physical service interaction is a V1.x lab; it has not been evidenced as V1 runtime validation. |

## Ownership and flow

```text
PostgreSQL + REST + CSV/JSON --Airflow/batch--> Bronze Parquet --Spark--> batch-owned Silver Iceberg
PostgreSQL WAL --Debezium--> Kafka --Flink--> CDC Bronze archive + CDC-owned Silver Iceberg
all Silver Iceberg --Snowflake external access--> dbt --> Snowflake-native Gold
```

| Boundary | Implemented owner | Repository evidence | Important rule |
| --- | --- | --- | --- |
| Source contracts | Source-data contracts | `source-data/contracts/commerce-operations.md`, PostgreSQL schema, REST app, fixtures | PostgreSQL owns commerce facts; REST owns reference data; files supply reference fixtures. |
| Batch orchestration | Airflow | `orchestration/dags/bronze_ingestion.py`, `ingestion/batch/` | Airflow orchestrates; it does not transform data into Silver. |
| Batch Bronze | Batch ingestion code | `ingestion/batch/bronze.py`, `pipeline.py` | Immutable Parquet landing plus reasoned quarantine. |
| Batch Silver | Spark | `processing/spark/silver_batch.py`, `contracts.py` | Only `exchange_rates` and `locations` are batch-owned Silver entities. |
| CDC transport | Debezium + Kafka | `ingestion/cdc/debezium-postgres-connector.json`, `contracts.py` | PostgreSQL commerce tables only; Debezium envelope is the change contract. |
| CDC Bronze / Silver | Flink | `processing/flink/flink_cdc_job.py`, `cdc_model.py` | Flink archives raw CDC and derives current state for commerce entities. |
| Analytical Gold | dbt in Snowflake | `analytics/dbt/`, `warehouse/snowflake/01_setup.sql` | dbt owns Gold dimensions, facts, and marts; it does not write Silver. |
| Reconciliation | Validation artifacts | `validation/reconciliation/`, `validation/quality-gates.yml` | Compare bounded sources, Silver, and Gold; templates do not assert live counts. |

## Source and ownership audit

`source-data/postgres/schema.sql` defines `commerce.customers`, `products`,
`orders`, and `payments`. `source-data/postgres/cdc-init.sql` creates the
publication for exactly those tables. The connector JSON uses the same four-table
allow list, so source ownership, publication scope, and CDC scope agree.

The REST service and file fixtures are batch reference sources. Spark refuses
accidental CDC ownership through `BATCH_SILVER_ENTITIES` and
`CDC_OWNED_ENTITIES` in `processing/spark/contracts.py`. This prevents a batch
job from becoming a second current-state writer for commerce tables.

## Batch path audit

The DAG calls batch ingestion code for PostgreSQL, REST, and files, and uses
idempotent run/identity concepts before writing Bronze Parquet. Invalid records
are preserved with a rejection reason instead of silently discarded. The design
keeps Airflow at the scheduling/retry/backfill boundary and keeps business data
logic in `ingestion/batch/` and Spark.

Spark reads batch Bronze and writes the two batch-owned Iceberg entities. Its
canonical Silver key and normalisation rules are pure Python so they can be
tested without Spark. `silver_batch.py` renders the physical Iceberg merge from
the same rules; physical Spark/Iceberg execution is deferred.

### Historical Spark correctness fix

The initial merge condition allowed a different `record_hash` to update an
existing row. That could let an older replay overwrite a newer state. The final
rule is lexicographic, not "hash changed":

* `exchange_rates`: `retrieved_at`, `source_extract_ts`, `ingested_at`, then `record_hash`.
* `locations`: `updated_at`, `source_extract_ts`, `ingested_at`, then `record_hash`.

`incoming_is_newer` compares the complete tuple. A changed hash is only the
last deterministic tie-breaker, never a freshness signal. An exact replay is a
no-op and an older business version loses even if its payload hash differs.

## CDC and Kafka audit

The connector is explicitly `io.debezium.connector.postgresql.PostgresConnector`
with `plugin.name=pgoutput`, an explicit publication/slot, `snapshot.mode=initial`,
`tombstones.on.delete=true`, and `provide.transaction.metadata=true`.
The latter replaced the incorrect `include.transaction` setting. Configuration
means Debezium is expected to enrich events and emit transaction metadata where
its runtime contract supports it; no transaction event is claimed as observed.

Kafka offsets identify transport position in a topic partition. They are not a
cross-partition source-freshness clock. `processing/flink/cdc_model.py` therefore
orders a keyed state transition by PostgreSQL LSN first. For an equal LSN in the
same transaction it uses Debezium `transaction.total_order`; only an identical
known topic/partition/offset is accepted as an exact replay. An equal-position
event without sufficient identity is conservatively rejected as a conflict.

## Flink current-state audit

`flink_cdc_job.py` is a real topology definition, with Kafka input, raw CDC
archive, quarantine side output, keyed current state, Iceberg table DDL, and a
statement set for the sinks. Deletes remove current state and emit a changelog
delete when prior state exists. The raw archive is evidence-first: malformed
payloads can still be retained.

Checkpointing, exactly-once checkpoint mode, restart strategy, and a 60-second
watermark are configured. They are not runtime-validated. In particular, the
PyFlink `KafkaSource` is configured with value-only deserialization, so the job
cannot currently retain Kafka key/partition/offset metadata at runtime. The pure
contract permits such metadata when supplied, but the production topology marks
it `None`; this is a known implementation limitation, not a hidden guarantee.

The watermark supports event-time processing context; it is deliberately not
used to accept or reject CDC current-state changes. LSN/transaction order owns
that decision.

## Iceberg, Snowflake, and dbt audit

Iceberg uses a Hadoop-style catalog/warehouse configuration in both batch and
streaming designs. Spark owns batch reference Silver tables; Flink owns CDC
commerce Silver tables. Snowflake consumes external Iceberg metadata but does
not become a Silver writer.

`warehouse/snowflake/01_setup.sql` creates `MDEP.SILVER_EXT` and `MDEP.GOLD`.
The historical `GOLD_GOLD` schema typo was corrected to `MDEP.GOLD`.
`analytics/dbt` stages external Silver, builds dimensions and facts, and creates
marts. `fct_payments` intentionally uses a full rebuild: it is a bounded V1
choice so deletes and upstream order relinks are represented without implementing
a more complex incremental merge strategy. That is a stated cost/performance
trade-off, not an assertion of production scale.

## Validation and reconciliation audit

`validation/mdep-13-validation-matrix.yml` lists the physical paths as
`BLOCKED` in the old runtime-oriented vocabulary. Under the final V1 Charter
amendment, those records are a V1.x runtime-deferred register, not a failure to
implement V1. `validation/reconciliation/` supplies counts, anti-joins,
duplicate/null checks, and exception recording patterns without fabricated
cross-system totals.

`scripts/validate-mdep-13-e2e.ps1` records a stage as `PASSED` only after a
native command returns zero and an evidence log exists. This fixes the earlier
false-PASS risk where an unsuccessful native command could otherwise be
misreported. The runner's self-test exercises successful, native-failure,
PowerShell-throw, blocked, and not-run states.

## Explicit V1.x runtime-deferred work

Docker/Airflow/PostgreSQL, Spark/Iceberg/S3, Debezium/Kafka, Flink checkpoint
recovery, Snowflake, and dbt physical integrations remain deferred. Their
required environments and commands are retained in
`validation/mdep-13-validation-matrix.yml`; their absence does not turn static
design into runtime evidence.
