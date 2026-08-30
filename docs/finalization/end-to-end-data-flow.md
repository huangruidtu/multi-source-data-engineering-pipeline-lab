# End-to-End Data Flow

This guide follows concrete records through the implemented code. Runtime stages
are marked as designed/configured when physical service execution is deferred.

## Case A — `order_id=1001` through the CDC path

| Step | Implemented path and metadata | Correctness / boundary |
| --- | --- | --- |
| Source row | `commerce.orders` in `source-data/postgres/schema.sql`; PK `order_id`, FK `customer_id`, `updated_at` | The schema has no `location_id`; no order-location join is invented. |
| WAL/publication | `source-data/postgres/cdc-init.sql` publishes `commerce.orders` | Logical replication is configured; live WAL/slot evidence is V1.x deferred. |
| Debezium | `ingestion/cdc/debezium-postgres-connector.json` emits `mdep.commerce.orders` using pgoutput | Envelope is `r/c/u/d`; deletes require `after=null`; tombstones are enabled. |
| Kafka | Topic/key contract in `ingestion/cdc/contracts.py` | A Kafka offset is transport position, not source freshness. |
| Flink parse | `parse_debezium` in `processing/flink/cdc_model.py` derives entity, key, operation, LSN, transaction and optional transport fields | Unsupported entities, invalid LSN, or invalid deletes are rejected. |
| Keyed state | `key_identity` gives `orders:1001`; `CdcStateApplier` in `flink_cdc_job.py` stores last version/current row | State is per entity + primary key, avoiding same-text-ID collisions. |
| Version decision | `version_decision` applies LSN, then same-transaction `total_order`, then known identical transport replay | Lower LSN is ignored; unresolved equal position is conservatively a conflict. |
| CDC Bronze | `_bronze_values` and `_add_file_sink_definitions` archive event evidence under the configured CDC Bronze layout | Malformed messages retain unparsed evidence; runtime sink writing is deferred. |
| Silver | `_submit_writes` inserts changelog rows into `mdep.silver.core_orders` | Flink is the sole CDC-commerce Silver writer. |
| Snowflake | `warehouse/snowflake/01_setup.sql` declares external `CORE_ORDERS` Iceberg access | External metadata access is designed, not runtime-proven. |
| dbt staging | `analytics/dbt/models/staging/stg_orders.sql` selects and types Silver order fields | Grain remains one current Silver order. |
| Enrichment | `int_orders_enriched.sql` joins order date/currency to DKK exchange rates | Missing FX becomes `missing_dkk_rate=true`, never a silent conversion. |
| Gold fact | `fct_orders.sql` merges by `order_id`, includes a delete post-hook, and joins customer/date dimensions | Grain is one current order; orders do not join locations. |
| Mart | `mart_daily_sales.sql` groups order date + source currency | It exposes count, gross/converted sales, and missing-rate count. |

### Mutation examples

An update for `order_id=1001` with a larger source LSN becomes current state.
An older replay with a lower LSN is rejected even if delivered later. Two source
changes in the same transaction can share an LSN, so `transaction.total_order`
resolves them when metadata is available. A source delete emits a delete envelope;
the current state is cleared, while Bronze preserves the event and dbt's
`fct_orders` post-hook removes a Gold fact no longer present upstream.

## Case B — exchange rate through the batch path

| Step | Implemented path and metadata | Correctness / boundary |
| --- | --- | --- |
| REST source | `source-data/rest-api/app.py`; batch entry `land_rest` in `ingestion/batch/pipeline.py` | `fetch_paginated_json` reads all pages before publication and retries retryable errors. |
| Airflow boundary | `orchestration/dags/bronze_ingestion.py` invokes the batch task family | Airflow schedules/retries/backfills; it does not own Silver transformation. |
| Bronze envelope | `enrich_record` in `bronze.py` adds ingestion ID, source locator/extract time, ingestion time, and hash | Deterministic `bronze_key` makes a logical rerun converge on one canonical object. |
| Spark validation | `validate_exchange_rates` in `processing/spark/silver_batch.py` validates date, currencies, positive rate and timestamp | Invalid records go to `quarantine/silver`, preserving a reason/payload. |
| In-batch winner | `split_valid_and_quarantine` ranks business key by `retrieved_at`, extract time, ingestion time, then hash | One valid winner per `(rate_date, base_currency, quote_currency)`. |
| Existing Silver | `merge_iceberg` applies the same lexicographic predicate | An older replay with a different hash cannot regress Silver. |
| Gold enrichment | `int_orders_enriched.sql` joins an order's date/currency to a DKK exchange rate | The batch-owned `ref_exchange_rates` is consumed as reference data, not a CDC state table. |

## Case C — location reference path

`land_rest` and `land_files` can publish reference inputs. File identity is the
SHA-256 of bytes (`file_identity`); duplicate file content is quarantined.
`validate_locations` accepts only known country references and derives `region`.
The current location version order is `updated_at`, extraction time, ingestion
time, then hash. `dim_locations.sql` is the Gold destination. A location has a
natural `location_id`; it is not added to orders because the source schema does
not provide that relationship.

## What has and has not been evidenced

Parsing, ownership, version decisions, validation, and model declarations are
implemented and covered by offline tests. The rows above are an executable design
trace, not records captured from a running Kafka, Flink, Spark, Iceberg, or
Snowflake environment. See [Offline Validation Coverage](offline-validation-coverage.md).
