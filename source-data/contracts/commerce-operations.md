# Commerce & Operations Data Contracts

**Story:** MDEP-6 — Define Commerce & Operations data contracts and ownership

**Status:** IMPLEMENTED as a contract baseline; source systems and pipelines remain unimplemented until their assigned Stories.
**Scope:** V1 identifiers, grains, schemas, quality expectations, and one canonical writer path per logical dataset.

## Contract conventions

- Timestamps are UTC ISO-8601 values. Business dates use `YYYY-MM-DD`.
- IDs ending in `_id` are stable natural/business identifiers unless explicitly described as a warehouse surrogate key.
- Every landed record carries source and ingestion metadata. Raw payloads are preserved in Bronze; validity is determined only in Silver processing.
- Schema changes are additive by default. Removing or changing the meaning/type of an existing field is a breaking change that requires an updated contract and a controlled migration.
- A rejected record is retained with a reason in Quarantine; it is not silently dropped.

## Source-to-layer ownership matrix

This matrix resolves the PostgreSQL batch/CDC overlap: PostgreSQL batch extracts are source-aligned Bronze snapshots/backfill material only. Debezium CDC is the sole canonical writer for current-state PostgreSQL tables in Silver after CDC is active.

| Logical dataset | Authoritative source | Bronze writer | Canonical Silver writer | Canonical Silver dataset | Gold consumer/owner | Key / grain |
| --- | --- | --- | --- | --- | --- | --- |
| customers | PostgreSQL `customers` | Airflow batch snapshot; Flink CDC archive | Flink CDC | `core_customers` | dbt dimension | one current row per `customer_id` |
| products | PostgreSQL `products` | Airflow batch snapshot; Flink CDC archive | Flink CDC | `core_products` | dbt dimension | one current row per `product_id` |
| orders | PostgreSQL `orders` | Airflow batch snapshot; Flink CDC archive | Flink CDC | `core_orders` | dbt fact | one current row per `order_id` |
| payments | PostgreSQL `payments` | Airflow batch snapshot; Flink CDC archive | Flink CDC | `core_payments` | dbt fact | one current row per `payment_id` |
| exchange rates | REST API | Airflow | Spark | `ref_exchange_rates` | dbt enrichment | one rate per `rate_date`, `base_currency`, `quote_currency` |
| locations | REST API reference feed | Airflow | Spark | `ref_locations` | dbt dimension | one current row per `location_id` |
| product categories | CSV reference file | Airflow | Spark | `ref_product_categories` | dbt enrichment | one current row per `category_code` |
| device reference | JSON reference file | Airflow | Spark | `ref_devices` | dbt dimension/enrichment | one current row per `device_id` |
| order events | Kafka `order.*` | Flink raw archive | Flink | `evt_orders` | dbt operational mart | one row per `event_id` |
| payment events | Kafka `payment.*` | Flink raw archive | Flink | `evt_payments` | dbt operational mart | one row per `event_id` |
| device events | Kafka `device.status_changed` | Flink raw archive | Flink | `evt_device_status` | dbt operational mart | one row per `event_id` |

**Layer ownership:** Bronze is immutable/source-aligned and replayable; Silver is technically trusted; Gold is owned by Snowflake/dbt. Spark does not apply CDC current state, and Flink does not create a duplicate Gold layer.

## Entity data dictionary and quality expectations

| Entity | Required business fields | Relationship / grain | Silver quality rules |
| --- | --- | --- | --- |
| customer | `customer_id`, `customer_name`, `email`, `customer_status`, `created_at`, `updated_at` | one row per customer | `customer_id` not null and unique; `customer_status` in documented accepted values; `updated_at` not null |
| product | `product_id`, `product_name`, `category_code`, `unit_price`, `currency`, `updated_at` | one row per product | id not null/unique; non-negative price; ISO currency; category is referentially valid when reference data is available |
| location | `location_id`, `location_name`, `country_code`, `timezone`, `updated_at` | one row per location | id not null/unique; ISO country code; timezone populated |
| device | `device_id`, `location_id`, `device_type`, `device_status`, `updated_at` | one row per device | id not null/unique; location relationship checked; status accepted-value check |
| order | `order_id`, `customer_id`, `order_status`, `order_ts`, `currency`, `order_total`, `updated_at` | one current source row per order | id not null/unique; customer key populated; non-negative total; valid currency and status |
| payment | `payment_id`, `order_id`, `payment_status`, `payment_ts`, `amount`, `currency`, `updated_at` | one current source row per payment | id not null/unique; order key populated; non-negative amount; valid status/currency |
| exchange rate | `rate_date`, `base_currency`, `quote_currency`, `rate`, `retrieved_at` | one daily currency pair | compound key unique; positive rate; base and quote currencies differ |

Gold model names (`dim_customer`, `dim_product`, `dim_location`, `dim_date`, `fct_orders`, `fct_payments`, and marts) are planned consumers, not implemented tables. Their grain must be reconciled to this contract before MDEP-7.

## Batch landing envelope

All Airflow batch ingestion records will add the following metadata without changing source business fields.

| Field | Type | Meaning |
| --- | --- | --- |
| `ingestion_id` | UUID | unique execution identity for a landing attempt |
| `source_name` | string | `postgresql`, `rest_api`, `csv_file`, or `json_file` |
| `source_entity` | string | table, endpoint, or file dataset name |
| `source_record_key` | string nullable | source primary/natural key when available |
| `source_extract_ts` | timestamp UTC | time supplied by the source or extraction boundary |
| `ingested_at` | timestamp UTC | time the record was landed |
| `source_version` | string nullable | schema/version indicator, ETag, or file version when available |
| `source_locator` | string | table watermark, request URL/page, or file path |
| `record_hash` | string | SHA-256 of canonicalized source payload for duplicate detection |

**Idempotency rule:** a retry reuses its deterministic landing identity for the logical interval and source locator. Silver deduplication uses a business key plus source version/updated timestamp where present; `record_hash` supports sources without a stable key. A duplicate remains acceptable in Bronze and is handled downstream.

## Kafka business-event envelope

Kafka topics use business-event payloads for `order.created`, `order.completed`, `payment.authorized`, `payment.completed`, `payment.failed`, and `device.status_changed`.

| Field | Type | Meaning |
| --- | --- | --- |
| `event_id` | UUID | immutable event identity and deduplication key |
| `event_type` | string | dotted event name, for example `payment.completed` |
| `event_version` | integer | backwards-compatible payload version |
| `occurred_at` | timestamp UTC | business event time used by Flink event-time logic |
| `produced_at` | timestamp UTC | producer emission time |
| `aggregate_type` | string | `order`, `payment`, or `device` |
| `aggregate_id` | string | business key for partitioning/keyed state |
| `payload` | object | event-specific business attributes |
| `trace_id` | string nullable | optional correlation identifier across a business flow |

**Ordering rule:** ordering is guaranteed only for records with the same Kafka key/`aggregate_id` in a partition. Consumers must not assume global ordering. **Duplicate rule:** Silver event tables deduplicate by `event_id`; a duplicate delivery does not create another logical event. **Late-data rule:** `occurred_at`, not `produced_at`, governs event-time processing; the explicit watermark/late-event disposition is deferred to MDEP-11.

## PostgreSQL CDC envelope and mutation rules

Debezium supplies the CDC envelope for the PostgreSQL entities in the matrix. The contract relies on the key and source metadata rather than inventing a second CDC protocol.

| Field/group | Required use in downstream processing |
| --- | --- |
| Kafka message key | source primary key, retained for all mutations including deletes |
| `op` | Debezium operation: `r` snapshot read, `c` create, `u` update, `d` delete |
| `before` / `after` | retain both where emitted; `after` is null for a delete |
| `source.lsn`, transaction metadata, and connector coordinates | ordering/audit position; combined with source/table/key to identify a delivered change |
| source timestamp | ingestion/audit metadata, not a substitute for business event time |
| schema/payload | input for additive schema evolution checks |

**Current-state application rule:** apply changes by source primary key. `r`, `c`, and `u` create or replace the current record after validation; `d` removes or marks the current record according to the Silver table’s documented delete representation, while Bronze retains the original event.

**Duplicate rule:** delivery is treated as at-least-once. The consumer records or compares the Debezium source position and does not apply the same logical change twice. **Conflict rule:** per-key source order follows the Kafka partition/key path; cross-key transaction ordering is not used to define business correctness. **Schema rule:** additive fields may be stored and documented; incompatible type/semantic changes are quarantined or paused pending a contract update.

## Quarantine contract

Every rejected batch or stream record must retain `source_name`, `source_entity` or topic, `source_locator` or Kafka partition/offset, `ingested_at`, `rejection_reason`, `contract_version`, and the original payload/reference. Quarantine is evidence for diagnosis and replay; it is not a consumer-ready Silver dataset.

## Contract validation walkthrough

The following contract-only walkthrough is the MDEP-6 validation baseline. It does not claim that a pipeline has run.

| Scenario | Expected contract result |
| --- | --- |
| order update | CDC key is `order_id`; Debezium `u` change replaces the one `core_orders` current-state row; the original change remains replayable in Bronze |
| payment failure event | one `payment.failed` business event with an immutable `event_id`; it belongs in `evt_payments` and must not overwrite the CDC `core_payments` state by itself |
| duplicate file row | both records may land in Bronze with metadata; Spark identifies the duplicate by business key/version or `record_hash` and routes the invalid/extra record according to Silver rules |
| late device event | raw event remains replayable; `occurred_at` allows Flink to evaluate lateness later; no batch or CDC current-state writer takes ownership of that event |

## Open implementation decisions

- Exact field names/types for the physical PostgreSQL tables and individual REST/file payloads are finalized in MDEP-7, but must conform to these logical contracts.
- Watermark duration, allowed lateness, and delete representation are operational decisions for MDEP-11/MDEP-6 integration work and must not be silently chosen by batch jobs.
- The shared Iceberg catalog and Snowflake access decision is recorded in [ADR-0001](../../docs/decisions/0001-iceberg-catalog-and-snowflake-access.md).
