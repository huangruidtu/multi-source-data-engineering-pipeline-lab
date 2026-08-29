# MDEP-6 Implementation Guide — Data Contracts and Ownership

## Story goal and result

MDEP-6 defines the shared Commerce & Operations contract baseline before pipeline code exists. The implemented result is a source-to-layer ownership matrix, a data dictionary, batch and event envelopes, CDC mutation rules, quarantine requirements, and one architecture decision record. It is documentation, not a running data platform.

## Actual files and structure

```text
source-data/contracts/commerce-operations.md  # logical data contracts
docs/decisions/0001-iceberg-catalog-and-snowflake-access.md  # ADR
docs/learning/mdep-6-data-contracts.md  # original concise learning note
```

The detailed contract is the implementation. No PostgreSQL tables, Kafka topics, Spark jobs, Iceberg tables, Snowflake objects, dbt models, Airflow DAGs, or Flink jobs were created by this Story.

## Components and data structures

The ownership matrix names the authoritative source, Bronze writer, canonical Silver writer, Silver dataset, Gold consumer, and grain for eleven logical datasets. PostgreSQL `customers`, `products`, `orders`, and `payments` are CDC-owned in Silver; Airflow batch extraction may create only Bronze snapshots/backfill material. REST and file references are batch-owned in Silver through Spark. Business events are Flink-owned in Silver. Snowflake/dbt owns Gold.

The data dictionary defines required fields and quality expectations for customer, product, location, device, order, payment, and exchange-rate records. IDs are natural/business identifiers unless explicitly made warehouse surrogate keys. Timestamps are UTC ISO-8601 and business dates are `YYYY-MM-DD`.

Two planned envelopes are specified:

- Batch landing metadata: `ingestion_id`, source identity/locator, extraction and ingestion times, version, and `record_hash`.
- Kafka business-event metadata: `event_id`, event type/version, `occurred_at`, `produced_at`, aggregate type/id, payload, and optional trace id.

For Debezium CDC, the contract relies on the source primary-key Kafka message key and Debezium `op`, `before`, `after`, source LSN, transaction metadata, and connector coordinates.

## Intended data flow

```text
PostgreSQL batch -> Bronze snapshot only
PostgreSQL -> Debezium -> Kafka -> Flink -> canonical Silver current state
REST/files -> Airflow -> Bronze -> Spark -> canonical Silver references
Kafka business events -> Flink -> canonical Silver event tables
Silver Iceberg -> Snowflake/dbt -> Gold (future stories)
```

The key boundary is that a current-state PostgreSQL entity has one canonical Silver writer: Flink applying CDC. The contract prevents a batch upsert and a streaming upsert from independently owning the same Silver table.

## How to review it

No service command exists for MDEP-6. Review the contract with:

```powershell
Get-Content -Raw source-data/contracts/commerce-operations.md
Get-Content -Raw docs/decisions/0001-iceberg-catalog-and-snowflake-access.md
```

Walk the four documented scenarios: an order update, a `payment.failed` event, a duplicate file row, and a late device event. For each, identify its key, Bronze evidence, one Silver writer, quality disposition, and eventual Gold consumer.

## Dependencies and boundaries

MDEP-6 is upstream of the physical source schema in MDEP-7 and of later Airflow, Debezium/Kafka, Spark, Flink, Iceberg, Snowflake, and dbt Stories. It does not select physical PostgreSQL column types, watermark values, delete representation, or create catalog integrations. Those choices are explicitly deferred.

## Expected inputs and outputs

Input: the approved charter and planned V1 architecture. Output: a reviewable contract that later source and processing code must conform to. It does not produce data, execute tests, or prove an external integration.
