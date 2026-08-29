# Learning Notes — MDEP-7 Reproducible Sources

**Status:** IMPLEMENTED source definitions and local runtime configuration. PostgreSQL runtime validation requires Docker on the host.

## What was implemented

This Story adds a two-service Docker Compose lab: PostgreSQL 16 with a compact `commerce` schema and a standard-library Python REST source. It also adds deterministic CSV/JSON reference fixtures, deliberately invalid fixtures, source mutation examples, reset/validation scripts, and [local run instructions](../source-systems.md).

## Source-system design

- PostgreSQL models only the four CDC-owned entities from the MDEP-6 contract: customers, products, orders, and payments.
- Primary and foreign keys make source relationships observable before any downstream pipeline exists.
- The database starts with `wal_level=logical`, but Debezium and Kafka are explicitly deferred to MDEP-10.
- The REST source returns fixed exchange-rate and location records. Query parameters expose pagination and controlled 503, 429, and delay behavior without relying on an external API.

## Concepts to inspect

- **Primary/foreign keys:** `orders.customer_id` and `payments.order_id` demonstrate why an invalid source relationship should fail before data reaches a pipeline.
- **Deterministic reset:** recreating the Compose volume restores exactly three rows in each table, which makes later backfill and CDC exercises repeatable.
- **Pagination/retry:** `next_page` communicates traversal state; 503 and 429 use `Retry-After` to distinguish retryable server failure from rate limiting.
- **File identity:** two CSV filenames have identical bytes, so an ingestion design must distinguish content duplication from a new file arrival.
- **Malformed versus semantically invalid JSON:** one fixture cannot be parsed; another parses but violates the contract. They need different diagnostics.

## Intentional failure exercises

- nullable customer email in the seed data;
- duplicate CSV content and duplicate category key;
- null category code and a non-numeric CSV sort order;
- semantic device JSON errors and malformed JSON;
- intentionally absent location-overrides file;
- PostgreSQL foreign-key violation example;
- additive `loyalty_tier` source schema change;
- API 503, 429, and timeout scenarios;
- PostgreSQL INSERT, UPDATE, and DELETE mutations for later CDC.

## Run and inspect

Use the exact commands in [Local Source Systems](../source-systems.md). Inspect the reset seed with `psql`, page through the REST endpoints, then run the mutation and failure-example SQL files one at a time. Do not run the schema-addition file before a reset if you want the baseline schema.

## Files created by this Story

- `docker-compose.yml`
- `source-data/postgres/`
- `source-data/rest-api/`
- `source-data/files/`
- `scripts/reset-sources.ps1`
- `scripts/validate-sources.ps1`
- `docs/source-systems.md`
- this learning document

## Interview questions derived from this implementation

- How do foreign keys help identify bad source data before ingestion?
- What makes a test data environment reproducible?
- How would you checkpoint a paginated API and safely retry a 503 or 429 response?
- How do malformed JSON and schema-invalid JSON require different handling?
- How could an ingestion job recognize duplicate file content?
- What must be true in PostgreSQL before a CDC connector can read changes?
