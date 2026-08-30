# Code Deep-Dive: `ingestion/batch/pipeline.py`

**Source of truth:** [`ingestion/batch/pipeline.py`](../../ingestion/batch/pipeline.py).

## Read beside
- **Source:** [`pipeline.py`](../../ingestion/batch/pipeline.py)
- **Tests:** [`tests/test_bronze_ingestion.py`](../../tests/test_bronze_ingestion.py)
- **Architecture:** [`docs/finalization/end-to-end-data-flow.md`](../finalization/end-to-end-data-flow.md)
- **Interview topics:** [`bronze-batch-publication.md`](bronze-batch-publication.md), [`source-extractors.md`](source-extractors.md)

## 1. Why this file exists
It composes source extractors with the deterministic Bronze contract. It is the batch task layer, not a Silver transformation engine.
## 2. Where it sits in the architecture
Airflow calls this file; it calls `extractors.py`; it invokes `BronzePublisher`; Spark later owns batch Silver.
## 3. Inputs / outputs / state
Inputs are logical date, source/table/entity, endpoint or files. Outputs are publication/quarantine result dictionaries. Object existence beneath `BronzePublisher` supplies idempotency state.
## 4. Important symbols
`context`, `publisher`, `land_rest`, `land_postgres`, and `land_files` are the public composition functions. `REPO_ROOT` makes fixtures/repository assets addressable without a caller-provided path.
## 5. Execution flow
`context()` converts `YYYY-MM-DD` into UTC `[start, end)`. A landing function extracts all source rows, enriches them, then publishes one canonical Bronze operation; file exceptions create quarantine evidence.
## 6. Function-by-function walkthrough
`context` fixes a calendar day to midnight UTC plus one day. `publisher` selects `S3ObjectStore` only when `BRONZE_S3_BUCKET` is configured, optionally with `BRONZE_S3_PREFIX`; otherwise it uses a local store under `build/local-object-store` or `BRONZE_LOCAL_ROOT`.

`land_rest` fetches all paginated rows before enriching each with retrieval/update evidence and publishing. `land_postgres` lazily requires `psycopg`, uses full snapshot by default, or passes the context's bounds to `postgres_rows` when `incremental=True`. It derives a source record key from singularized table name plus `_id` (for example `orders -> order_id`).

`land_files` separates valid and invalid fixtures. Valid content hashes prevent duplicate-content publication even under a renamed file. Invalid but parseable rows are retained for later Silver validation; malformed JSON, duplicates, and a deliberately missing expected file get reasoned quarantine records.
## 7. Critical code-block reasoning
The layer does not call Spark or create Silver rows: extraction/landing composition must not mix source evidence with business transformation. Its REST path publishes only after `fetch_paginated_json` returns every page. Its PostgreSQL incremental bounds are sourced from the same deterministic `BatchContext` that determines the output key.
## 8. Correctness invariants
- One logical source/entity/day maps to one UTC interval and deterministic Bronze identity.
- A local fallback changes storage adapter, not landing semantics.
- Partial REST pages never reach publication.
- Duplicate file *content*, not filename, is quarantined.
- This layer never owns Silver transformations.
## 9. Failure behavior
Missing `psycopg` fails PostgreSQL extraction explicitly. Extractor failures prevent publication. File parsing failures are quarantined with payload/error evidence; a missing scenario file is also observable rather than silently ignored.
## 10. Tests that protect the behavior
`test_bronze_ingestion.py` covers deterministic context-derived landing metadata, duplicate file content, REST retry/pages, quarantine, and—when PyArrow exists—publication/fixture behavior. **MDEP OFFLINE TESTED.**
## 11. What is not implemented / runtime deferred
**MDEP RUNTIME DEFERRED:** real PostgreSQL, REST service, S3, Airflow task execution, and physical Parquet publication.
## 12. Production concepts beyond current code
**GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED:** source credential management, per-source rate budgets, distributed lease control, and external run metadata.
## 13. Common misunderstandings
Airflow retry is not data idempotency; deterministic context/key plus conditional publication is. A full snapshot is not CDC. Quarantine is retained evidence, not silent deletion.
## 14. Interview questions
**Why not transform to Silver here?** This layer preserves source-aligned landing responsibility. Silver has different validation, ownership, and replay semantics, so mixing it into task composition makes failure/retry reasoning less clear.
## 15. 30-second spoken explanation
“`pipeline.py` composes source-specific extraction with deterministic Bronze publication. It turns a logical date into a UTC half-open interval, chooses an object-store adapter, lands REST/PostgreSQL/file sources, and quarantines bad file scenarios. It deliberately stops at Bronze; Spark owns Silver correctness.”
## 16. Senior follow-up discussion
Discuss how a backfill invokes the same functions with historical logical dates. Deterministic identity makes it safe to distinguish a rerun from a different interval, but source snapshot semantics and external mutations still need explicit reconciliation.
