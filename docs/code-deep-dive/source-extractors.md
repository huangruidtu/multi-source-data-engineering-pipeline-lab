# Code Deep-Dive: `ingestion/batch/extractors.py`

**Source of truth:** [`ingestion/batch/extractors.py`](../../ingestion/batch/extractors.py).

## Read beside
- **Source:** [`extractors.py`](../../ingestion/batch/extractors.py)
- **Tests:** [`tests/test_bronze_ingestion.py`](../../tests/test_bronze_ingestion.py)
- **Architecture:** [`docs/finalization/offline-validation-coverage.md`](../finalization/offline-validation-coverage.md)
- **Interview topics:** [`batch-pipeline.md`](batch-pipeline.md), [`bronze-batch-publication.md`](bronze-batch-publication.md)

## 1. Why this file exists
It contains source-boundary behavior only: pagination/retry, file parsing/identity, and PostgreSQL read shape. No Bronze publishing or Silver business transformation lives here.
## 2. Where it sits in the architecture
Sources feed these extractors; `pipeline.py` enriches and publishes their returned records.
## 3. Inputs / outputs / state
HTTP URL, local path, or database connection factory enters; lists of source dictionaries exit. Retry attempts/page number are transient local control state.
## 4. Important symbols
`RETRYABLE_HTTP`, `fetch_paginated_json`, `file_identity`, `read_csv`, `read_json`, `postgres_rows`.
## 5. Execution flow
REST requests are made page-by-page and accumulated until `next_page` is null. Files are decoded into records and content-hashed. PostgreSQL executes either an allowlisted full-table select or bounded `updated_at` query.
## 6. Function-by-function walkthrough
`RETRYABLE_HTTP` limits retries to 429 and selected transient 5xx responses. `fetch_paginated_json` parses an existing URL query, replaces/adds `page`, allows `retries + 1` attempts, honors `Retry-After` for retryable HTTP failures, and retries `URLError` with one second sleep. It extends its local list only after each successful decoded response and returns only after every page succeeds.

`file_identity` SHA-256 hashes bytes, so renamed identical files remain duplicates. `read_csv` uses `DictReader`; `read_json` accepts a list or wraps one JSON object in a list. `postgres_rows` rejects tables outside the four commerce tables, uses parameterized interval values, and applies `updated_at >= %s AND updated_at < %s` only when both bounds exist.
## 7. Critical code-block reasoning
Publishing page 1 before page 2 is known to succeed would create a partial logical landing: retrying might duplicate/reconcile an incomplete set. Returning all pages first lets `pipeline.py` publish one deterministic operation. The half-open SQL interval gives adjacent windows a single owner for a boundary timestamp.
## 8. Correctness invariants
- Non-retryable or exhausted HTTP failures fail extraction.
- Source records are returned only after all REST pages succeed.
- File duplicate identity is content-based.
- PostgreSQL table scope is explicit.
- Adjacent incremental windows do not overlap at `end`.
## 9. Failure behavior
HTTP/URL exhaustion raises a contextual `RuntimeError`; malformed JSON raises to the caller, which decides quarantine. Unsupported database table raises before SQL construction. Nothing in this file publishes a partial object.
## 10. Tests that protect the behavior
The Bronze tests simulate a 429 followed by two pages and assert both records return; they also check content identity. **MDEP OFFLINE TESTED**; no live endpoint/database was contacted.
## 11. What is not implemented / runtime deferred
**MDEP RUNTIME DEFERRED:** real source authentication, database connections, HTTP rate-limit behavior, and source consistency under concurrent mutation.
## 12. Production concepts beyond current code
**GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED:** circuit breakers, jittered exponential backoff, cursor checkpoints, API schema contracts, and transaction-isolation tuning.
## 13. Common misunderstandings
Retries do not make an API response idempotent; they only repeat a request. `catchup` policy is not an extractor concern. A time-bounded query is not proof that `updated_at` is a perfect change-data clock.
## 14. Interview questions
**Why collect all pages before Bronze publication?** A Bronze object represents one logical extraction. Publishing partial pages makes retries/reconciliation ambiguous; accumulating first gives all-or-fail landing semantics.
## 15. 30-second spoken explanation
“`extractors.py` keeps source behavior isolated: it retries only retryable HTTP failures, respects Retry-After, collects every page before returning, hashes file content for duplicate detection, and allowlists PostgreSQL reads with optional half-open incremental bounds. Publication is intentionally somebody else’s responsibility.”
## 16. Senior follow-up discussion
Ask how an API changes while pages are fetched. A robust answer distinguishes this implementation’s all-pages-before-publish guarantee from a true point-in-time source snapshot, which may require source cursor/version semantics or reconciliation.
