# Code Deep-Dive: `ingestion/batch/bronze.py`

**Source of truth:** [`ingestion/batch/bronze.py`](../../ingestion/batch/bronze.py). It implements deterministic batch landing and can use a local object-store stand-in or S3 API adapter. Real AWS/S3 execution is **MDEP RUNTIME DEFERRED**.

## Read beside

- **Source file:** [`ingestion/batch/bronze.py`](../../ingestion/batch/bronze.py)
- **Test file:** [`tests/test_bronze_ingestion.py`](../../tests/test_bronze_ingestion.py)
- **Related architecture:** [`docs/finalization/data-model-and-grain.md`](../finalization/data-model-and-grain.md)
- **Related interview topic:** [`docs/finalization/interview-cheat-sheet.md`](../finalization/interview-cheat-sheet.md) — deterministic Bronze landing and retry behavior

## 1. Why this file exists

Bronze should be source-aligned evidence, not an anonymous collection of transformed records. This file gives each batch landing operation a stable identity, enriches each record with lineage fields, generates deterministic Bronze/Quarantine keys, and publishes a Parquet object plus a content manifest without overwriting an existing canonical object.

## 2. Where it sits in the architecture

`REST API / files -> extractor/pipeline -> this Bronze publisher -> Bronze Parquet + quarantine -> Spark Silver`.

It is the batch ingestion boundary; CDC Bronze is a separate Flink-owned layout. Airflow may trigger batch work but does not replace the identity/idempotency contract in this module.

## 3. Inputs, outputs, and state

| Item | Contract |
|---|---|
| Input | `BatchContext` plus source records and record-level lineage facts. |
| Bronze output | `bronze/<source>/<entity>/ingest_date=.../ingestion_id=.../data.parquet`. |
| Manifest | Adjacent `data.parquet.manifest.json` with interval, record count, payload hash. |
| Quarantine output | JSON Lines with original payload and rejection reason. |
| State | Object existence is the idempotency state; no database state table is introduced. |

## 4. Important symbols

| Symbol | Meaning |
|---|---|
| `canonical_json`, `sha256` | Stable representation/hash independent of dictionary insertion order. |
| `BatchContext` | Immutable identity inputs for one source/entity/data-interval operation. |
| `ingestion_id` | First 24 hex characters of context hash; deterministic, not a random run ID. |
| `bronze_key`, `quarantine_key` | Canonical physical layout. |
| `enrich_record` | Adds the MDEP source envelope while retaining business fields. |
| `LocalObjectStore` | Local filesystem adapter matching object-key semantics. |
| `S3ObjectStore` | Optional `boto3` adapter using conditional writes. |
| `BronzePublisher` | Stages Parquet, publishes only if absent, then writes manifest. |

## 5. Execution flow

1. An orchestrator/extractor creates `BatchContext` for a logical date and data interval.
2. `enrich_record` keeps the source payload and appends lineage, locator, timestamps, source version, and content hash.
3. Invalid source material becomes a `quarantine_record` with original payload.
4. `publish` resolves a deterministic key and returns early if both data and manifest already exist.
5. It writes a staged local Parquet file, conditionally creates the canonical data object, then conditionally creates the manifest.
6. A retry returns `already_published` instead of appending or replacing data.

## 6. Function-by-function walkthrough

### Canonical serialization and `sha256`

`canonical_json` sorts dictionary keys and uses compact separators. `sha256` then hashes that representation. This matters because the same logical Python dictionary can have a different construction history; an identity/hash must reflect content, not incidental insertion order.

### `BatchContext.ingestion_id`

The ID derives from logical date, exact interval bounds, source name, and source entity. It deliberately excludes wall-clock execution time and retry attempt. A retry of the same logical operation therefore points to the same target object, while a different interval produces a different identity.

### Key builders

The Bronze path exposes source/entity/date/identity as partitions/prefixes. That enables human inspection and source-scoped reads. The Quarantine path mirrors that identity so rejected evidence stays attributable to the same logical landing operation.

### `enrich_record` and `quarantine_record`

`enrich_record` starts with a copy of business fields, then adds the source envelope. Critically, `record_hash` is calculated from the original business `record`, not from timestamps/locator/ingestion metadata that naturally change between attempts. `quarantine_record` records reason, contract version, locator, and raw payload; it does not pretend to normalize an invalid record.

### Object-store adapters

`LocalObjectStore.put_if_absent` opens the destination with exclusive-create mode (`xb`). `S3ObjectStore` uses `IfNoneMatch="*"`; a 412/PreconditionFailed means another attempt has already won, not that the operation should overwrite it. Both implement the same small protocol so local tests exercise the intended collision behavior.

### `BronzePublisher.publish`

The early return requires **both** data object and manifest. If only data exists, the function does not overwrite it; it creates the missing manifest. It writes Parquet to a temporary directory first, then conditionally publishes the final object. The manifest captures the record count and a hash of the record collection, giving a reviewer compact evidence of what the logical operation intended to land.

`quarantine` similarly uses conditional text creation. It does not use append semantics despite the local method name `append_json_lines`; retry behavior must remain deterministic.

## 7. Critical code-block reasoning

`object_exists = self.store.exists(key)` followed by `put_if_absent` is intentionally not treated as an atomic check-and-create transaction. Another writer can win between them; the conditional write resolves that race safely. The returned boolean is inverted into `object_exists = not put_if_absent(...)`, so the final result accurately says `already_published` if a competing canonical write exists.

The publish ordering is data first, manifest second. The manifest is treated as completion evidence: seeing both gives a fast idempotent success. Seeing only data triggers repair of the manifest without rewriting the data. This is an intentionally small reconciliation rule, not a full distributed commit protocol.

## 8. Correctness invariants

- Same logical context maps to the same ingestion ID and keys.
- Record content hash does not depend on changing landing metadata.
- A published canonical Bronze object is never overwritten by a retry.
- A completed publication has both Parquet data and manifest.
- Quarantine retains raw rejected evidence and reason.
- Local and S3 adapters share conditional-create semantics.

## 9. Failure behavior

If PyArrow is unavailable, `publish` raises an actionable error rather than emitting a fake Parquet object. Unexpected storage errors propagate; only expected conditional-write conflict codes translate to an already-exists result. A crash after data but before manifest leaves a detectable partial completion that a retry repairs. The code does not claim transactional atomicity across two S3 objects.

## 10. Tests that protect the behavior

[`tests/test_bronze_ingestion.py`](../../tests/test_bronze_ingestion.py) checks deterministic path/metadata, evidence-preserving quarantine, content-based file identity, and retry behavior for paginated source requests. When PyArrow is installed, it also checks first publication versus repeat `already_published` and a source-aligned file landing case. On the current standard offline environment, the PyArrow-dependent tests may be skipped; that is reported as a dependency limitation, not a passed physical S3 validation.

## 11. What is not implemented / runtime deferred

**MDEP RUNTIME DEFERRED:** actual S3 credentials/network access, object-versioning/lifecycle policies, Airflow execution, production IAM, and fault injection around real S3/PyArrow writes. MDEP does not claim that a multipart upload or cross-object manifest/data transaction was executed.

## 12. Production concepts beyond current code

**GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED:** immutable object versioning, encryption/KMS policy, lifecycle retention, event notifications, content-addressed data keys, and a durable outbox/commit log. Those can be appropriate at scale, but adding them would change the small V1 learning architecture.

## 13. Common misunderstandings

- "A deterministic `ingestion_id` means every execution is the same." It identifies the same *logical* operation; records can still be wrong and require investigation.
- "Manifest means multi-object atomic commit." No; it is completion evidence plus retry repair.
- "Bronze data must be cleaned before storage." No; Bronze preserves source-aligned payload/envelope; validation belongs downstream or in quarantine.
- "Conditional create eliminates all concurrent design concerns." It prevents overwriting the canonical key, but operational monitoring still matters.

## 14. Interview questions

**How is batch ingestion idempotent here?** The source/entity/interval context creates a deterministic key. Publication uses conditional create; the first object is canonical and retries return `already_published`, while the manifest lets a retry repair an incomplete data-plus-manifest pair without rewriting data.

**Why hash only the original record in `enrich_record`?** Ingestion timestamps and locators can change between retries. Including them would make the same source payload look different and defeat content-level comparison.

**Why keep a local object store?** It makes the key and conditional-write contract testable offline without claiming real AWS execution.

## 15. 30-second spoken explanation

“`bronze.py` is the batch landing contract. It gives each source/entity/data-interval operation a deterministic ingestion ID, enriches raw records with lineage, and conditionally writes one canonical Parquet object plus a manifest. A retry cannot overwrite or append to that object; it either returns already published or repairs the missing manifest. The conditional behavior is tested offline with a local adapter, while real S3 execution is deferred.”

## 16. Senior follow-up discussion

Ask how to handle a data object that exists with an invalid or missing manifest. The MDEP answer is intentionally limited: a retry can create a missing manifest, but it cannot prove an existing object was produced correctly. A production design would need stronger checksums/metadata, audit records, permissions, and an explicit operator reconciliation workflow before treating it as complete.
