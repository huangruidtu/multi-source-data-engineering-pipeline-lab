# MDEP-7 Interview Q&A

## 1. How did you make source integration data repeatable?

**Direct answer:** I used deterministic SQL seeds, a local Compose topology, fixed REST records, and explicit mutation/failure files.

**Explanation:** Repeatability allows the same bug, retry, or backfill exercise to start from known data.

**Project example:** resetting recreates three rows in each of `customers`, `products`, `orders`, and `payments`; REST exchange rates are fixed in `app.py`.

**Follow-up:** “Was it fully runtime-validated?” **Stronger answer:** REST and fixture behavior were validated; Docker/PostgreSQL validation was not run on the implementation host because Docker and `psql` were unavailable.

## 2. What does `wal_level=logical` accomplish here?

**Direct answer:** It is a PostgreSQL prerequisite that allows logical decoding, which a later Debezium connector needs to capture changes.

**Explanation:** It does not itself publish to Kafka or create a connector.

**Project example:** Compose sets `wal_level=logical`, replication slots, and WAL senders; `cdc-mutations.sql` supplies INSERT, UPDATE, and DELETE inputs.

**Follow-up:** “What else is needed?” **Stronger answer:** Connector configuration, source permissions, publication/slot behavior as required, Kafka connectivity, snapshot/offset policy, and event inspection—all deferred here.

## 3. Why create both a malformed JSON fixture and a semantically invalid JSON fixture?

**Direct answer:** They fail at different stages: parsing versus contract/reference validation.

**Explanation:** Different failures need different reason codes, metrics, and recovery actions.

**Project example:** `device-reference-malformed.json` cannot parse; `device-reference-invalid.json` parses but includes contract-level problems.

**Follow-up:** “Where would they go?” **Stronger answer:** Future Bronze capture should retain evidence, and future Silver validation should quarantine with explicit reason and source locator.

## 4. How should an ingestion job treat 503, 429, and a timeout?

**Direct answer:** Treat them as transport/control-plane failures, not invalid data; retry with a bounded policy that respects `Retry-After` for 503/429 and avoids duplicate writes.

**Explanation:** 429 signals provider throttling, while timeout creates ambiguity about whether the request/response completed.

**Project example:** the local API returns `Retry-After: 1` for both deterministic scenarios and delays three seconds for timeout testing.

**Follow-up:** “What would production add?” **Stronger answer:** persisted checkpoints, exponential backoff with jitter, observability, circuit breaking, and idempotent landing keyed by request/source identity.

## 5. Why enforce foreign keys if downstream also validates data?

**Direct answer:** Source constraints reject impossible relationships early, while downstream checks protect against sources that cannot enforce them or data extracted before validation.

**Explanation:** These are complementary controls at different boundaries.

**Project example:** an order for `cust-999` fails in PostgreSQL; later Silver quality rules still require populated/referentially valid keys.

**Follow-up:** “Could deletes break it?” **Stronger answer:** Yes; the mutation deletes an unreferenced customer. Production delete policy must be coordinated with CDC and downstream tombstone/current-state representation.

## 6. What is the duplicate-file lesson?

**Direct answer:** File path/name is not a safe unique identity; a renamed or resent file can have identical content.

**Explanation:** The deduplication strategy must combine content hash, source version, business keys, and processing context.

**Project example:** two product-category CSV files have byte-for-byte identical contents.

**Follow-up:** “Would you hash huge production files?” **Stronger answer:** It depends on scale and latency; object version/ETag plus manifest metadata may be preferable, but the policy must handle multipart and provider semantics.
