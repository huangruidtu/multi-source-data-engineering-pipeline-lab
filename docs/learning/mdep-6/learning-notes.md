# MDEP-6 Learning Notes — Contracts as the Pipeline's Control Plane

## Canonical ownership

**Meaning:** one system/path is responsible for producing the trusted version of a dataset. **Here:** Flink CDC owns current PostgreSQL Silver tables; Spark owns batch reference Silver tables. **Why:** two writers can race, reconcile differently, or overwrite each other. **Production risk:** a batch snapshot can erase a CDC update if both upsert `core_orders`. The contract avoids that before code exists.

## Grain and keys

**Meaning:** grain says what one row represents; a key identifies that row/event. **Here:** `core_orders` is one current row per `order_id`; `evt_payments` is one row per `event_id`; exchange rates use a compound daily-currency-pair key. **Why:** fact/dimension calculations and deduplication are impossible to reason about without grain. **Risk:** using `payment_id` for a business-event table would collapse legitimate multiple payment events.

## Bronze, Silver, and Quarantine

**Meaning:** Bronze is raw/replayable evidence, Silver is validated technical state, Quarantine is retained rejected evidence. **Here:** duplicates can land in Bronze; invalid data is not silently discarded. **Why:** diagnosis and replay need the original payload and source locator. **Risk:** validating too early or deleting bad data removes evidence needed to reproduce an incident.

## Idempotency and delivery semantics

**Meaning:** a retry should not create a second logical result. **Here:** batch landing has deterministic identity plus `record_hash`; events deduplicate by `event_id`; CDC compares source positions. **Why:** file/API retries and Kafka delivery are not exactly-once by default. **Risk:** treating a successful retry as a new order/payment duplicates downstream measures.

## Event time and source order

**Meaning:** `occurred_at` is when the business event happened; Kafka source order is only meaningful per key/partition. **Here:** watermarks are deferred, but the envelope preserves both occurred and produced time. **Why:** late events must eventually be assessed against business time. **Risk:** using producer time hides late records; assuming global Kafka ordering invents guarantees the platform does not have.

## Schema evolution

**Meaning:** data contracts change deliberately, preferably additively. **Here:** additive fields can proceed with a contract update; incompatible changes require migration/quarantine/pause. **Why:** downstream consumers otherwise fail or silently reinterpret data. **Risk:** changing a currency field from a code to a symbol without versioning corrupts validation and joins.
