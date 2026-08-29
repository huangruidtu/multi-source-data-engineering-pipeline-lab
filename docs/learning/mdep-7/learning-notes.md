# MDEP-7 Learning Notes — Deterministic Inputs and Deliberate Defects

## Reproducible source state

**Meaning:** the same reset produces the same starting data and behavior. **Here:** the seed inserts three rows per relational table and `reset-sources.ps1` destroys the local Compose volume before restarting. **Why:** a failed mutation or ingestion exercise can be repeated from a known state. **Production relevance:** production recovery uses backups, snapshots, migrations, and controlled restores rather than casually deleting volumes.

## Source-side relational integrity

**Meaning:** primary keys identify rows and foreign keys prevent invalid references. **Here:** `orders.customer_id` and `payments.order_id` are foreign keys; the invalid-order SQL demonstrates a rejected unknown customer. **Why:** a source constraint can prevent downstream garbage. **Risk:** not all real sources enforce such rules, so downstream quality checks remain necessary.

## CDC-ready mutations versus CDC itself

**Meaning:** source INSERT/UPDATE/DELETE are prerequisites for a CDC exercise, but they are not CDC events until a connector captures them. **Here:** `cdc-mutations.sql` changes `cust-400`, `ord-200`, and `cust-300`; logical WAL configuration is set. **Why:** it makes later Debezium testing deliberate and observable. **Risk:** claiming the changes reached Kafka would be false; no connector is implemented or validated.

## Pagination, retry, rate limit, and timeout

**Meaning:** API ingestion needs a traversal checkpoint and response-aware retry behavior. **Here:** `next_page` carries page traversal state; 503 and 429 return `Retry-After: 1`; timeout delays three seconds. **Why:** retrying every failure identically can overload a provider or repeat data. **Production relevance:** persist checkpoints, back off with jitter, respect rate budgets, and make writes idempotent.

## Malformed versus semantically invalid data

**Meaning:** malformed JSON cannot be decoded; semantically invalid JSON is syntactically valid but violates business/schema expectations. **Here:** one device fixture has bad JSON syntax; another has null/unknown/bad values. **Why:** parsing and validation failures have different diagnostics and potential quarantine actions. **Risk:** a pipeline that only checks parser success can accept unusable data.

## File identity versus file content

**Meaning:** a different filename can contain an identical payload. **Here:** `product_categories_duplicate.csv` is byte-for-byte equal to the valid file. **Why:** ingestion must decide whether deduplication is based on path, content hash, source version, or business keys. **Production relevance:** this prevents a resend/renamed file from doubling facts.

## Schema evolution exercise

**Meaning:** a new source field can be a controlled additive change. **Here:** `add-customer-loyalty-tier.sql` adds `loyalty_tier` outside the reset path. **Why:** it lets later ingestion demonstrate contract evolution. **Risk:** a consumer with a fixed schema may fail or drop the new field unless its evolution policy is explicit.
