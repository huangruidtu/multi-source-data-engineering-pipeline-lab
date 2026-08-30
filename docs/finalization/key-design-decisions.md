# Key Design Decisions

Each decision is grounded in the repository and distinguishes V1 design from
future production recommendations.

## Canonical Silver writers
**Problem:** two processors writing the same current-state table can race or
erase each other's semantics. **Decision:** Spark owns only reference Silver;
Flink owns CDC commerce Silver. **Evidence:** `BATCH_SILVER_ENTITIES` and
`CDC_ENTITIES`. **Why:** each path has one freshness rule. **Alternative:** let
Spark and Flink both write orders. **Trade-off:** explicit ownership reduces
flexibility. **Failure implication:** ownership errors fail early. **Interview:**
“A table has one writer because correctness is clearer than shared convenience.”

## Bronze history, Silver current state, quarantine
**Problem:** replay and malformed data need evidence. **Decision:** immutable
Bronze/archive, current-state Silver, reasoned quarantine. **Evidence:**
`bronze.py`, `_bronze_values`, `quarantine_record`. **Alternative:** overwrite
raw data or drop invalid rows. **Trade-off:** storage/operational cost. **Failure
implication:** replay does not destroy evidence. **Interview:** “Bronze tells me
what arrived; Silver tells consumers what state is trusted.”

## Airflow orchestrates; Spark transforms
**Problem:** DAG code becomes untestable business logic. **Decision:** Airflow
calls batch functions; Spark owns Silver transforms. **Evidence:** DAG plus
`pipeline.py`/`silver_batch.py`. **Alternative:** transform in Airflow tasks.
**Trade-off:** cross-module flow. **Failure implication:** retry does not change
transformation semantics. **Interview:** “Airflow controls execution, not truth.”

## Spark for bounded batch; Flink for CDC state
**Problem:** reference loads and change streams have different temporal models.
**Decision:** Spark windows/ranks bounded Bronze; Flink keeps keyed state.
**Evidence:** `split_valid_and_quarantine` and `CdcStateApplier`. **Alternative:**
one engine for all work. **Trade-off:** two learning/runtime surfaces. **Failure
implication:** each uses its appropriate idempotency rule. **Interview:** “I use
Spark for bounded transforms and Flink where stateful change handling matters.”

## Iceberg rather than plain Parquet for Silver
**Problem:** trusted current state needs table metadata and updates, not only
files. **Decision:** Parquet Bronze; Iceberg Silver. **Evidence:** Spark merge
and Flink Iceberg DDL. **Alternative:** plain Parquet everywhere. **Trade-off:**
catalog/runtime complexity. **Failure implication:** snapshot/commit behavior
still needs V1.x proof. **Interview:** “Parquet is a format; Iceberg is the table
contract around it.”

## External Snowflake Silver; dbt-owned Gold
**Problem:** analytics should not become another Silver writer. **Decision:**
Snowflake externally reads Iceberg Silver; dbt materializes Gold. **Evidence:**
`01_setup.sql`, dbt models. **Alternative:** duplicate Silver into warehouse.
**Trade-off:** external metadata integration. **Failure implication:** stale
metadata is visible as a runtime risk. **Interview:** “The warehouse consumes
Silver and owns business models, not ingestion state.”

## LSN beats Kafka offset for freshness
**Problem:** transport order is not source change order across partitions.
**Decision:** source LSN first, same-transaction order next. **Evidence:**
`version_decision`. **Alternative:** largest Kafka offset wins. **Trade-off:**
requires source metadata. **Failure implication:** lower LSN cannot regress
state. **Interview:** “Offsets locate delivery; LSN orders PostgreSQL change.”

## Hash is a final tie-breaker
**Problem:** a payload change may be old. **Decision:** hash is last in Spark's
version tuple. **Evidence:** `VERSION_FIELDS`, `incoming_is_newer`, merge SQL.
**Alternative:** update whenever hash differs. **Trade-off:** equal-time records
need deterministic choice. **Failure implication:** stale-hash regression is
prevented. **Interview:** “Hash proves difference, never freshness.”

## Conservative equal-position CDC conflict
**Problem:** same LSN can be ambiguous without order/identity evidence.
**Decision:** accept a known exact transport replay only; otherwise conflict.
**Evidence:** `EQUAL_POSITION_CONFLICT`. **Alternative:** last arrival wins.
**Trade-off:** some events require investigation. **Failure implication:** avoids
unprovable overwrite. **Interview:** “When evidence is insufficient, correctness
wins over availability.”

## `fct_payments` full rebuild and Type 1 dimensions
**Problem:** an incremental payment fact must reflect deletes and order relinks.
**Decision:** full rebuild; current Type 1 dimensions in V1. **Evidence:**
`fct_payments.sql` and marts. **Alternative:** incremental merge plus SCD2.
**Trade-off:** higher runtime cost, no dimension history. **Failure implication:**
current-state correctness is simpler. **Interview:** “It is a bounded V1 choice,
not a production-scale claim.”
