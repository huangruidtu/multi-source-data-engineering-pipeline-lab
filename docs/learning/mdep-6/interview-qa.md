# MDEP-6 Interview Q&A

## 1. What makes this data contract useful?

**Direct answer:** It specifies grain, keys, quality expectations, mutation semantics, and exactly one canonical Silver writer per dataset.

**Explanation:** A schema alone does not say who may write a dataset or how retries/deletes behave. The contract does.

**Project example:** `core_orders` is one current row per `order_id`; Flink CDC is its canonical Silver writer, while Airflow batch extraction is Bronze-only.

**Follow-up:** “How do you enforce it?” **Stronger answer:** The next implementation Stories should encode the contract into source validation, ingestion metadata, quality tests, and code review; MDEP-6 itself is a documented control, not automated enforcement.

## 2. Why not let batch and CDC both update Silver?

**Direct answer:** They can race and apply different freshness/deletion logic, causing dual-write inconsistency.

**Explanation:** Batch snapshots are valuable for recovery but do not automatically preserve the ordered mutation semantics of CDC.

**Project example:** PostgreSQL batch data is designated Bronze snapshot/backfill material; Debezium-to-Flink owns current-state Silver.

**Follow-up:** “How would you reconcile a snapshot?” **Stronger answer:** Design an explicit, controlled reconciliation/backfill workflow with cutover position and audit evidence rather than treating routine batch as a second writer.

## 3. What is the difference between `event_id` and `aggregate_id`?

**Direct answer:** `event_id` identifies one immutable event for deduplication; `aggregate_id` identifies the business entity and is used for keyed ordering/state.

**Explanation:** Several events can belong to one order, payment, or device.

**Project example:** `payment.failed` is one row in `evt_payments` per `event_id`, keyed by its payment aggregate.

**Follow-up:** “Is order global?” **Stronger answer:** No. The contract only promises ordering for the same Kafka key/aggregate in a partition.

## 4. How does the contract handle bad data?

**Direct answer:** Bronze retains the raw payload; Silver validates it; rejected records go to Quarantine with reason and source location.

**Explanation:** This separates evidence retention from consumer-ready data.

**Project example:** A duplicate file row can land in Bronze and then be deduplicated or rejected using its business key/version or `record_hash`.

**Follow-up:** “Would you drop PII?” **Stronger answer:** The current contract does not define privacy handling; a production design would add access control, retention, and redaction requirements before raw-payload retention.

## 5. Why choose HadoopCatalog?

**Direct answer:** It gives Spark and Flink a shared S3-backed Iceberg catalog without adding an extra catalog service to V1.

**Explanation:** The choice preserves the approved technology boundary.

**Project example:** ADR-0001 fixes the warehouse shape as `s3://<bucket>/iceberg/<namespace>/<table>/` and keeps Snowflake read-only for Silver.

**Follow-up:** “Has the integration been tested?” **Stronger answer:** No; it is an accepted planning decision. Physical S3, Spark/Flink, and Snowflake integration are deferred.
