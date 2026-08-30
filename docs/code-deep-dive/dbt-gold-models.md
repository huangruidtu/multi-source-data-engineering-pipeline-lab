# Code Deep-Dive: dbt dimensions, facts, and marts
**Source of truth:** [`analytics/dbt/models/marts/`](../../analytics/dbt/models/marts/) and [`schema.yml`](../../analytics/dbt/models/schema.yml).
## Read beside
- **Source:** [`marts`](../../analytics/dbt/models/marts/), [`schema.yml`](../../analytics/dbt/models/schema.yml)
- **Tests:** [`tests/test_mdep12_warehouse_contract.py`](../../tests/test_mdep12_warehouse_contract.py)
- **Architecture:** [`docs/finalization/data-model-and-grain.md`](../finalization/data-model-and-grain.md)
- **Interview topics:** [`docs/finalization/interview-cheat-sheet.md`](../finalization/interview-cheat-sheet.md)
## 1. Why this file exists
It turns current Silver relations into documented-dimensional analytical Gold models.
## 2. Where it sits in the architecture
Staging/intermediate -> dimensions/facts -> customer-value and daily-sales marts.
## 3. Inputs / outputs / state
Current Silver-derived dbt refs produce table dimensions/facts/marts. `fct_orders` has incremental target state; other models rebuild tables.
## 4. Important symbols
Declared `Grain` comments, surrogate keys, Type 1 dimensions, `unique_key='order_id'`, merge, delete post-hook, warning relationship.
## 5. Execution flow
Dimensions project current entities; date dimension unions observed order/payment dates. Orders fact enriches customer/date; payments fact enriches order context. Daily sales and customer value aggregate facts.
## 6. Function-by-function walkthrough
`dim_customers`/`dim_products` use deterministic surrogate keys and Type 1 current-state attributes. `dim_locations` deliberately does not fabricate an order join because source orders lack `location_id`. `fct_orders` merges by order ID and its post-hook deletes targets absent from current enriched source, making source deletes visible. `fct_payments` intentionally rebuilds so deletes/relinks propagate. `mart_daily_sales` groups by order date and source currency, retaining missing-rate count; customer value excludes orders without current customer only in that customer mart.
## 7. Critical code-block reasoning
Incremental merge alone does not remove deleted source rows; the `fct_orders` post-hook addresses that current-state gap. Payments use full rebuild because relationship/delete correctness is clearer than a partial incremental rule. Schema relationship for payment->order is `warn`, preserving orphan evidence rather than turning it into a hard failure.
## 8. Correctness invariants
- Every model declares grain.
- No unsupported location fact join.
- One current order fact per order ID.
- Gold retains missing-FX and orphan evidence.
## 9. Failure behavior
dbt tests catch uniqueness/not-null/accepted values; relationship warning surfaces orphan payments. Broken SQL, merge, or post-hook fails runtime rather than silently succeeding.
## 10. Tests that protect the behavior
Static contract tests require dimension/fact/mart grain, incremental order delete synchronization, full-rebuild payments, currency evidence, and warning relationship. **MDEP OFFLINE TESTED.**
## 11. What is not implemented / runtime deferred
**MDEP RUNTIME DEFERRED:** dbt materialization, post-hook execution, Snowflake merge/delete, source freshness, and actual dbt tests.
## 12. Production concepts beyond current code
**GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED:** SCD Type 2, semantic layer/metrics, CI selection, exposures, and operational ownership of orphan remediation.
## 13. Common misunderstandings
Incremental merge does not automatically process deletes. A warning relationship is intentional evidence retention, not a missing test. Location dimension does not prove orders have locations.
## 14. Interview questions
**Why rebuild payments but incrementally merge orders?** Payments need simple correctness for delete/relink propagation; orders demonstrate a merge plus explicit delete synchronization. Materialization follows semantics, not fashion.
## 15. 30-second spoken explanation
“Gold models are current-state dimensions, order/payment facts, and two marts with declared grain. The key correctness choices are explicit order-delete synchronization, payment full rebuild for relationship correctness, no fabricated location join, and visible missing-FX/orphan evidence.”
## 16. Senior follow-up discussion
Discuss when payment full rebuild stops scaling: quantify volume/SLAs, then design a CDC-aware incremental/delete/relink strategy with reconciliation before changing materialization.
