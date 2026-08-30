# 06 — Snowflake and dbt analytics workbook

Attempt this file before [the matching solutions](solutions/06-snowflake-dbt-solutions.md). Record work in a copy of [the session template](training-records/06-snowflake-dbt-session-template.md). This workbook is based on checked-in SQL, dbt models, and contract tests; it is not evidence of a physical Snowflake/dbt run.

## SD-01 — Silver ownership and external Iceberg architecture

- **Difficulty:** Intermediate
- **Task type:** ARCHITECTURE REVIEW / DATA-OWNERSHIP TRACE / INTERVIEW EXPLANATION
- **Source files/models to inspect:** `warehouse/snowflake/01_setup.sql`; `analytics/dbt/profiles.yml.example`; `analytics/dbt/models/sources.yml`; `analytics/dbt/models/staging/`; `analytics/dbt/models/intermediate/`; `analytics/dbt/models/marts/`; `tests/test_mdep12_warehouse_contract.py`; [Snowflake/Iceberg deep-dive](../code-deep-dive/snowflake-iceberg-setup.md) and [dbt Gold deep-dive](../code-deep-dive/dbt-gold-models.md).
- **Scenario:** A reviewer proposes: “Before dbt runs, copy every Silver Iceberg table into a Snowflake-managed table. Then dbt should read that Snowflake copy.” Review this against MDEP rather than a generic preference for external tables.
- **Concrete model facts:** The setup SQL creates `MDEP.SILVER_EXT` and six `CREATE OR REPLACE ICEBERG TABLE` registrations using `MDEP_ICEBERG_VOLUME` and `MDEP_ICEBERG_OBJECT_STORE`; it creates `MDEP.GOLD` separately. `sources.yml` reads exactly six `silver_ext` tables. The profile defaults to database `MDEP`, schema `GOLD`, and role `MDEP_TRANSFORMER`. Spark/Flink own canonical Silver: CDC current state and batch references have different upstream producers, but neither is written by dbt. dbt staging and intermediate models shape/read Silver; marts are Gold outputs.
- **Engineering deliverables:**
  1. Draw and label the current path: `Spark/Flink -> canonical Silver Iceberg -> Snowflake external Iceberg access -> dbt staging/intermediate -> dbt Gold marts`.
  2. State who owns canonical Silver data, Snowflake access objects, and Gold tables. Explicitly answer whether Snowflake/dbt is a second Silver writer.
  3. Explain what the setup SQL establishes (external volume, object-store catalog integration, external Iceberg registrations, role grants) and what it does not establish.
  4. Review the copy proposal: distinguish “technically impossible” from “outside the current architecture,” and name its synchronization, freshness, lineage, and ownership questions.
  5. Explain why one canonical Silver writer per dataset matters. Contrast batch-reference Silver ownership with CDC current-state Silver ownership without assigning either to Snowflake/dbt.
  6. Write one architecture invariant and a 30–60 second interview explanation.
- **Constraints:** Do not claim external Iceberg access, cross-engine reads, or dbt execution was physically run. Use `MDEP IMPLEMENTED` for checked-in SQL/configuration/model intent, `MDEP OFFLINE TESTED` for static warehouse contract tests, and `MDEP RUNTIME DEFERRED` for physical Snowflake/Iceberg integration. A Snowflake-managed copy may be a general production design, but it is **GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED** in MDEP.
- **Competency trained:** ownership boundaries, external-table architecture, and avoiding accidental dual canonical state.
- **Learner workspace/template:**

```text
Canonical Silver owner: ___
Snowflake setup establishes: ___
dbt owns: ___
Second Silver writer? ___
Copy proposal: impossible / possible-but-outside-MDEP because ___
Duplicate-copy risks: ___
CDC versus batch-reference ownership: ___
Invariant: ___
Interview explanation: ___
```

## SD-02 — Incremental fact delete synchronization

- **Difficulty:** Senior
- **Task type:** CODE REVIEW / MODEL TRACE / REGRESSION TEST DESIGN
- **Source files/models to inspect:** `analytics/dbt/models/marts/fct_orders.sql`; `analytics/dbt/models/marts/fct_payments.sql`; `analytics/dbt/models/intermediate/int_orders_enriched.sql`; `analytics/dbt/models/staging/stg_orders.sql`; `analytics/dbt/models/schema.yml`; `tests/test_mdep12_warehouse_contract.py`; [dbt Gold deep-dive](../code-deep-dive/dbt-gold-models.md).
- **Scenario:** A teammate claims: “`fct_orders` uses incremental merge, so when an order disappears from Silver, dbt automatically deletes it from Gold.” Decide whether the claim is accurate using the actual model.
- **Concrete SQL behavior/model facts:** `fct_orders` has `materialized='incremental'`, `unique_key='order_id'`, and `incremental_strategy='merge'`. In an incremental run, its source row selection is:

```sql
where o.applied_at >= (select coalesce(max(applied_at), '1900-01-01') from {{ this }})
   or o.updated_at >= (select coalesce(max(updated_at), '1900-01-01') from {{ this }})
```

Its separate post-hook executes a target delete where no row with the same `order_id` exists in `int_orders_enriched`. `fct_payments` instead has `materialized='table'` and joins to `fct_orders` after each rebuild. Example: target `fct_orders` contains `order_id='O-77'`; a physical upstream delete means `int_orders_enriched` no longer returns `O-77`.
- **Engineering deliverables:**
  1. Explain what merge can do for selected source rows and why it does not, by itself, synchronize absence/deletion.
  2. Describe the actual incremental predicate, including both clocks and the use of `{{ this }}` maxima. Explain why row selection is not target-delete synchronization.
  3. Trace the `O-77` deletion: merge input, post-hook anti-join, target result, and the consequence if the post-hook were omitted.
  4. Explain why `fct_payments` intentionally takes the full-table rebuild route and how that helps payment delete/relink and upstream order changes.
  5. Reject “incremental is always more advanced”; compare correctness, compute cost, and operational complexity.
  6. Propose one static regression/contract test and one runtime acceptance test needed later. State the production consequence and give a 30–60 second interview explanation.
- **Constraints:** Do not rewrite the predicate from memory or call source freshness a delete mechanism. Checked-in SQL is `MDEP IMPLEMENTED`; the contract assertions for `unique_key`, `delete from {{ this }}`, and payment table materialization are `MDEP OFFLINE TESTED`. Actual dbt execution and physical Gold deletion are `MDEP RUNTIME DEFERRED`.
- **Competency trained:** incremental-model semantics, delete correctness, and cost-versus-correctness reasoning.
- **Learner workspace/template:**

```text
Claim verdict: ___
Merge handles: ___
Incremental predicate: ___
Why selection != delete synchronization: ___
O-77 after merge / post-hook: ___
If post-hook is absent: ___
Why payments rebuild: ___
Test: ___
Production consequence: ___
Interview explanation: ___
```

## SD-03 — Missing FX and orphan-payment trace

- **Difficulty:** Intermediate
- **Task type:** DATA TRACE / DATA-QUALITY REVIEW / TEST DESIGN
- **Source files/models to inspect:** `analytics/dbt/models/staging/stg_orders.sql`; `analytics/dbt/models/staging/stg_payments.sql`; `analytics/dbt/models/staging/stg_exchange_rates.sql`; `analytics/dbt/models/intermediate/int_orders_enriched.sql`; `analytics/dbt/models/intermediate/int_payments_enriched.sql`; `analytics/dbt/macros/safe_currency_conversion.sql`; `analytics/dbt/models/marts/fct_orders.sql`; `analytics/dbt/models/marts/fct_payments.sql`; `analytics/dbt/models/marts/mart_daily_sales.sql`; `analytics/dbt/models/schema.yml`; `analytics/dbt/tests/positive_amounts.sql`; [intermediate-model deep-dive](../code-deep-dive/dbt-intermediate-models.md).
- **Scenario:** Two data-quality conditions arrive together. Determine how MDEP preserves evidence instead of fabricating valid-looking analytics.
- **Concrete rows / SQL behavior:**

```text
Silver core_orders:
  order_id=O-200, customer_id=C-9, order_ts=2026-08-30T09:00:00Z,
  currency=EUR, order_total=100.00, order_status=completed,
  updated_at=2026-08-30T09:00:00Z, applied_at=2026-08-30T09:05:00Z

Silver ref_exchange_rates:
  no row where rate_date=2026-08-30, base_currency=EUR, quote_currency=DKK

Silver core_payments:
  payment_id=P-900, order_id=O-404, payment_ts=2026-08-30T10:00:00Z,
  currency=EUR, amount=100.00, payment_status=completed,
  updated_at=2026-08-30T10:00:00Z, applied_at=2026-08-30T10:05:00Z
```

`int_orders_enriched` left joins rates on `order_date = rate_date` and `currency = base_currency`, after filtering rates to `quote_currency = 'DKK'`. `safe_currency_conversion` returns the original amount for DKK, multiplies only by a positive non-null rate, and otherwise returns null. `int_payments_enriched` left joins payments to orders on `order_id`. `fct_payments` left joins to `fct_orders`; the `fct_payments.order_id` relationship test has `severity: warn`.
- **Engineering deliverables:**
  1. Trace `O-200` from Silver through staging, `int_orders_enriched`, `fct_orders`, and `mart_daily_sales`: join outcome, `dkk_rate`, `order_total_dkk`, `missing_dkk_rate`, fact survival, and aggregate consequence.
  2. Explain why zero conversion, an inner rate join, or silently dropping the EUR order violates the intended semantics. Include why safe conversion needs a positive rate.
  3. Trace `P-900` through staging, `int_payments_enriched`, and `fct_payments`: join type, preserved evidence, and likely null dimensional field when `O-404` is absent from `fct_orders`.
  4. Explain exactly what the warning-level relationship test detects, what it does not do, and whether a dbt relationship test physically deletes a row.
  5. Distinguish data preservation from data-quality signalling. Propose one regression/contract test for each condition, state one production interpretation, and provide a 30–60 second interview explanation.
  6. Add a compact historical correctness note: explain the former `GOLD_GOLD` schema-resolution risk from combining a model-level `schema: gold` override with profile schema `GOLD`, and why the current project lets the profile resolve `MDEP.GOLD`. Do not claim setup SQL had a literal `GOLD_GOLD` typo.
- **Constraints:** Do not claim dbt relationship tests repair data, real FX data was read, or Snowflake execution occurred. Model logic is `MDEP IMPLEMENTED`; static source/model/schema contracts are `MDEP OFFLINE TESTED`; physical source-to-Gold results are `MDEP RUNTIME DEFERRED`.
- **Competency trained:** preserving uncertainty, join semantics, quality signalling, grain-aware analytics, and schema-resolution reasoning.
- **Learner workspace/template:**

```text
O-200 rate join/result: ___
O-200 in fact/mart: ___
Why preserve missing FX: ___
P-900 join/result: ___
Warning relationship test means / does not mean: ___
Preservation versus signalling: ___
Two tests: ___
Historical schema note: ___
Interview explanation: ___
```
