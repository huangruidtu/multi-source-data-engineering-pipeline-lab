# Solutions — 06 Snowflake and dbt analytics workbook

Read these only after attempting [the workbook](../06-snowflake-dbt.md). The answers describe checked-in code and static contracts, not a completed Snowflake/dbt runtime.

## SD-01 — Silver ownership and external Iceberg architecture

**Correct answer:** Reject the proposal as a change to MDEP’s established ownership boundary, not as an assertion that Snowflake-managed copies are universally impossible. Spark/Flink own canonical Silver Iceberg: Spark materializes batch-reference Silver and Flink materializes CDC current-state Silver. `01_setup.sql` establishes Snowflake-side access objects: an external volume, an object-store Iceberg catalog integration, six registrations under `MDEP.SILVER_EXT`, and grants for `MDEP_TRANSFORMER`. dbt reads those source declarations, shapes them in staging/intermediate, and creates native Gold outputs in `MDEP.GOLD`. Snowflake/dbt is not a second Silver writer.

**Step-by-step model/data trace:**

```text
batch / CDC processors -> canonical Silver Iceberg
                         -> MDEP.SILVER_EXT external registrations
                         -> dbt sources, staging views, intermediate views
                         -> MDEP.GOLD dimensions, facts, marts
```

Batch-reference and CDC current-state data differ in upstream ownership and freshness evidence, but both converge at externally managed Silver; neither becomes dbt-owned merely because dbt queries it. A copied managed Silver table would introduce a second representation with a separate refresh policy, lag, failure state, lineage, and a question of which copy is canonical during disagreement.

**Correctness invariant:** Each dataset has one canonical Silver writer and one canonical Silver representation for downstream consumption; Snowflake access does not silently become Silver ownership.

**Common wrong answer:** “External Iceberg means Snowflake owns or writes the Iceberg tables,” or “a managed copy is automatically safer.” External access is not ownership, while a copy may be reasonable in a different architecture only with explicit synchronization and ownership design.

**Production consequence:** An ungoverned duplicate can present stale or inconsistent state to Gold and make incident reconciliation ambiguous.

**Suggested regression/contract test:** Retain the contract that setup contains six external Iceberg registrations and an object-store catalog, and `sources.yml` names only six approved Silver tables. A later runtime test would need to read the external registrations, compare agreed business keys/freshness with canonical Iceberg, and capture cross-engine evidence; that is `MDEP RUNTIME DEFERRED`.

**Interview-ready English answer:** “In this design Spark and Flink own canonical Silver Iceberg, Snowflake exposes it externally, and dbt owns Gold. Copying Silver into a managed Snowflake table is possible in another design, but here it creates a second representation with refresh, lag, and ownership questions. I keep one canonical Silver writer and make any replication an explicit architecture decision.”

**Senior follow-up discussion:** Define an SLO for a deliberately introduced replica: producer ownership, refresh mechanism, watermark, reconciliation, backfill/deletion propagation, and which consumer must use which representation. No such managed replica is implemented in MDEP. `MDEP IMPLEMENTED` is the SQL/model intent; `MDEP OFFLINE TESTED` is its static contract; external access is `MDEP RUNTIME DEFERRED`.

## SD-02 — Incremental fact delete synchronization

**Correct answer:** Qualify/reject the claim. dbt incremental merge upserts the selected source rows by `order_id`; it does not infer that a target row should disappear merely because no matching source row was selected. The `fct_orders` model selects rows on an incremental run when either `o.applied_at` is at least the target maximum `applied_at` or `o.updated_at` is at least the target maximum `updated_at`. This controls update/late-arrival candidate selection, not delete propagation.

**Step-by-step model/data trace:** For `O-77`, an upstream physical delete means `int_orders_enriched` no longer returns the key. It cannot appear in the merge source, so merge alone leaves an existing target `O-77` untouched. The configured post-hook then runs `delete from {{ this }} as target where not exists (...)` against `int_orders_enriched`, removes the stale target row, and makes current-state Gold reflect the source absence. If the post-hook is removed, `O-77` can remain a stale Gold fact even though current Silver no longer has it.

`fct_payments` makes a different, intentional choice: it is `materialized='table'`, so each run rebuilds from `int_payments_enriched` and left joins the current `fct_orders`. At the current project scale, a rebuild makes payment deletion and order relink/delete behavior easier to reason about than a second incremental/delete contract.

**Relevant SQL/dbt logic:** The precise incremental predicate is the two-clause `applied_at`/`updated_at` condition in `fct_orders.sql`; its post-hook is the separate anti-join delete. `unique_key='order_id'`, the delete statement, and non-incremental payment materialization are checked by `test_mdep12_warehouse_contract.py`.

**Correctness invariant:** Current-state Gold must not retain an order absent from its authoritative enriched current-state input after the configured model lifecycle completes.

**Common wrong answer:** “A merge removes unmatched target rows,” or “source freshness detects deletions.” Freshness describes recency evidence; it does not execute an anti-join delete.

**Production consequence:** Omitted delete synchronization creates phantom orders, incorrect sales aggregates, and broken joins long after the source deletion.

**Suggested regression/contract test:** Statically assert that `fct_orders` retains `unique_key='order_id'`, both predicate clocks, and the `delete from {{ this }}` post-hook; assert payments remains a table. Later, a runtime acceptance test should seed `O-77`, build Gold, delete it from authoritative Silver, rerun dbt, and prove target absence. That physical execution is `MDEP RUNTIME DEFERRED`.

**Interview-ready English answer:** “An incremental merge is an upsert mechanism for rows present in its selected source; it is not delete synchronization. Our orders fact uses an explicit anti-join post-hook to remove target keys absent from the enriched current state. Payments intentionally rebuild, because at this scale a full rebuild is the clearer way to preserve delete and relink correctness.”

**Senior follow-up discussion:** At larger scale, assess anti-join scan cost, deletion markers/change data, partitioning, transaction behavior, observability, and reconciliation. “Incremental” is a performance mechanism, not a maturity score. The SQL is `MDEP IMPLEMENTED` and its static contract is `MDEP OFFLINE TESTED`; dbt execution is `MDEP RUNTIME DEFERRED`.

## SD-03 — Missing FX and orphan-payment trace

**Correct answer:** Preserve both conditions as evidence. `stg_orders` casts `order_ts` to `order_date` and retains currency and total. For `O-200`, `int_orders_enriched` left joins a rate only when the date matches, the order currency matches `base_currency`, and the rate has `quote_currency = 'DKK'`. No EUR/DKK row makes `r.rate`/`dkk_rate` null. `safe_currency_conversion` therefore returns null for `order_total_dkk`, while `missing_dkk_rate` is true. The order remains in `fct_orders`; `mart_daily_sales` keeps its source-currency grain and increments `missing_rate_order_count` rather than treating the unknown conversion as zero.

For `P-900`, `int_payments_enriched` left joins the missing order, so the payment remains present with order-derived fields null. `fct_payments` then left joins the missing `fct_orders` row, leaving `customer_sk` null while retaining the payment and its `order_id='O-404'`. The configured relationship test on `fct_payments.order_id` targets `fct_orders.order_id` with warning severity: it detects the broken relationship as a data-quality signal, but it neither repairs nor physically deletes the payment.

**Relevant SQL/dbt logic:** `safe_currency_conversion` returns the input only for DKK and multiplies only by a non-null rate greater than zero. This guards against accidental zero/negative-rate arithmetic. `int_orders_enriched` uses `missing_dkk_rate`; `int_payments_enriched` and `fct_payments` use left joins. `schema.yml` specifies the warn-level payment relationship. The project also contains singular `positive_amounts.sql`; it concerns positive amount data quality, not an FX-rate substitute or orphan-row deletion mechanism.

**Correctness invariant:** Unknown conversion and broken referential integrity must remain visible as unknown/broken evidence; the pipeline must not convert them into apparently valid zero amounts or silently discard the rows.

**Common wrong answer:** Inner join away `O-200`/`P-900`, set converted sales to zero, or say a dbt relationship test deletes the orphan. Those actions hide uncertainty or confuse validation with mutation.

**Production consequence:** Zeroing missing FX understates revenue and dropping orphan payments hides operational data problems; both make reconciliation and incident investigation harder.

**Suggested regression/contract tests:** Assert the model keeps `quote_currency = 'DKK'`, `missing_dkk_rate`, and the safe-conversion macro; assert the payment relationship test remains warning severity and model joins remain left joins. A later runtime scenario can load these rows and inspect Gold/mart results, but that is `MDEP RUNTIME DEFERRED`.

**Historical correctness note:** The former `GOLD_GOLD` risk was dbt schema resolution: a model-level `schema: gold` combined with profile schema `GOLD` could produce an unintended suffix. The current project removes the model-level custom schema override and lets `profiles.yml.example` resolve the target as `MDEP.GOLD`. This is a historical configuration-resolution risk, not evidence that `01_setup.sql` contained a literal `GOLD_GOLD` typo or that the faulty configuration ran in production.

**Interview-ready English answer:** “When an EUR order lacks its DKK rate, I keep the order, set the converted amount to null, and expose a missing-rate flag; I never turn unknown into zero. Likewise, payment enrichment uses left joins so an orphan remains visible, while a warning-level relationship test signals the broken link without deleting evidence. That separates data preservation from data-quality signalling.”

**Senior follow-up discussion:** In production, define remediation ownership, alert thresholds for missing rates/orphans, exception ageing, and whether a downstream report can consume incomplete conversions. MDEP implements model logic and offline contracts, but Snowflake/dbt execution and cross-engine results remain `MDEP RUNTIME DEFERRED`.
