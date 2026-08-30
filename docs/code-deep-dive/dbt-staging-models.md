# Code Deep-Dive: dbt staging models
**Source of truth:** [`analytics/dbt/models/staging/`](../../analytics/dbt/models/staging/) and [`sources.yml`](../../analytics/dbt/models/sources.yml).
## Read beside
- **Source:** [`staging`](../../analytics/dbt/models/staging/), [`sources.yml`](../../analytics/dbt/models/sources.yml)
- **Tests:** [`tests/test_mdep12_warehouse_contract.py`](../../tests/test_mdep12_warehouse_contract.py)
- **Architecture:** [`docs/finalization/data-model-and-grain.md`](../finalization/data-model-and-grain.md)
- **Interview topics:** [`dbt-gold-models.md`](dbt-gold-models.md)
## 1. Why this file exists
Staging creates explicit, minimal dbt projections over the six approved external Silver sources.
## 2. Where it sits in the architecture
External Silver -> `stg_*` -> intermediate -> Gold dimensions/facts/marts.
## 3. Inputs / outputs / state
Inputs are six `MDEP.SILVER_EXT` sources; outputs are six staging relations. State is inherited current-state Silver, not recreated here.
## 4. Important symbols
`source('silver_ext', ...)`, `loaded_at_field`, CDC `applied_at`, reference `ingested_at`.
## 5. Execution flow
dbt resolves source declarations, projects named columns, casts order/payment timestamps to dates, and exposes consistent model references.
## 6. Function-by-function walkthrough
There are no functions: customers/products/orders/payments project CDC fields plus `source_lsn`/`applied_at`; exchange rates/locations project reference freshness fields. `sources.yml` uses `try_to_timestamp_tz(applied_at)` for CDC freshness but `ingested_at` for batch references, reflecting different source evidence.
## 7. Critical code-block reasoning
The models do not select Bronze. That keeps dbt on the approved Silver boundary. Date casts deliberately support analytical grain but preserve source timestamps in Silver for processing correctness.
## 8. Correctness invariants
- Exactly six approved Silver sources.
- CDC and reference freshness fields remain distinct.
- Staging has no fabricated location-to-order relationship.
## 9. Failure behavior
Missing external source/column or bad connection fails dbt compilation/run; freshness warnings/errors are configured but unexecuted.
## 10. Tests that protect the behavior
Warehouse contract tests assert source scope and freshness field choices. **MDEP OFFLINE TESTED.**
## 11. What is not implemented / runtime deferred
**MDEP RUNTIME DEFERRED:** dbt parse/compile/run/test/source freshness against Snowflake.
## 12. Production concepts beyond current code
**GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED:** source SLAs by domain, contracts enforcement, exposures, and environment database overrides.
## 13. Common misunderstandings
Staging is not raw ingestion; it is a controlled analytical interface. Freshness configuration is not observed freshness evidence.
## 14. Interview questions
**Why use different loaded-at fields?** CDC current-state tables expose pipeline `applied_at`; batch references expose `ingested_at`. Forcing one clock would misrepresent their source semantics.
## 15. 30-second spoken explanation
“The staging layer is intentionally thin: six explicit external Silver sources, selected columns, and source-appropriate freshness clocks. It gives dbt a stable analytical boundary without reaching back into Bronze.”
## 16. Senior follow-up discussion
Discuss what happens when a Silver schema adds a field: external registration, staging projection, schema tests, and downstream adoption should be reviewed independently.
