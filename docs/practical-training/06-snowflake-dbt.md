# 06 — Snowflake and dbt analytics
Read: `warehouse/snowflake/01_setup.sql`, `analytics/dbt/models/`, `tests/test_mdep12_warehouse_contract.py`.

## SD-01 — ARCHITECTURE
**Difficulty:** Intermediate. **Scenario:** reviewer proposes copying Silver into Snowflake-managed tables before dbt. **Deliverable:** reject/accept with ownership boundary and external Iceberg rationale. **Competency:** layer ownership.
## SD-02 — CODE REVIEW
**Difficulty:** Senior. **Claim:** incremental `fct_orders` merge automatically removes deleted Silver orders. **Deliverable:** identify gap, explain post-hook, and compare intentional full rebuild for payments. **Competency:** current-state Gold correctness.
## SD-03 — TRACE
**Difficulty:** Intermediate. **Scenario:** order is EUR, matching DKK rate missing; a payment references missing order. **Deliverable:** trace `missing_dkk_rate`, left joins, Gold facts/marts, and relationship-test severity. **Competency:** preserve uncertainty.
