# Code Deep-Dive: dbt intermediate models
**Source of truth:** [`analytics/dbt/models/intermediate/`](../../analytics/dbt/models/intermediate/) and [`safe_currency_conversion.sql`](../../analytics/dbt/macros/safe_currency_conversion.sql).
## Read beside
- **Source:** [`intermediate`](../../analytics/dbt/models/intermediate/)
- **Tests:** [`tests/test_mdep12_warehouse_contract.py`](../../tests/test_mdep12_warehouse_contract.py)
- **Architecture:** [`docs/finalization/key-design-decisions.md`](../finalization/key-design-decisions.md)
- **Interview topics:** [`dbt-gold-models.md`](dbt-gold-models.md)
## 1. Why this file exists
It centralizes analytical enrichment before facts: DKK conversion for orders and optional order context for payments.
## 2. Where it sits in the architecture
Staging inputs feed intermediate relations; Gold facts consume them.
## 3. Inputs / outputs / state
Orders plus exchange rates yield `int_orders_enriched`; payments plus orders yield `int_payments_enriched`. No independent persistence/state contract is created here.
## 4. Important symbols
`safe_currency_conversion`, `quote_currency = 'DKK'`, `missing_dkk_rate`, left joins.
## 5. Execution flow
Orders join same-date/base-currency DKK rates; macro computes converted amount only when safe. Payments left-join orders to retain relational context or orphan evidence.
## 6. Function-by-function walkthrough
`int_orders_enriched` filters rates to DKK quote, matches date and source currency, keeps original order, exposes `dkk_rate`, conversion, and boolean missing-rate evidence. `int_payments_enriched` left joins rather than drops payments whose order is unavailable. The macro protects conversion semantics rather than assuming every rate exists.
## 7. Critical code-block reasoning
The left join plus `missing_dkk_rate` means missing conversion remains visible, not zeroed or silently excluded. Payment left join preserves potential orphans for a warning-level relationship check; analytics can distinguish absent dimension/fact relation from disappeared data.
## 8. Correctness invariants
- DKK rate joins use rate date and base currency.
- Missing non-DKK rate is an explicit fact.
- Payments survive missing orders.
## 9. Failure behavior
Missing/duplicate rates can produce null conversion/evidence and must be reconciled; SQL does not invent FX. Runtime query failures remain failures.
## 10. Tests that protect the behavior
Contract tests assert DKK quote filter and `missing_dkk_rate`. **MDEP OFFLINE TESTED.**
## 11. What is not implemented / runtime deferred
**MDEP RUNTIME DEFERRED:** dbt execution, rate data distribution, and actual source relationship results.
## 12. Production concepts beyond current code
**GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED:** rate-source hierarchy, calendar fallback, historical FX restatement, and anomaly alerting.
## 13. Common misunderstandings
Null converted value is not zero revenue. An orphan payment is not automatically invalid; it can be meaningful reconciliation evidence.
## 14. Interview questions
**Why keep missing FX visible?** Replacing it with zero corrupts analysis; an explicit boolean preserves fact amount and exposes remediation work.
## 15. 30-second spoken explanation
“Intermediate dbt models add only reusable analytical context: safe DKK conversion with a visible missing-rate flag, and payment-to-order enrichment that preserves orphans. They make uncertainty observable before Gold modeling.”
## 16. Senior follow-up discussion
Explain how finance would define rate precedence and restatement policy before converting a missing-rate flag into a business default.
