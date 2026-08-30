# MDEP-12 learning notes

| Concept | MDEP-12 use | Failure / production implication |
| --- | --- | --- |
| Warehouse vs lakehouse | Snowflake Gold consumes external Iceberg Silver | Moving Silver into warehouse tables would duplicate ownership and snapshots. |
| External volume/catalog integration | Gives Snowflake controlled S3 metadata/data access | Wrong IAM trust/path produces inaccessible or stale Iceberg reads. |
| dbt source/ref | Sources are Silver; refs build the DAG | Using raw relation names hides lineage and ordering. |
| Dimension/fact/grain | Dimensions are current entities; facts are one row per order/payment | Undefined grain causes duplicated measures. |
| Surrogate key | Stable hash of business key for dimension joins | Source-key changes/history need explicit migration policy. |
| Type 1 | Current customer/product state | It cannot answer historical-attribute questions; Type 2 needs trustworthy history. |
| Incremental merge | `fct_orders`/payments avoid full rebuild | Late updates need a predicate; hard deletes need explicit reconciliation. |
| Freshness/tests | dbt checks source timestamp, keys, statuses, relationships | Tests detect symptoms, not source-system repair. |
| Currency conversion | Order-date source-to-DKK join | Missing rate stays null/flagged; do not invent FX. |
| Micro-partitions/cost | XSMALL auto-suspend, predicate pruning, no lab clustering | Inspect query profile before sizing/clustering in production. |

Late dimensions create null surrogate keys in current facts and are visible to relationship tests. Current-state Silver deletes are reflected by the `fct_orders` anti-join post-hook on successful incremental runs; a full refresh is the recovery/reset path. Gold preserves no independent history in V1.
