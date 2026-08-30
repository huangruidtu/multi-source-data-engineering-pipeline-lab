# Data Model and Grain

## Dataset map

| Entity | Source / key | Important timestamps and mutation | Silver owner / grain | Gold destination / grain |
| --- | --- | --- | --- | --- |
| customers | PostgreSQL; `customer_id` | `updated_at`; CDC create/update/delete | Flink; one current customer per ID | `dim_customers`; one row per current customer, surrogate key |
| products | PostgreSQL; `product_id` | `updated_at`; CDC create/update/delete | Flink; one current product per ID | `dim_products`; one row per current product |
| orders | PostgreSQL; `order_id`, `customer_id` FK | `order_ts`, `updated_at`; CDC create/update/delete | Flink; one current order per ID | `fct_orders`; one current order per ID |
| payments | PostgreSQL; `payment_id`, `order_id` FK | `payment_ts`, `updated_at`; CDC create/update/delete | Flink; one current payment per ID | `fct_payments`; one current payment per ID |
| locations | REST/files; `location_id` | `updated_at`; batch reference revision | Spark; one current location per ID | `dim_locations`; one current location |
| exchange rates | REST; `(rate_date, base_currency, quote_currency)` | `retrieved_at`; batch reference revision | Spark; one current rate version per natural key | `int_orders_enriched`; rate used for DKK conversion |

## Keys, grains, and relationships

A **primary key** is a source table's uniqueness constraint: `order_id` for
orders. A **business/natural key** is the meaningful identity used across a
pipeline: the same `order_id`, or the three-field exchange-rate key. A
**surrogate key** is warehouse-specific: dbt dimensions expose keys such as
`customer_sk` so facts can join a stable dimensional representation.

The **Silver grain** is current state, not event history. CDC Bronze retains
history; Flink Silver retains one accepted version per entity/key. Batch Silver
retains one accepted reference record per documented key. Gold fact grain is
declared in each dbt model, not inferred from a join.

Relationships available from source code are:

```text
customers 1 <- many orders
orders    1 <- many payments
orders    many -> one dim_date (order date)
orders    many -> one exchange rate (order date + currency)
```

Orders do **not** contain `location_id`. Therefore MDEP intentionally has no
fabricated order-to-location fact join. This is a data-model limitation to state
in an interview, not a missing SQL trick.

## Quality and temporal rules

PostgreSQL enforces non-null keys/amounts and foreign keys for orders/payments.
Spark validates reference attributes and quarantines bad data. Flink validates
CDC envelopes, rejects invalid delete shape, and controls state with LSN and
transaction ordering. dbt's relationship quality rule for payment/order is
warning-oriented: an orphan payment is surfaced rather than silently dropped.

The project is current-state oriented. It does not implement a full history/SCD2
dimension model in V1. A late-arriving customer or missing exchange rate remains
visible through nullable/quality indicators and reconciliation, rather than being
invented or discarded. Type 1 current dimensions are the bounded V1 decision;
history-preserving dimension strategy is a production extension.
