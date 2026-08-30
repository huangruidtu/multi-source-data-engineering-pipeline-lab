{{ config(materialized='table') }}
-- Grain: one current Silver payment. It is intentionally rebuilt so source
-- deletes and an upstream order relink/delete are reflected every dbt run.
select p.payment_id, p.order_id, o.customer_sk, p.customer_id, p.payment_date,
       p.payment_status, p.currency, p.amount, p.authorization_code, p.updated_at, p.applied_at
from {{ ref('int_payments_enriched') }} p
left join {{ ref('fct_orders') }} o on p.order_id = o.order_id
