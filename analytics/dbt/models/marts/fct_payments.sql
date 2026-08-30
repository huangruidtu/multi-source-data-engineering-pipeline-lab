{{ config(materialized='incremental', unique_key='payment_id', incremental_strategy='merge') }}
-- Grain: one current Silver payment; orders missing in Silver remain as orphan facts for diagnosis.
select p.payment_id, p.order_id, o.customer_sk, p.customer_id, p.payment_date,
       p.payment_status, p.currency, p.amount, p.authorization_code, p.updated_at, p.applied_at
from {{ ref('int_payments_enriched') }} p
left join {{ ref('fct_orders') }} o on p.order_id = o.order_id
{% if is_incremental() %}
where p.applied_at >= (select coalesce(max(applied_at), '1900-01-01') from {{ this }})
   or p.updated_at >= (select coalesce(max(updated_at), '1900-01-01') from {{ this }})
{% endif %}
