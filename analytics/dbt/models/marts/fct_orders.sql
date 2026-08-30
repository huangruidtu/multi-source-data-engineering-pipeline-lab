{{ config(
  materialized='incremental', unique_key='order_id', incremental_strategy='merge',
  on_schema_change='append_new_columns',
  post_hook="delete from {{ this }} as target where not exists (select 1 from " ~ ref('int_orders_enriched') ~ " as source where source.order_id = target.order_id)"
) }}
-- Grain: one current Silver order. Merge handles updates/late arrivals; post-hook
-- makes physical Silver deletes visible in current-state Gold.
select o.order_id, c.customer_sk, o.customer_id, d.date_day as order_date,
       o.order_status, o.currency, o.order_total, o.order_total_dkk,
       o.missing_dkk_rate, o.updated_at, o.applied_at
from {{ ref('int_orders_enriched') }} o
left join {{ ref('dim_customers') }} c on o.customer_id = c.customer_id
left join {{ ref('dim_date') }} d on o.order_date = d.date_day
{% if is_incremental() %}
where o.applied_at >= (select coalesce(max(applied_at), '1900-01-01') from {{ this }})
   or o.updated_at >= (select coalesce(max(updated_at), '1900-01-01') from {{ this }})
{% endif %}
