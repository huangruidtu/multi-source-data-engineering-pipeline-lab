{{ config(
  materialized='incremental',
  unique_key='order_id',
  incremental_strategy='merge',
  on_schema_change='fail'
) }}
-- Grain: one current order. Absence from an ordinary feed never deletes a row.
with candidates as (
  select
    o.order_id,
    c.customer_sk,
    d.date_sk,
    o.customer_id,
    o.order_date,
    o.order_ts,
    o.order_status,
    o.currency as source_currency,
    cast(o.order_total as decimal(18,2)) as source_order_total,
    cast(o.dkk_rate as decimal(20,10)) as dkk_rate,
    cast(o.order_total_dkk as decimal(18,2)) as order_total_dkk,
    o.missing_dkk_rate,
    o.orphan_customer,
    o.fx_retrieved_at,
    o.fx_record_hash,
    o.updated_at as source_updated_at,
    o.source_lsn,
    o.applied_at as source_applied_at,
    o.ingested_at as source_ingested_at,
    o.record_hash as source_record_hash
  from {{ ref('int_orders_enriched') }} o
  left join {{ ref('dim_customers') }} c on o.customer_id = c.customer_id
  left join {{ ref('dim_date') }} d on o.order_date = d.date_day
)
select * from candidates incoming
{% if is_incremental() %}
where not exists (
  select 1
  from {{ this }} current_row
  where current_row.order_id = incoming.order_id
    and (
      current_row.source_updated_at > incoming.source_updated_at
      or (
        current_row.source_updated_at = incoming.source_updated_at
        and current_row.source_applied_at > incoming.source_applied_at
      )
      or (
        current_row.source_updated_at = incoming.source_updated_at
        and current_row.source_applied_at = incoming.source_applied_at
        and coalesce(current_row.fx_retrieved_at, cast('1900-01-01' as timestamp))
            >= coalesce(incoming.fx_retrieved_at, cast('1900-01-01' as timestamp))
      )
    )
)
{% endif %}
