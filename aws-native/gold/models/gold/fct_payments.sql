-- Grain: one current payment. Orphan order evidence is retained.
select
  p.payment_id,
  o.order_id as matched_order_id,
  p.order_id as source_order_id,
  o.customer_sk,
  d.date_sk,
  p.payment_date,
  p.payment_ts,
  p.payment_status,
  p.currency as source_currency,
  cast(p.amount as decimal(18,2)) as source_amount,
  p.authorization_code,
  p.orphan_order,
  p.updated_at as source_updated_at,
  p.source_lsn,
  p.applied_at as source_applied_at,
  p.ingested_at as source_ingested_at,
  p.record_hash as source_record_hash
from {{ ref('int_payments_enriched') }} p
left join {{ ref('fct_orders') }} o on p.order_id = o.order_id
left join {{ ref('dim_date') }} d on p.payment_date = d.date_day
