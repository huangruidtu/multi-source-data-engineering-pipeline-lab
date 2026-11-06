-- Current-state reconciliation; mutation counts are emitted by the integration harness.
with source_metrics as (
  select
    count(*) as source_key_count,
    count(distinct order_id) as source_distinct_key_count,
    cast(sum(case when currency = 'DKK' then order_total else 0 end) as decimal(18,2)) as source_dkk_total,
    cast(sum(case when currency = 'EUR' then order_total else 0 end) as decimal(18,2)) as source_eur_total
  from {{ ref('stg_orders') }}
),
gold_metrics as (
  select
    count(*) as gold_key_count,
    count(distinct order_id) as gold_distinct_key_count,
    sum(case when orphan_customer then 1 else 0 end) as orphan_customer_count,
    sum(case when missing_dkk_rate then 1 else 0 end) as missing_fx_count,
    cast(sum(order_total_dkk) as decimal(18,2)) as known_converted_total_dkk
  from {{ ref('fct_orders') }}
)
select
  s.source_key_count,
  s.source_distinct_key_count,
  g.gold_key_count,
  g.gold_distinct_key_count,
  s.source_key_count - s.source_distinct_key_count as source_duplicate_count,
  g.gold_key_count - g.gold_distinct_key_count as gold_duplicate_count,
  g.orphan_customer_count,
  g.missing_fx_count,
  s.source_dkk_total,
  s.source_eur_total,
  g.known_converted_total_dkk
from source_metrics s
cross join gold_metrics g
