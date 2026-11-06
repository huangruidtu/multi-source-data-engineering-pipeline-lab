with orders as (
  select * from {{ ref('stg_orders') }}
),
rates as (
  select
    rate_date,
    base_currency,
    rate,
    retrieved_at,
    source_extract_ts as fx_source_extract_ts,
    ingested_at as fx_ingested_at,
    record_hash as fx_record_hash
  from {{ ref('stg_exchange_rates') }}
  where quote_currency = 'DKK' and rate > 0
),
customers as (
  select customer_id from {{ ref('stg_customers') }}
)
select
  o.*,
  r.rate as dkk_rate,
  r.retrieved_at as fx_retrieved_at,
  r.fx_source_extract_ts,
  r.fx_ingested_at,
  r.fx_record_hash,
  {{ amount_in_dkk('o.order_total', 'o.currency', 'r.rate') }} as order_total_dkk,
  case when o.currency <> 'DKK' and r.rate is null then true else false end as missing_dkk_rate,
  case when c.customer_id is null then true else false end as orphan_customer
from orders o
left join rates r
  on o.order_date = r.rate_date
 and o.currency = r.base_currency
left join customers c
  on o.customer_id = c.customer_id
