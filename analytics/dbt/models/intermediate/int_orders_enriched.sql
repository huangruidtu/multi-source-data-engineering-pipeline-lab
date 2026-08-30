with orders as (select * from {{ ref('stg_orders') }}),
rates as (
  select rate_date, base_currency, rate
  from {{ ref('stg_exchange_rates') }}
  where quote_currency = 'DKK'
)
select o.*, r.rate as dkk_rate,
       {{ safe_currency_conversion('o.order_total', 'o.currency', 'r.rate') }} as order_total_dkk,
       case when o.currency <> 'DKK' and r.rate is null then true else false end as missing_dkk_rate
from orders o
left join rates r on o.order_date = r.rate_date and o.currency = r.base_currency
