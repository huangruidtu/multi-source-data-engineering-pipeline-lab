with expected as (
  select
    order_date,
    source_currency,
    count(*) as order_count,
    cast(sum(source_order_total) as decimal(18,2)) as source_gross_sales,
    cast(sum(order_total_dkk) as decimal(18,2)) as converted_sales_dkk,
    sum(case when missing_dkk_rate then 1 else 0 end) as missing_rate_order_count
  from {{ ref('fct_orders') }}
  group by order_date, source_currency
),
actual as (
  select * from {{ ref('mart_daily_sales') }}
)
select
  coalesce(e.order_date, a.order_date) as order_date,
  coalesce(e.source_currency, a.source_currency) as source_currency
from expected e
full outer join actual a
  on e.order_date = a.order_date and e.source_currency = a.source_currency
where e.order_date is null
   or a.order_date is null
   or e.order_count <> a.order_count
   or e.source_gross_sales <> a.source_gross_sales
   or coalesce(e.converted_sales_dkk, -1) <> coalesce(a.converted_sales_dkk, -1)
   or e.missing_rate_order_count <> a.missing_rate_order_count
