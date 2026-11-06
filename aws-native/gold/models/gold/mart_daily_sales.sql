-- Grain: one order date and source currency; currencies are never mixed.
select
  order_date,
  source_currency,
  count(*) as order_count,
  count(distinct customer_id) as source_customer_count,
  cast(sum(source_order_total) as decimal(18,2)) as source_gross_sales,
  cast(sum(order_total_dkk) as decimal(18,2)) as converted_sales_dkk,
  sum(case when missing_dkk_rate then 1 else 0 end) as missing_rate_order_count,
  sum(case when orphan_customer then 1 else 0 end) as orphan_customer_order_count
from {{ ref('fct_orders') }}
group by order_date, source_currency
