-- Grain: one source customer identifier, including identifiers absent from dim_customers.
select
  customer_id,
  max(customer_sk) as customer_sk,
  count(*) as order_count,
  cast(sum(order_total_dkk) as decimal(18,2)) as known_value_dkk,
  sum(case when missing_dkk_rate then 1 else 0 end) as missing_rate_order_count,
  max(order_date) as last_order_date,
  max(case when orphan_customer then 1 else 0 end) as orphan_customer_flag
from {{ ref('fct_orders') }}
group by customer_id
