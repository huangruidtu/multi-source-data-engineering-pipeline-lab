{{ config(materialized='table') }}
-- Grain: one current customer; orders without a current customer are retained in fct_orders but excluded here.
select customer_sk, customer_id, count(*) as order_count, sum(order_total) as total_spend,
       avg(order_total) as avg_order_value, max(order_date) as last_order_date
from {{ ref('fct_orders') }}
where customer_sk is not null
group by 1, 2
