{{ config(materialized='table') }}
-- Grain: one order_date + source currency, including missing conversion evidence.
select order_date, currency, count(*) as order_count, count(distinct customer_id) as customer_count,
       sum(order_total) as gross_sales, sum(order_total_dkk) as converted_sales_dkk,
       count_if(missing_dkk_rate) as missing_rate_order_count
from {{ ref('fct_orders') }}
group by 1, 2
