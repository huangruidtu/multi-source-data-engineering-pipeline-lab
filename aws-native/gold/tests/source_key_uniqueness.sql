select 'customers' as entity, customer_id as business_key, count(*) as row_count
from {{ ref('stg_customers') }} group by customer_id having count(*) <> 1
union all
select 'products', product_id, count(*)
from {{ ref('stg_products') }} group by product_id having count(*) <> 1
union all
select 'orders', order_id, count(*)
from {{ ref('stg_orders') }} group by order_id having count(*) <> 1
union all
select 'payments', payment_id, count(*)
from {{ ref('stg_payments') }} group by payment_id having count(*) <> 1
