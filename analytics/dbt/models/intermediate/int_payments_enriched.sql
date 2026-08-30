select p.*, o.customer_id, o.order_date
from {{ ref('stg_payments') }} p
left join {{ ref('stg_orders') }} o on p.order_id = o.order_id
