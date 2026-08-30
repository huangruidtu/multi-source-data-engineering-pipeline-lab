select order_id
from {{ ref('fct_orders') }}
where order_total < 0 or (order_total_dkk is not null and order_total_dkk < 0)
