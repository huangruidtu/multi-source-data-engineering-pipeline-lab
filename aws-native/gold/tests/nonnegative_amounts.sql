select order_id
from {{ ref('fct_orders') }}
where source_order_total < 0
   or order_total_dkk < 0
union all
select payment_id
from {{ ref('fct_payments') }}
where source_amount < 0
