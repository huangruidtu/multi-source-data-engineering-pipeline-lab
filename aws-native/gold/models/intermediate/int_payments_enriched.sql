select
  p.*,
  o.customer_id,
  o.order_date,
  case when o.order_id is null then true else false end as orphan_order
from {{ ref('stg_payments') }} p
left join {{ ref('stg_orders') }} o
  on p.order_id = o.order_id
