select order_id
from {{ ref('fct_orders') }}
where (source_currency = 'DKK' and order_total_dkk <> source_order_total)
   or (source_currency <> 'DKK' and dkk_rate > 0
       and order_total_dkk <> cast(round(source_order_total * dkk_rate, 2) as decimal(18,2)))
   or (source_currency <> 'DKK' and dkk_rate is null
       and (order_total_dkk is not null or missing_dkk_rate = false))
