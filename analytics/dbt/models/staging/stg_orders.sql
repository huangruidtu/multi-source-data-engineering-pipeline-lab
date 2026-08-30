select order_id, customer_id, order_status, cast(order_ts as date) as order_date,
       currency, order_total, updated_at, source_lsn, applied_at
from {{ source('silver_ext', 'core_orders') }}
