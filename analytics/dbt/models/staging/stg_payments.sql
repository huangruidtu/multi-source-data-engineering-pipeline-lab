select payment_id, order_id, payment_status, cast(payment_ts as date) as payment_date,
       amount, currency, authorization_code, updated_at, source_lsn, applied_at
from {{ source('silver_ext', 'core_payments') }}
