select customer_id, customer_name, email, customer_status, created_at, updated_at,
       preferred_language, source_lsn, applied_at
from {{ source('silver_ext', 'core_customers') }}
