-- Grain: one current customer. Type 1 current-state dimension.
select
  {{ deterministic_key(['customer_id']) }} as customer_sk,
  customer_id,
  customer_name,
  email,
  customer_status,
  created_at,
  updated_at,
  source_lsn,
  applied_at,
  ingested_at,
  record_hash
from {{ ref('stg_customers') }}
