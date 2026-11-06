-- Grain: one current product. Products are not fabricated onto orders.
select
  {{ deterministic_key(['product_id']) }} as product_sk,
  product_id,
  product_name,
  category_code,
  unit_price,
  currency,
  updated_at,
  source_lsn,
  applied_at,
  ingested_at,
  record_hash
from {{ ref('stg_products') }}
