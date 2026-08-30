select product_id, product_name, category_code, unit_price, currency, updated_at, source_lsn, applied_at
from {{ source('silver_ext', 'core_products') }}
