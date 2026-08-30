{{ config(materialized='table') }}
-- Grain: one current Silver product; Type 1 current-state dimension.
select {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_sk,
       product_id, product_name, category_code, unit_price, currency, updated_at, applied_at
from {{ ref('stg_products') }}
