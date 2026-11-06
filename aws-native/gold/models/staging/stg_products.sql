with source_rows as (
  {% if target.type == 'duckdb' %}
  select * from {{ ref('fixture_products') }}
  {% else %}
  select * from {{ source('cdc_silver', 'core_products') }}
  {% endif %}
)
select
  cast(product_id as varchar) as product_id,
  cast(product_name as varchar) as product_name,
  cast(category_code as varchar) as category_code,
  cast(unit_price as decimal(18,2)) as unit_price,
  upper(cast(currency as varchar)) as currency,
  cast(updated_at as timestamp) as updated_at,
  cast(source_lsn as varchar) as source_lsn,
  cast(applied_at as timestamp) as applied_at,
  cast(ingested_at as timestamp) as ingested_at,
  cast(record_hash as varchar) as record_hash
from source_rows
{% if target.type != 'duckdb' %}
where not coalesce(is_deleted, false)
{% endif %}
