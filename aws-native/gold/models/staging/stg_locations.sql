with source_rows as (
  {% if target.type == 'duckdb' and env_var('MDEP_GOLD_INPUT_MODE', 'fixture') == 'fixture' %}
  select * from {{ ref('silver_bridge_locations') }}
  {% else %}
  select * from {{ source('silver_iceberg', 'ref_locations') }}
  {% endif %}
)
select
  cast(location_id as varchar) as location_id,
  cast(location_name as varchar) as location_name,
  upper(cast(country_code as varchar)) as country_code,
  cast(timezone as varchar) as timezone,
  cast(region as varchar) as region,
  cast(updated_at as timestamp) as updated_at,
  cast(location_type as varchar) as location_type,
  cast(source_extract_ts as timestamp) as source_extract_ts,
  cast(ingested_at as timestamp) as ingested_at,
  cast(record_hash as varchar) as record_hash,
  cast(manifest_key as varchar) as manifest_key
from source_rows
