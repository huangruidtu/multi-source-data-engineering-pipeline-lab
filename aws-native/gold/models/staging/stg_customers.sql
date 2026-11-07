with source_rows as (
  {% if target.type == 'duckdb' and env_var('MDEP_GOLD_INPUT_MODE', 'fixture') == 'fixture' %}
  select * from {{ ref('fixture_customers') }}
  {% else %}
  select * from {{ source('cdc_silver', 'core_customers') }}
  {% endif %}
)
select
  cast(customer_id as varchar) as customer_id,
  cast(customer_name as varchar) as customer_name,
  cast(email as varchar) as email,
  upper(cast(customer_status as varchar)) as customer_status,
  cast(created_at as timestamp) as created_at,
  cast(updated_at as timestamp) as updated_at,
  cast(source_lsn as varchar) as source_lsn,
  cast(applied_at as timestamp) as applied_at,
  cast(ingested_at as timestamp) as ingested_at,
  cast(record_hash as varchar) as record_hash
from source_rows
{% if target.type != 'duckdb' or env_var('MDEP_GOLD_INPUT_MODE', 'fixture') == 'integrated' %}
where not coalesce(is_deleted, false)
{% endif %}
