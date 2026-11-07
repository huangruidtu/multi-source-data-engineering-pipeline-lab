with source_rows as (
  {% if target.type == 'duckdb' %}
  select * from {{ ref('fixture_orders') }}
  {% else %}
  select * from {{ source('cdc_silver', 'core_orders') }}
  {% endif %}
)
select
  cast(order_id as varchar) as order_id,
  cast(customer_id as varchar) as customer_id,
  lower(cast(order_status as varchar)) as order_status,
  cast(order_ts as timestamp) as order_ts,
  cast(order_ts as date) as order_date,
  upper(cast(currency as varchar)) as currency,
  cast(order_total as decimal(18,2)) as order_total,
  cast(updated_at as timestamp) as updated_at,
  cast(source_lsn as varchar) as source_lsn,
  cast(applied_at as timestamp) as applied_at,
  cast(ingested_at as timestamp) as ingested_at,
  cast(record_hash as varchar) as record_hash
from source_rows
{% if target.type != 'duckdb' %}
where not coalesce(is_deleted, false)
{% endif %}
