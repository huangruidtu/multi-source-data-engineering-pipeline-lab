with source_rows as (
  {% if target.type == 'duckdb' and env_var('MDEP_GOLD_INPUT_MODE', 'fixture') == 'fixture' %}
  select * from {{ ref('fixture_payments') }}
  {% else %}
  select * from {{ source('cdc_silver', 'core_payments') }}
  {% endif %}
)
select
  cast(payment_id as varchar) as payment_id,
  cast(order_id as varchar) as order_id,
  lower(cast(payment_status as varchar)) as payment_status,
  cast(payment_ts as timestamp) as payment_ts,
  cast(payment_ts as date) as payment_date,
  cast(amount as decimal(18,2)) as amount,
  upper(cast(currency as varchar)) as currency,
  cast(authorization_code as varchar) as authorization_code,
  cast(updated_at as timestamp) as updated_at,
  cast(source_lsn as varchar) as source_lsn,
  cast(applied_at as timestamp) as applied_at,
  cast(ingested_at as timestamp) as ingested_at,
  cast(record_hash as varchar) as record_hash
from source_rows
{% if target.type != 'duckdb' or env_var('MDEP_GOLD_INPUT_MODE', 'fixture') == 'integrated' %}
where not coalesce(is_deleted, false)
{% endif %}
