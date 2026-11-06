with source_rows as (
  {% if target.type == 'duckdb' %}
  select * from {{ ref('silver_bridge_exchange_rates') }}
  {% else %}
  select * from {{ source('silver_iceberg', 'ref_exchange_rates') }}
  {% endif %}
)
select
  cast(rate_date as date) as rate_date,
  upper(cast(base_currency as varchar)) as base_currency,
  upper(cast(quote_currency as varchar)) as quote_currency,
  cast(rate as decimal(20,10)) as rate,
  cast(retrieved_at as timestamp) as retrieved_at,
  cast(provider_name as varchar) as provider_name,
  cast(source_extract_ts as timestamp) as source_extract_ts,
  cast(ingested_at as timestamp) as ingested_at,
  cast(record_hash as varchar) as record_hash,
  cast(manifest_key as varchar) as manifest_key
from source_rows
