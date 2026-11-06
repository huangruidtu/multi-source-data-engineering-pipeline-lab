-- Grain: one AWS-03 current location. No order/location relationship exists.
select
  {{ deterministic_key(['location_id']) }} as location_sk,
  location_id,
  location_name,
  country_code,
  timezone,
  region,
  location_type,
  updated_at,
  source_extract_ts,
  ingested_at,
  record_hash,
  manifest_key
from {{ ref('stg_locations') }}
