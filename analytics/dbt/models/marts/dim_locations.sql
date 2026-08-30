{{ config(materialized='table') }}
-- Grain: one current reference location. No fact join is fabricated: orders lack location_id.
select {{ dbt_utils.generate_surrogate_key(['location_id']) }} as location_sk,
       location_id, location_name, country_code, timezone, region, updated_at, ingested_at
from {{ ref('stg_locations') }}
