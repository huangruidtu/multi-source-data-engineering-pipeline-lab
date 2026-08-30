select location_id, location_name, country_code, timezone, region, updated_at, ingested_at
from {{ source('silver_ext', 'ref_locations') }}
