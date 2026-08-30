select rate_date, base_currency, quote_currency, rate, retrieved_at, ingested_at
from {{ source('silver_ext', 'ref_exchange_rates') }}
