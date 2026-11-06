select *
from {{ ref('stg_exchange_rates') }}
where rate <= 0
   or base_currency = quote_currency
   or rate_date is null
