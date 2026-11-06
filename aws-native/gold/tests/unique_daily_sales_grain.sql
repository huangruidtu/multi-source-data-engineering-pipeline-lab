select order_date, source_currency, count(*) as row_count
from {{ ref('mart_daily_sales') }}
group by order_date, source_currency
having count(*) <> 1
