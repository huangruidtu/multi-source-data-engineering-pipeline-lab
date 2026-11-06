-- Grain: one observed commerce business date.
with dates as (
  select order_date as date_day from {{ ref('stg_orders') }}
  union
  select payment_date as date_day from {{ ref('stg_payments') }}
)
select
  {{ deterministic_key(['date_day']) }} as date_sk,
  date_day,
  cast(date_part('year', date_day) as integer) as calendar_year,
  cast(date_part('month', date_day) as integer) as calendar_month,
  cast(date_part('day', date_day) as integer) as day_of_month
from dates
where date_day is not null
