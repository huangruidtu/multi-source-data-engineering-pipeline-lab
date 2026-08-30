{{ config(materialized='table') }}
-- Grain: one calendar date observed in orders or payments.
with dates as (
  select order_date as date_day from {{ ref('stg_orders') }}
  union
  select payment_date as date_day from {{ ref('stg_payments') }}
)
select distinct date_day, year(date_day) as calendar_year, month(date_day) as calendar_month,
       dayofweek(date_day) as day_of_week
from dates where date_day is not null
