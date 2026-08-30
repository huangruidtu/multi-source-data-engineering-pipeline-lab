{{ config(materialized='table') }}
-- Grain: one current Silver customer. Type 1: source updates overwrite attributes.
select {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk,
       customer_id, customer_name, email, customer_status, created_at, updated_at, preferred_language, applied_at
from {{ ref('stg_customers') }}
