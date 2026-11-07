select 'customer_orphan_lost' as failure
where not exists (
  select 1 from {{ ref('fct_orders') }}
  where customer_id = 'cust-missing' and customer_sk is null and orphan_customer = true
)
union all
select 'payment_orphan_lost' as failure
where not exists (
  select 1 from {{ ref('fct_payments') }}
  {% if target.type == 'duckdb' and env_var('MDEP_GOLD_INPUT_MODE', 'fixture') == 'integrated' %}
  where matched_order_id is null and orphan_order = true
  {% else %}
  where source_order_id = 'ord-missing' and matched_order_id is null and orphan_order = true
  {% endif %}
)
