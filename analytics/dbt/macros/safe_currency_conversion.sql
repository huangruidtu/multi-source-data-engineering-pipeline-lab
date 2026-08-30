{% macro safe_currency_conversion(amount, base_currency, rate) %}
case
  when {{ amount }} is null then null
  when {{ base_currency }} = 'DKK' then {{ amount }}
  when {{ rate }} is not null and {{ rate }} > 0 then round({{ amount }} * {{ rate }}, 2)
  else null
end
{% endmacro %}
