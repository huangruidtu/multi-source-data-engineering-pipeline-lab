{% macro amount_in_dkk(amount, source_currency, rate) -%}
case
  when {{ amount }} is null then cast(null as decimal(18,2))
  when {{ source_currency }} = 'DKK' then cast({{ amount }} as decimal(18,2))
  when {{ rate }} > 0 then cast(round({{ amount }} * {{ rate }}, 2) as decimal(18,2))
  else cast(null as decimal(18,2))
end
{%- endmacro %}
