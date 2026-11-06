{% macro deterministic_key(columns) -%}
md5(
  {%- for column in columns -%}
    coalesce(cast({{ column }} as varchar), '__null__')
    {%- if not loop.last %} || '|' || {% endif -%}
  {%- endfor -%}
)
{%- endmacro %}
