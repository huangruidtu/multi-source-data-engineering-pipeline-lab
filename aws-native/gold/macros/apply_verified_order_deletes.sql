{% macro apply_verified_order_deletes() %}
  {% if not execute %}
    {{ return('compile-only') }}
  {% endif %}
  {% if not var('full_snapshot_verified', false) %}
    {{ exceptions.raise_compiler_error('Hard deletes require full_snapshot_verified=true; an ordinary or partial feed is not delete proof.') }}
  {% endif %}

  {% set target_relation = ref('fct_orders') %}
  {% set source_relation = ref('stg_orders') %}
  {% set counts_sql %}
    select
      (select count(*) from {{ target_relation }}) as target_count,
      (select count(*) from {{ target_relation }} target
       where not exists (
         select 1 from {{ source_relation }} source
         where source.order_id = target.order_id
       )) as delete_count
  {% endset %}
  {% set counts = run_query(counts_sql).rows[0] %}
  {% set target_count = counts[0] | int %}
  {% set delete_count = counts[1] | int %}
  {% set maximum_fraction = var('maximum_delete_fraction', 0.25) | float %}
  {% set delete_fraction = (delete_count / target_count) if target_count > 0 else 0 %}

  {% if delete_fraction > maximum_fraction %}
    {{ exceptions.raise_compiler_error(
      'Refusing ' ~ delete_count ~ ' order deletes from ' ~ target_count ~
      ' rows: fraction ' ~ delete_fraction ~ ' exceeds maximum_delete_fraction=' ~ maximum_fraction
    ) }}
  {% endif %}

  {% set delete_sql %}
    delete from {{ target_relation }} target
    where not exists (
      select 1 from {{ source_relation }} source
      where source.order_id = target.order_id
    )
  {% endset %}
  {% do run_query(delete_sql) %}
  {% do log('AWS04_DELETE_EVIDENCE target=' ~ target_count ~ ' deleted=' ~ delete_count ~ ' fraction=' ~ delete_fraction, info=true) %}
  {{ return('deleted=' ~ delete_count) }}
{% endmacro %}
