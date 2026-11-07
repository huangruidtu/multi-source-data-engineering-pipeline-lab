{% macro apply_verified_order_tombstones() %}
  {% if not execute %}
    {{ return('compile-only') }}
  {% endif %}
  {% if not var('cdc_tombstones_verified', false) %}
    {{ exceptions.raise_compiler_error(
      'CDC deletes require cdc_tombstones_verified=true; ordinary source absence is not delete evidence.'
    ) }}
  {% endif %}

  {% set target_relation = ref('fct_orders') %}
  {% set source_relation = source('cdc_silver', 'core_orders') %}
  {% set evidence_sql %}
    select count(*)
    from {{ source_relation }}
    where coalesce(is_deleted, false)
      and lower(dms_operation) = 'delete'
      and source_lsn is not null
  {% endset %}
  {% set delete_count = run_query(evidence_sql).rows[0][0] | int %}
  {% set delete_sql %}
    delete from {{ target_relation }} target
    using {{ source_relation }} tombstone
    where target.order_id = tombstone.order_id
      and coalesce(tombstone.is_deleted, false)
      and lower(tombstone.dms_operation) = 'delete'
      and tombstone.source_lsn is not null
  {% endset %}
  {% do run_query(delete_sql) %}
  {% do log('AWS07_TOMBSTONE_DELETE_EVIDENCE verified=' ~ delete_count, info=true) %}
  {{ return('verified_tombstones=' ~ delete_count) }}
{% endmacro %}
