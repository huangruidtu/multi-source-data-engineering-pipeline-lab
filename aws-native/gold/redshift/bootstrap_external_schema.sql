-- Render these four Jinja parameters through an approved deployment step.
-- This maps an EXISTING AWS-03 Glue database; it never creates or replaces
-- ref_exchange_rates or ref_locations.
create external schema if not exists {{ external_schema }}
from data catalog
database '{{ glue_database }}'
region '{{ aws_region }}'
iam_role '{{ redshift_iam_role_arn }}';

-- Evidence queries; these are read-only and deliberately name only AWS-03 tables.
select * from {{ external_schema }}.ref_exchange_rates limit 1;
select * from {{ external_schema }}.ref_locations limit 1;
