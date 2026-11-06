select *
from {{ ref('reconciliation_orders') }}
where gold_duplicate_count <> 0
   or source_duplicate_count <> 0
   or gold_distinct_key_count < source_distinct_key_count
