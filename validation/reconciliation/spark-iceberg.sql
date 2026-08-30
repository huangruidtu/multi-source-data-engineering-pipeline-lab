-- MDEP-13 Spark SQL / Iceberg reconciliation templates. Run in spark-sql or a
-- notebook configured with the same mdep catalog as MDEP-9/MDEP-11.

-- R01/R02: compare a source extract temp view to Silver current state by key.
-- Create source_orders_current/source_payments_current from the bounded source
-- snapshot before running these anti-joins.
SELECT 'source_not_silver' AS exception_type, s.order_id
FROM source_orders_current s
LEFT ANTI JOIN mdep.silver.core_orders t ON s.order_id = t.order_id
UNION ALL
SELECT 'silver_not_source', t.order_id
FROM mdep.silver.core_orders t
LEFT ANTI JOIN source_orders_current s ON s.order_id = t.order_id;

SELECT order_id, COUNT(*) AS duplicate_count
FROM mdep.silver.core_orders GROUP BY order_id HAVING COUNT(*) > 1;

SELECT payment_id, COUNT(*) AS duplicate_count
FROM mdep.silver.core_payments GROUP BY payment_id HAVING COUNT(*) > 1;

SELECT COUNT(*) AS null_required_order_ids
FROM mdep.silver.core_orders WHERE order_id IS NULL;

-- R08: after a source deletion, the selected key must be absent from current
-- state; Bronze is intentionally not queried here because it retains history.
SELECT * FROM mdep.silver.core_orders WHERE order_id = '<deleted-order-id>';
