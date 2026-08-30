-- MDEP-13 Snowflake/dbt reconciliation templates. Replace scope predicates and
-- schema names only after recording the run id in evidence.

-- R03: Silver orders versus Gold facts by business key.
SELECT 'silver_not_gold' AS exception_type, s.order_id
FROM MDEP.SILVER_EXT.CORE_ORDERS s
LEFT JOIN MDEP.GOLD.FCT_ORDERS g ON s.order_id = g.order_id
WHERE g.order_id IS NULL
UNION ALL
SELECT 'gold_not_silver', g.order_id
FROM MDEP.GOLD.FCT_ORDERS g
LEFT JOIN MDEP.SILVER_EXT.CORE_ORDERS s ON g.order_id = s.order_id
WHERE s.order_id IS NULL;

-- R04/R07: payment facts and visible orphan relationship evidence.
SELECT p.payment_id, p.order_id
FROM MDEP.GOLD.FCT_PAYMENTS p
LEFT JOIN MDEP.GOLD.FCT_ORDERS o ON p.order_id = o.order_id
WHERE p.order_id IS NOT NULL AND o.order_id IS NULL;

-- R05: daily mart must equal the fact aggregation at the mart grain.
WITH expected AS (
  SELECT order_date, currency, COUNT(*) AS order_count, SUM(amount) AS gross_amount
  FROM MDEP.GOLD.FCT_ORDERS
  GROUP BY 1, 2
)
SELECT COALESCE(e.order_date, m.order_date) AS order_date,
       COALESCE(e.currency, m.currency) AS currency,
       e.order_count AS expected_count, m.order_count AS actual_count,
       e.gross_amount AS expected_amount, m.gross_amount AS actual_amount
FROM expected e FULL OUTER JOIN MDEP.GOLD.MART_DAILY_SALES m
  ON e.order_date = m.order_date AND e.currency = m.currency
WHERE NOT (e.order_count = m.order_count AND e.gross_amount = m.gross_amount);

-- R09/R10: canonical quality checks (repeat for each current-state model).
SELECT order_id, COUNT(*) AS duplicate_count
FROM MDEP.SILVER_EXT.CORE_ORDERS GROUP BY order_id HAVING COUNT(*) > 1;
SELECT COUNT(*) AS null_order_ids FROM MDEP.SILVER_EXT.CORE_ORDERS WHERE order_id IS NULL;
