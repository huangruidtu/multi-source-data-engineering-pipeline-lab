SET search_path TO commerce;

-- INSERT: a new source record for later CDC exercises.
INSERT INTO customers (customer_id, customer_name, email, customer_status, created_at, updated_at)
VALUES ('cust-400', 'Margaret Hamilton', 'margaret@example.test', 'active', '2025-02-04T09:00:00Z', '2025-02-04T09:00:00Z');

-- UPDATE: changes a source record while preserving the business key.
UPDATE orders
SET order_status = 'completed', updated_at = '2025-02-04T12:05:00Z'
WHERE order_id = 'ord-200';

-- DELETE: removes an unreferenced source record so a later CDC consumer can observe a delete.
DELETE FROM customers WHERE customer_id = 'cust-300';
