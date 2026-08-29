SET search_path TO commerce;

-- This statement is expected to fail because cust-999 does not exist.
INSERT INTO orders (order_id, customer_id, order_status, order_ts, currency, order_total, updated_at)
VALUES ('ord-invalid-fk', 'cust-999', 'created', '2025-02-05T12:00:00Z', 'EUR', 10.00, '2025-02-05T12:00:00Z');
