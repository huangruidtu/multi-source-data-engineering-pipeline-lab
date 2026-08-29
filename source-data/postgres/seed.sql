SET search_path TO commerce;

INSERT INTO customers (customer_id, customer_name, email, customer_status, created_at, updated_at) VALUES
    ('cust-100', 'Ada Lovelace', 'ada@example.test', 'active', '2025-01-01T09:00:00Z', '2025-01-01T09:00:00Z'),
    ('cust-200', 'Grace Hopper', 'grace@example.test', 'active', '2025-01-02T09:00:00Z', '2025-01-02T09:00:00Z'),
    ('cust-300', 'Linus Torvalds', NULL, 'inactive', '2025-01-03T09:00:00Z', '2025-01-03T09:00:00Z');

INSERT INTO products (product_id, product_name, category_code, unit_price, currency, updated_at) VALUES
    ('prod-100', 'Telemetry Sensor', 'ELEC', 49.90, 'EUR', '2025-01-01T10:00:00Z'),
    ('prod-200', 'Field Toolkit', 'HOME', 79.00, 'EUR', '2025-01-02T10:00:00Z'),
    ('prod-300', 'Service Plan', 'SERV', 19.50, 'EUR', '2025-01-03T10:00:00Z');

INSERT INTO orders (order_id, customer_id, order_status, order_ts, currency, order_total, updated_at) VALUES
    ('ord-100', 'cust-100', 'completed', '2025-02-01T12:00:00Z', 'EUR', 49.90, '2025-02-01T12:05:00Z'),
    ('ord-200', 'cust-200', 'created', '2025-02-02T12:00:00Z', 'EUR', 79.00, '2025-02-02T12:00:00Z'),
    ('ord-300', 'cust-100', 'cancelled', '2025-02-03T12:00:00Z', 'EUR', 19.50, '2025-02-03T12:10:00Z');

INSERT INTO payments (payment_id, order_id, payment_status, payment_ts, amount, currency, authorization_code, updated_at) VALUES
    ('pay-100', 'ord-100', 'completed', '2025-02-01T12:01:00Z', 49.90, 'EUR', 'auth-100', '2025-02-01T12:05:00Z'),
    ('pay-200', 'ord-200', 'authorized', '2025-02-02T12:01:00Z', 79.00, 'EUR', 'auth-200', '2025-02-02T12:01:00Z'),
    ('pay-300', 'ord-300', 'failed', '2025-02-03T12:01:00Z', 19.50, 'EUR', NULL, '2025-02-03T12:10:00Z');
