CREATE SCHEMA commerce;
SET search_path TO commerce;

CREATE TABLE customers (
    customer_id TEXT PRIMARY KEY,
    customer_name TEXT NOT NULL,
    email TEXT,
    customer_status TEXT NOT NULL CHECK (customer_status IN ('active', 'inactive', 'suspended')),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE products (
    product_id TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    category_code TEXT NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    currency CHAR(3) NOT NULL CHECK (currency = UPPER(currency)),
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL REFERENCES customers(customer_id),
    order_status TEXT NOT NULL CHECK (order_status IN ('created', 'completed', 'cancelled')),
    order_ts TIMESTAMPTZ NOT NULL,
    currency CHAR(3) NOT NULL CHECK (currency = UPPER(currency)),
    order_total NUMERIC(12, 2) NOT NULL CHECK (order_total >= 0),
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE payments (
    payment_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL REFERENCES orders(order_id),
    payment_status TEXT NOT NULL CHECK (payment_status IN ('authorized', 'completed', 'failed', 'refunded')),
    payment_ts TIMESTAMPTZ NOT NULL,
    amount NUMERIC(12, 2) NOT NULL CHECK (amount >= 0),
    currency CHAR(3) NOT NULL CHECK (currency = UPPER(currency)),
    authorization_code TEXT,
    updated_at TIMESTAMPTZ NOT NULL
);
