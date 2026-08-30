-- MDEP-10 PostgreSQL logical-decoding prerequisites. The compose `lab` role is
-- the database owner and is granted replication for the local Debezium lab.
ALTER ROLE lab WITH REPLICATION;
CREATE PUBLICATION mdep_publication FOR TABLE
  commerce.customers, commerce.products, commerce.orders, commerce.payments;
