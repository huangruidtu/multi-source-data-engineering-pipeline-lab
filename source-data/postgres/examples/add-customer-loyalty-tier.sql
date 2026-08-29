SET search_path TO commerce;

-- Controlled additive schema-change exercise. Do not include this in the default seed/reset path.
ALTER TABLE customers ADD COLUMN loyalty_tier TEXT;
UPDATE customers SET loyalty_tier = 'standard' WHERE loyalty_tier IS NULL;
