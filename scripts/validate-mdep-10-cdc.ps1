param([switch]$Reset)

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { throw "Docker is required for MDEP-10 runtime validation." }
if ($Reset) { docker compose down -v }
docker compose up --build --wait

$config = Get-Content -Raw ingestion/cdc/debezium-postgres-connector.json
Invoke-RestMethod -Method Put -Uri http://localhost:8083/connectors/mdep-postgres-cdc/config -ContentType 'application/json' -Body (($config | ConvertFrom-Json).config | ConvertTo-Json -Depth 20)
Invoke-RestMethod http://localhost:8083/connectors/mdep-postgres-cdc/status | ConvertTo-Json -Depth 10
docker compose exec postgres psql -U lab -d commerce -c "SHOW wal_level; SELECT pubname FROM pg_publication; SELECT slot_name, confirmed_flush_lsn FROM pg_replication_slots; SELECT pg_current_wal_lsn();"
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic mdep.commerce.customers

# Snapshot messages appear first. Then demonstrate create/update/delete, an additive column,
# one transaction, restart/resume, and replay with a separate group.
docker compose exec postgres psql -U lab -d commerce -c "INSERT INTO commerce.customers VALUES ('cust-cdc-1','CDC Example','cdc@example.test','active',now(),now()); UPDATE commerce.customers SET customer_status='inactive', updated_at=now() WHERE customer_id='cust-cdc-1'; DELETE FROM commerce.customers WHERE customer_id='cust-cdc-1'; ALTER TABLE commerce.customers ADD COLUMN IF NOT EXISTS preferred_language TEXT; UPDATE commerce.customers SET preferred_language='en' WHERE customer_id='cust-100'; BEGIN; UPDATE commerce.products SET updated_at=now() WHERE product_id='prod-100'; UPDATE commerce.products SET updated_at=now() WHERE product_id='prod-200'; COMMIT;"
docker compose restart connect
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic mdep.commerce.customers --from-beginning --property print.key=true --property key.separator=' | ' --timeout-ms 10000
