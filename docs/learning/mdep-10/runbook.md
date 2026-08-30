# MDEP-10 runbook

Start the local stack and register the connector:

```powershell
docker compose up --build --wait
./scripts/validate-mdep-10-cdc.ps1
Invoke-RestMethod http://localhost:8083/connectors/mdep-postgres-cdc/status
```

Inspect source replication and topics:

```powershell
docker compose exec postgres psql -U lab -d commerce -c "SHOW wal_level; SELECT * FROM pg_publication; SELECT slot_name, confirmed_flush_lsn FROM pg_replication_slots; SELECT pg_current_wal_lsn();"
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic mdep.commerce.customers
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic mdep.commerce.customers --from-beginning --property print.key=true --property key.separator=' | '
```

Run explicit mutations with `psql`: INSERT a row, UPDATE it, DELETE it; `after` is null for the delete and a tombstone follows. Run the SQL inside the validator for a multi-row transaction and `ALTER TABLE commerce.customers ADD COLUMN IF NOT EXISTS preferred_language TEXT`, then update a customer. The connector is **CONFIGURED** with `provide.transaction.metadata=true`; consume the configured transaction topic and data topics to inspect transaction metadata/boundary events. Debezium 3.0 is **expected** to emit/enrich these where supported, but this is **runtime unvalidated** until observed. Restart `connect`, `kafka`, or `postgres` with `docker compose restart <service>` and compare connector status, slot LSN, and messages. Use a fresh console-consumer group with `--group mdep-replay --from-beginning` to replay retained history.

For a clean reset, run `docker compose down -v` then `./scripts/validate-mdep-10-cdc.ps1 -Reset`; this deliberately destroys offsets and source data. Runtime validation is currently UNVALIDATED on this host because Docker is unavailable.
