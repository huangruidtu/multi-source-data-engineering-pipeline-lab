# MDEP-11 runbook

```powershell
docker compose up --build --wait
./scripts/validate-mdep-10-cdc.ps1
Invoke-WebRequest http://localhost:8082
# package the Flink job with compatible Flink 1.20 Kafka and Iceberg runtime jars, then submit it:
docker compose exec flink-jobmanager flink run /opt/flink/usrlib/mdep/<packaged-job>.jar
docker compose exec flink-jobmanager flink list
```

Inspect Debezium input with the MDEP-10 Kafka consumer, source slot/LSN with `psql`, and local artifacts under `build/mdep-11-checkpoints`, `build/mdep-11-savepoints`, Bronze CDC, Quarantine, and HadoopCatalog warehouse. Insert/update/delete a customer, then replay an old lower-LSN message and an exact duplicate; only newer state may change. Restart the Flink job and inspect checkpoint recovery and current Iceberg table/snapshots.

The watermark exercise sends a source timestamp older than 60 seconds; record its late disposition separately from LSN acceptance. Run the MDEP-10 `preferred_language` alteration/update and verify a later Iceberg table schema/record. These commands are **UNVALIDATED** because Docker/Flink/Kafka/Iceberg are unavailable on this host.
