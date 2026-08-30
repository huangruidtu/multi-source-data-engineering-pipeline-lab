param([string]$Bucket = "mdep-lake")

$ErrorActionPreference = "Stop"
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { throw "Docker is required for MDEP-11 runtime validation." }
$env:BRONZE_CDC_ROOT = "s3a://$Bucket/bronze/cdc"
$env:QUARANTINE_CDC_ROOT = "s3a://$Bucket/quarantine/cdc"
$env:ICEBERG_WAREHOUSE = "s3a://$Bucket/iceberg"
docker compose up --build --wait
Invoke-RestMethod http://localhost:8083/connectors/mdep-postgres-cdc/status | ConvertTo-Json -Depth 10
Invoke-WebRequest http://localhost:8082 | Out-Null

# The Dockerfile installs PyFlink 1.20.0, Kafka 3.3.0-1.20, Iceberg 1.20-1.6.1,
# and enables Flink's S3 filesystem plugin; no placeholder JAR is required.
docker compose exec flink-jobmanager flink run -py /opt/flink/usrlib/mdep/flink_cdc_job.py
docker compose exec flink-jobmanager flink list
Write-Host "Create INSERT, UPDATE, DELETE, replay, malformed-envelope, and tombstone cases. Inspect S3 Bronze/Quarantine Parquet paths, Iceberg mdep.silver.core_* tables/snapshots, and checkpoint recovery. This script makes no success claim until observations are recorded."
