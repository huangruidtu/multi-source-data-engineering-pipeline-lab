param()
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { throw "Docker/Flink runtime is required." }
docker compose up --build --wait
Invoke-RestMethod http://localhost:8083/connectors/mdep-postgres-cdc/status | ConvertTo-Json -Depth 10
Invoke-WebRequest http://localhost:8082 | Out-Null
Write-Host "Submit the packaged Flink Kafka/Iceberg job, then inspect checkpoints, Bronze CDC, Quarantine, and mdep.silver.core_* tables. Runtime artifacts are not faked by this script."
