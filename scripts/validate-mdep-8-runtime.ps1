[CmdletBinding()]
param(
    [string]$StartDate = '2025-02-01',
    [string]$EndDate = '2025-02-02'
)

$ErrorActionPreference = 'Stop'

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw 'Docker is required for MDEP-8 runtime validation. See docs/learning/mdep-8/runbook.md.'
}

docker compose up --build --wait
& "$PSScriptRoot/reset-sources.ps1"
docker compose exec -T airflow airflow dags list | Select-String 'mdep_bronze_ingestion'
docker compose exec -T airflow airflow dags test mdep_bronze_ingestion $StartDate
docker compose exec -T airflow airflow dags test mdep_bronze_ingestion $StartDate
docker compose exec -T airflow airflow dags backfill mdep_bronze_ingestion -s $StartDate -e $EndDate

$root = Join-Path (Get-Location) 'build/mdep-8-bronze'
if (-not (Test-Path "$root/bronze")) { throw 'No Bronze output was produced.' }
if (-not (Test-Path "$root/quarantine")) { throw 'No Quarantine output was produced.' }

docker compose exec -T airflow python -c "import pyarrow.parquet as pq; from pathlib import Path; files=list(Path('/opt/airflow/bronze/bronze').rglob('*.parquet')); assert files; table=pq.read_table(files[0]); required={'ingestion_id','source_name','source_entity','source_record_key','source_extract_ts','ingested_at','source_version','source_locator','record_hash'}; assert required <= set(table.column_names); print({'file': str(files[0]), 'rows': table.num_rows, 'metadata_columns': sorted(required)})"
Write-Host 'MDEP-8 runtime validation passed.'
