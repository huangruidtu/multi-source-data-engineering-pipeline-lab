[CmdletBinding()]
param(
    [switch]$RunRuntime,
    [string]$RunId = (Get-Date -Format 'yyyyMMddTHHmmssZ'),
    [string]$BronzeRoot,
    [string]$Warehouse,
    [string]$Bucket
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$evidenceRoot = Join-Path $repoRoot (Join-Path 'validation/evidence' $RunId)
New-Item -ItemType Directory -Force -Path $evidenceRoot, (Join-Path $evidenceRoot 'batch'), (Join-Path $evidenceRoot 'cdc'), (Join-Path $evidenceRoot 'silver'), (Join-Path $evidenceRoot 'gold'), (Join-Path $evidenceRoot 'failures'), (Join-Path $evidenceRoot 'reconciliation') | Out-Null

$logPath = Join-Path $evidenceRoot 'commands.log'
$summary = [System.Collections.Generic.List[object]]::new()

function Add-StageResult {
    param([string]$Id, [string]$Status, [string]$Evidence, [string]$Note)
    $summary.Add([ordered]@{ id = $Id; actual_status = $Status; evidence_path = $Evidence; note = $Note })
}

function Invoke-RecordedCommand {
    param([string]$Id, [string]$WorkingDirectory, [scriptblock]$Command, [string]$EvidenceDirectory)
    "`n[$((Get-Date).ToUniversalTime().ToString('o'))] $Id" | Add-Content -Path $logPath
    try {
        $output = & $Command 2>&1
        $output | Tee-Object -FilePath $logPath -Append | Set-Content -Path (Join-Path $EvidenceDirectory "$Id.log")
        Add-StageResult $Id 'PASSED' (Join-Path $EvidenceDirectory "$Id.log") 'Command completed successfully.'
    } catch {
        $_ | Out-String | Tee-Object -FilePath $logPath -Append | Set-Content -Path (Join-Path $EvidenceDirectory "$Id.log")
        Add-StageResult $Id 'FAILED' (Join-Path $EvidenceDirectory "$Id.log") $_.Exception.Message
    }
}

Push-Location $repoRoot
try {
    $preflightPath = Join-Path $evidenceRoot 'preflight.json'
    $preflight = & (Join-Path $PSScriptRoot 'preflight-mdep-13.ps1') -OutputPath $preflightPath
    $preflight | ConvertTo-Json -Depth 8 | Set-Content -Path $preflightPath -Encoding utf8
    $PSVersionTable | Out-String | Set-Content -Path (Join-Path $evidenceRoot 'environment.txt') -Encoding utf8

    if ($preflight.capabilities.python.status -eq 'AVAILABLE') {
        Invoke-RecordedCommand 'STATIC-PYTHON-TESTS' $repoRoot { python -m unittest discover -s tests -p 'test_*.py' -v } $evidenceRoot
    } else {
        Add-StageResult 'STATIC-PYTHON-TESTS' 'BLOCKED' $null 'python is unavailable.'
    }

    if (-not $RunRuntime) {
        Add-StageResult 'M8-AIRFLOW-BATCH' 'NOT_RUN' $null 'Pass -RunRuntime only in a Docker-capable disposable environment.'
        Add-StageResult 'M9-SPARK-ICEBERG' 'NOT_RUN' $null 'Pass -RunRuntime with -BronzeRoot and -Warehouse after preparing Bronze input.'
        Add-StageResult 'M10-DEBEZIUM-KAFKA' 'NOT_RUN' $null 'Pass -RunRuntime only in a Docker-capable disposable environment.'
        Add-StageResult 'M11-FLINK-CDC' 'NOT_RUN' $null 'Pass -RunRuntime with -Bucket and AWS credentials in a disposable environment.'
        Add-StageResult 'M12-SNOWFLAKE-DBT' 'NOT_RUN' $null 'Pass -RunRuntime only with dbt and a non-production Snowflake target.'
    } else {
        if ($preflight.capabilities.docker.status -eq 'AVAILABLE' -and $preflight.capabilities.docker_compose.status -eq 'AVAILABLE') {
            Invoke-RecordedCommand 'M8-AIRFLOW-BATCH' $repoRoot { & scripts/validate-mdep-8-runtime.ps1 } (Join-Path $evidenceRoot 'batch')
            Invoke-RecordedCommand 'M10-DEBEZIUM-KAFKA' $repoRoot { & scripts/validate-mdep-10-cdc.ps1 } (Join-Path $evidenceRoot 'cdc')
            if ($Bucket -and $preflight.capabilities.aws_s3_credentials.status -eq 'AVAILABLE') {
                Invoke-RecordedCommand 'M11-FLINK-CDC' $repoRoot { & scripts/validate-mdep-11-flink-cdc.ps1 -Bucket $Bucket } (Join-Path $evidenceRoot 'cdc')
            } else { Add-StageResult 'M11-FLINK-CDC' 'BLOCKED' $null 'Bucket and AWS credentials are required.' }
        } else {
            'M8-AIRFLOW-BATCH','M10-DEBEZIUM-KAFKA','M11-FLINK-CDC' | ForEach-Object { Add-StageResult $_ 'BLOCKED' $null 'Docker Compose is unavailable.' }
        }
        if ($preflight.capabilities.spark_submit.status -eq 'AVAILABLE' -and $BronzeRoot -and $Warehouse) {
            Invoke-RecordedCommand 'M9-SPARK-ICEBERG' $repoRoot { & scripts/run-mdep-9-silver.ps1 -BronzeRoot $BronzeRoot -Warehouse $Warehouse -Inspect } (Join-Path $evidenceRoot 'silver')
        } else { Add-StageResult 'M9-SPARK-ICEBERG' 'BLOCKED' $null 'spark-submit, BronzeRoot, and Warehouse are required.' }
        if ($preflight.capabilities.dbt.status -eq 'AVAILABLE' -and $preflight.capabilities.snowflake_credentials.status -eq 'AVAILABLE') {
            Invoke-RecordedCommand 'M12-SNOWFLAKE-DBT' (Join-Path $repoRoot 'analytics/dbt') { Push-Location analytics/dbt; try { dbt deps; dbt debug; dbt parse; dbt compile; dbt run; dbt test; dbt source freshness } finally { Pop-Location } } (Join-Path $evidenceRoot 'gold')
        } else { Add-StageResult 'M12-SNOWFLAKE-DBT' 'BLOCKED' $null 'dbt and Snowflake credentials are required.' }
    }
} finally {
    Pop-Location
}

$result = [ordered]@{ run_id = $RunId; generated_at_utc = (Get-Date).ToUniversalTime().ToString('o'); stages = $summary }
$result | ConvertTo-Json -Depth 8 | Set-Content -Path (Join-Path $evidenceRoot 'validation-summary.json') -Encoding utf8
Write-Host "Evidence written to $evidenceRoot"
Write-Host 'A runtime stage is PASSED only when its command completed and its log is present; NOT_RUN and BLOCKED are never promoted automatically.'
