[CmdletBinding()]
param(
    [switch]$RunRuntime,
    [switch]$SelfTest,
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
    param(
        [string]$Id,
        [ValidateSet('PASSED', 'FAILED', 'BLOCKED', 'NOT_RUN')][string]$Status,
        [string]$Evidence,
        [Nullable[int]]$ExitCode,
        [string]$Note
    )
    $summary.Add([ordered]@{ id = $Id; actual_status = $Status; evidence_path = $Evidence; exit_code = $ExitCode; note = $Note })
}

function Invoke-RecordedCommand {
    param([string]$Id, [string]$WorkingDirectory, [scriptblock]$Command, [string]$EvidenceDirectory)

    New-Item -ItemType Directory -Force -Path $EvidenceDirectory | Out-Null
    $stdoutPath = Join-Path $EvidenceDirectory "$Id.stdout.log"
    $stderrPath = Join-Path $EvidenceDirectory "$Id.stderr.log"
    Set-Content -Path $stdoutPath -Value '' -Encoding utf8
    Set-Content -Path $stderrPath -Value '' -Encoding utf8
    "`n[$((Get-Date).ToUniversalTime().ToString('o'))] $Id (cwd=$WorkingDirectory)" | Add-Content -Path $logPath

    $exitCode = $null
    $failure = $null
    $previousLocation = Get-Location
    try {
        # Do not let a prior native command's code leak into this stage.
        $global:LASTEXITCODE = 0
        Set-Location -LiteralPath $WorkingDirectory
        & $Command 1> $stdoutPath 2> $stderrPath
        $exitCode = $global:LASTEXITCODE
        if ($exitCode -ne 0) { throw "Recorded command exited with code $exitCode." }
    } catch {
        $failure = $_
        if ($exitCode -eq 0) { $exitCode = $null }
        $_ | Out-String | Add-Content -Path $stderrPath
    } finally {
        Set-Location -LiteralPath $previousLocation
        Get-Content -Raw -Path $stdoutPath | Add-Content -Path $logPath
        Get-Content -Raw -Path $stderrPath | Add-Content -Path $logPath
    }

    if ($failure) {
        Add-StageResult $Id 'FAILED' $stdoutPath $exitCode $failure.Exception.Message
    } else {
        Add-StageResult $Id 'PASSED' $stdoutPath $exitCode 'Command completed with exit code 0.'
    }
}

function Write-ValidationSummary {
    $result = [ordered]@{ run_id = $RunId; generated_at_utc = (Get-Date).ToUniversalTime().ToString('o'); stages = $summary }
    $result | ConvertTo-Json -Depth 8 | Set-Content -Path (Join-Path $evidenceRoot 'validation-summary.json') -Encoding utf8
}

function Invoke-RunnerSelfTest {
    $selfTestEvidence = Join-Path $evidenceRoot 'runner-self-test'
    Invoke-RecordedCommand 'SELFTEST-SUCCESS' $repoRoot { & $env:ComSpec /c exit 0 } $selfTestEvidence
    Invoke-RecordedCommand 'SELFTEST-NATIVE-FAILURE' $repoRoot { & $env:ComSpec /c exit 7 } $selfTestEvidence
    Invoke-RecordedCommand 'SELFTEST-POWERSHELL-THROW' $repoRoot { throw 'Expected self-test PowerShell failure.' } $selfTestEvidence
    Add-StageResult 'SELFTEST-BLOCKED' 'BLOCKED' $null $null 'Intentional self-test blocked stage.'
    Add-StageResult 'SELFTEST-NOT-RUN' 'NOT_RUN' $null $null 'Intentional self-test not-run stage.'
    Write-ValidationSummary

    $results = @{}
    $summary | ForEach-Object { $results[$_.id] = $_ }
    if ($results['SELFTEST-SUCCESS'].actual_status -ne 'PASSED' -or -not (Test-Path $results['SELFTEST-SUCCESS'].evidence_path)) { throw 'Self-test success stage was not evidenced as PASSED.' }
    if ($results['SELFTEST-NATIVE-FAILURE'].actual_status -ne 'FAILED' -or $results['SELFTEST-NATIVE-FAILURE'].exit_code -ne 7) { throw 'Self-test native non-zero exit was not FAILED with exit code 7.' }
    if ($results['SELFTEST-POWERSHELL-THROW'].actual_status -ne 'FAILED') { throw 'Self-test PowerShell exception was not FAILED.' }
    if ($results['SELFTEST-BLOCKED'].actual_status -ne 'BLOCKED') { throw 'Self-test BLOCKED semantics changed.' }
    if ($results['SELFTEST-NOT-RUN'].actual_status -ne 'NOT_RUN') { throw 'Self-test NOT_RUN semantics changed.' }
}

if ($SelfTest) {
    Invoke-RunnerSelfTest
    Write-Host "Runner self-test evidence written to $evidenceRoot"
    exit 0
}

$preflightPath = Join-Path $evidenceRoot 'preflight.json'
$preflight = & (Join-Path $PSScriptRoot 'preflight-mdep-13.ps1') -OutputPath $preflightPath
$preflight | ConvertTo-Json -Depth 8 | Set-Content -Path $preflightPath -Encoding utf8
$PSVersionTable | Out-String | Set-Content -Path (Join-Path $evidenceRoot 'environment.txt') -Encoding utf8

if ($preflight.capabilities.python.status -eq 'AVAILABLE') {
    Invoke-RecordedCommand 'STATIC-PYTHON-TESTS' $repoRoot { python -m unittest discover -s tests -p 'test_*.py' -v } $evidenceRoot
} else {
    Add-StageResult 'STATIC-PYTHON-TESTS' 'BLOCKED' $null $null 'python is unavailable.'
}

if (-not $RunRuntime) {
    'M8-AIRFLOW-BATCH','M9-SPARK-ICEBERG','M10-DEBEZIUM-KAFKA','M11-FLINK-CDC','M12-SNOWFLAKE-DBT' | ForEach-Object { Add-StageResult $_ 'NOT_RUN' $null $null 'This invocation did not request -RunRuntime.' }
} else {
    if ($preflight.capabilities.docker.status -eq 'AVAILABLE' -and $preflight.capabilities.docker_compose.status -eq 'AVAILABLE') {
        Invoke-RecordedCommand 'M8-AIRFLOW-BATCH' $repoRoot { & scripts/validate-mdep-8-runtime.ps1 } (Join-Path $evidenceRoot 'batch')
        Invoke-RecordedCommand 'M10-DEBEZIUM-KAFKA' $repoRoot { & scripts/validate-mdep-10-cdc.ps1 } (Join-Path $evidenceRoot 'cdc')
        if ($Bucket -and $preflight.capabilities.aws_s3_credentials.status -eq 'AVAILABLE') {
            Invoke-RecordedCommand 'M11-FLINK-CDC' $repoRoot { & scripts/validate-mdep-11-flink-cdc.ps1 -Bucket $Bucket } (Join-Path $evidenceRoot 'cdc')
        } else { Add-StageResult 'M11-FLINK-CDC' 'BLOCKED' $null $null 'Bucket and AWS credentials are required.' }
    } else {
        'M8-AIRFLOW-BATCH','M10-DEBEZIUM-KAFKA','M11-FLINK-CDC' | ForEach-Object { Add-StageResult $_ 'BLOCKED' $null $null 'Docker Compose is unavailable.' }
    }
    if ($preflight.capabilities.spark_submit.status -eq 'AVAILABLE' -and $BronzeRoot -and $Warehouse) {
        Invoke-RecordedCommand 'M9-SPARK-ICEBERG' $repoRoot { & scripts/run-mdep-9-silver.ps1 -BronzeRoot $BronzeRoot -Warehouse $Warehouse -Inspect } (Join-Path $evidenceRoot 'silver')
    } else { Add-StageResult 'M9-SPARK-ICEBERG' 'BLOCKED' $null $null 'spark-submit, BronzeRoot, and Warehouse are required.' }
    if ($preflight.capabilities.dbt.status -eq 'AVAILABLE' -and $preflight.capabilities.snowflake_credentials.status -eq 'AVAILABLE') {
        Invoke-RecordedCommand 'M12-SNOWFLAKE-DBT' (Join-Path $repoRoot 'analytics/dbt') {
            foreach ($step in @('deps', 'debug', 'parse', 'compile', 'run', 'test', 'source freshness')) {
                $arguments = $step -split ' '
                & dbt @arguments
                if ($LASTEXITCODE -ne 0) { throw "dbt $step exited with code $LASTEXITCODE." }
            }
        } (Join-Path $evidenceRoot 'gold')
    } else { Add-StageResult 'M12-SNOWFLAKE-DBT' 'BLOCKED' $null $null 'dbt and Snowflake credentials are required.' }
}

Write-ValidationSummary
Write-Host "Evidence written to $evidenceRoot"
Write-Host 'PASSED requires exit code 0 and an evidence log. BLOCKED describes unavailable capability; NOT_RUN describes a stage intentionally not attempted by this invocation.'
