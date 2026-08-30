[CmdletBinding()]
param(
    [string]$OutputPath
)

$ErrorActionPreference = 'Stop'

function Test-CommandAvailable {
    param([string]$Name)
    return [bool](Get-Command $Name -ErrorAction SilentlyContinue)
}

function Get-CommandVersion {
    param([string]$Name, [string[]]$Arguments = @('--version'))
    if (-not (Test-CommandAvailable $Name)) { return $null }
    try { return ((& $Name @Arguments 2>&1 | Select-Object -First 1) -join ' ').Trim() }
    catch { return 'available (version unavailable)' }
}

function Get-EnvironmentPresence {
    param([string[]]$Names)
    $present = @($Names | Where-Object { -not [string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($_)) })
    return [pscustomobject]@{ available = ($present.Count -eq $Names.Count); present = $present }
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$requiredFiles = @(
    'docker-compose.yml',
    'scripts/validate-mdep-8-runtime.ps1',
    'scripts/run-mdep-9-silver.ps1',
    'scripts/validate-mdep-10-cdc.ps1',
    'scripts/validate-mdep-11-flink-cdc.ps1',
    'warehouse/snowflake/01_setup.sql',
    'analytics/dbt/dbt_project.yml',
    'validation/mdep-13-validation-matrix.yml',
    'validation/failure-scenarios.yml'
)
$missingFiles = @($requiredFiles | Where-Object { -not (Test-Path (Join-Path $repoRoot $_)) })
$dockerAvailable = Test-CommandAvailable docker
$composeAvailable = $false
if ($dockerAvailable) {
    try { $composeAvailable = ((& docker compose version 2>$null) -match 'Docker Compose') }
    catch { $composeAvailable = $false }
}
$snowflakeCredentials = Get-EnvironmentPresence @('SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER')
$awsCredentials = Get-EnvironmentPresence @('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY')

$report = [ordered]@{
    generated_at_utc = (Get-Date).ToUniversalTime().ToString('o')
    repository_root = $repoRoot
    capabilities = [ordered]@{
        docker = [ordered]@{ status = if ($dockerAvailable) { 'AVAILABLE' } else { 'BLOCKED' }; version = Get-CommandVersion docker @('version', '--format', '{{.Client.Version}}') }
        docker_compose = [ordered]@{ status = if ($composeAvailable) { 'AVAILABLE' } else { 'BLOCKED' }; version = if ($composeAvailable) { Get-CommandVersion docker @('compose', 'version') } else { $null } }
        python = [ordered]@{ status = if (Test-CommandAvailable python) { 'AVAILABLE' } else { 'BLOCKED' }; version = Get-CommandVersion python }
        java = [ordered]@{ status = if (Test-CommandAvailable java) { 'AVAILABLE' } else { 'BLOCKED' }; version = Get-CommandVersion java @('-version') }
        spark_submit = [ordered]@{ status = if (Test-CommandAvailable spark-submit) { 'AVAILABLE' } else { 'BLOCKED' }; version = Get-CommandVersion spark-submit }
        powershell = [ordered]@{ status = if (Test-CommandAvailable pwsh) { 'AVAILABLE' } else { 'AVAILABLE' }; version = $PSVersionTable.PSVersion.ToString() }
        dbt = [ordered]@{ status = if (Test-CommandAvailable dbt) { 'AVAILABLE' } else { 'BLOCKED' }; version = Get-CommandVersion dbt }
        snowflake_cli = [ordered]@{ status = if (Test-CommandAvailable snow) { 'AVAILABLE' } else { 'BLOCKED' }; version = Get-CommandVersion snow }
        snowflake_credentials = [ordered]@{ status = if ($snowflakeCredentials.available) { 'AVAILABLE' } else { 'BLOCKED' }; present_variables = $snowflakeCredentials.present }
        aws_s3_credentials = [ordered]@{ status = if ($awsCredentials.available) { 'AVAILABLE' } else { 'BLOCKED' }; present_variables = $awsCredentials.present }
        repository_assets = [ordered]@{ status = if ($missingFiles.Count -eq 0) { 'AVAILABLE' } else { 'BLOCKED' }; missing_files = $missingFiles }
    }
}

$json = $report | ConvertTo-Json -Depth 8
if ($OutputPath) {
    $directory = Split-Path -Parent $OutputPath
    if ($directory) { New-Item -ItemType Directory -Force -Path $directory | Out-Null }
    Set-Content -Path $OutputPath -Value $json -Encoding utf8
}

$report.capabilities.GetEnumerator() | ForEach-Object {
    Write-Host ('{0}: {1}' -f $_.Key, $_.Value.status.ToUpperInvariant())
}

[pscustomobject]$report
