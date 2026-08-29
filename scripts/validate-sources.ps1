[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'

function Invoke-Psql([string]$Sql) {
    return docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U lab -d commerce -At -c $Sql
}

function Assert-Equal([string]$Actual, [string]$Expected, [string]$Name) {
    if ($Actual.Trim() -ne $Expected) { throw "$Name expected $Expected but received $Actual" }
}

# The reset is intentionally executed twice to prove deterministic reseeding.
& "$PSScriptRoot/reset-sources.ps1"
& "$PSScriptRoot/reset-sources.ps1"

foreach ($table in @('customers', 'products', 'orders', 'payments')) {
    Assert-Equal (Invoke-Psql "SELECT count(*) FROM commerce.$table;") '3' "seed count for $table"
}
Assert-Equal (Invoke-Psql 'SHOW wal_level;') 'logical' 'PostgreSQL logical replication setting'

# Verify the expected foreign-key violation is rejected without altering source data.
Get-Content -Raw "$PSScriptRoot/../source-data/postgres/examples/foreign-key-violation.sql" | docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U lab -d commerce
if ($LASTEXITCODE -eq 0) { throw 'Expected foreign-key violation was accepted.' }

# Exercise deterministic source INSERT, UPDATE, and DELETE examples.
Get-Content -Raw "$PSScriptRoot/../source-data/postgres/examples/cdc-mutations.sql" | docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U lab -d commerce
Assert-Equal (Invoke-Psql "SELECT order_status FROM commerce.orders WHERE order_id = 'ord-200';") 'completed' 'updated order status'
Assert-Equal (Invoke-Psql "SELECT count(*) FROM commerce.customers WHERE customer_id = 'cust-300';") '0' 'deleted customer'
Assert-Equal (Invoke-Psql "SELECT count(*) FROM commerce.customers WHERE customer_id = 'cust-400';") '1' 'inserted customer'

$page = Invoke-RestMethod 'http://localhost:8080/v1/exchange-rates?page=1&page_size=2'
if ($page.items.Count -ne 2 -or $page.next_page -ne 2) { throw 'REST pagination response did not match the deterministic contract.' }

foreach ($scenario in @('retryable', 'rate_limit')) {
    $response = Invoke-WebRequest "http://localhost:8080/v1/locations?scenario=$scenario" -SkipHttpErrorCheck
    $expected = if ($scenario -eq 'retryable') { 503 } else { 429 }
    if ($response.StatusCode -ne $expected -or $response.Headers['Retry-After'] -ne '1') { throw "$scenario response did not match contract." }
}

if (-not (Test-Path "$PSScriptRoot/../source-data/files/valid/product_categories.csv")) { throw 'Missing valid CSV fixture.' }
if (-not (Test-Path "$PSScriptRoot/../source-data/files/invalid/product_categories_invalid.csv")) { throw 'Missing invalid CSV fixture.' }
if ((Get-FileHash "$PSScriptRoot/../source-data/files/valid/product_categories.csv").Hash -ne (Get-FileHash "$PSScriptRoot/../source-data/files/valid/product_categories_duplicate.csv").Hash) { throw 'Duplicate CSV content mismatch.' }
Get-Content -Raw "$PSScriptRoot/../source-data/files/valid/device-reference.json" | ConvertFrom-Json | Out-Null
$malformedParsed = $true
try { Get-Content -Raw "$PSScriptRoot/../source-data/files/invalid/device-reference-malformed.json" | ConvertFrom-Json | Out-Null } catch { $malformedParsed = $false }
if ($malformedParsed) { throw 'Malformed JSON unexpectedly parsed.' }
if (Test-Path "$PSScriptRoot/../source-data/files/scenarios/expected-location-overrides.csv") { throw 'Missing-file scenario must remain absent.' }

Write-Host 'MDEP-7 source validation passed.'
