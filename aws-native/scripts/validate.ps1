[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$terraformRoot = Join-Path $repositoryRoot 'aws-native\terraform'

if (-not (Get-Command terraform -ErrorAction SilentlyContinue)) {
    throw 'terraform is required (>= 1.9.0).'
}
if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
    throw 'uv is required to reproduce the locked Python 3.12 environment.'
}

uv sync --project (Join-Path $repositoryRoot 'aws-native') --locked --python 3.12
if ($LASTEXITCODE -ne 0) { throw 'uv sync failed.' }

terraform "-chdir=$terraformRoot" fmt -check -recursive
if ($LASTEXITCODE -ne 0) { throw 'terraform fmt -check failed.' }

terraform "-chdir=$terraformRoot" init -backend=false -input=false
if ($LASTEXITCODE -ne 0) { throw 'terraform init failed.' }

terraform "-chdir=$terraformRoot" validate
if ($LASTEXITCODE -ne 0) { throw 'terraform validate failed.' }

terraform "-chdir=$terraformRoot" test -no-color
if ($LASTEXITCODE -ne 0) { throw 'terraform test failed.' }

Push-Location $repositoryRoot
try {
    $env:PYTHONUTF8 = '1'
    uv run --project aws-native --locked python aws-native/scripts/validate_lambda_package.py
    if ($LASTEXITCODE -ne 0) { throw 'Lambda source-package validation failed.' }

    uv run --project aws-native --locked python -m unittest discover -s aws-native/tests -v
    if ($LASTEXITCODE -ne 0) { throw 'AWS-native ingestion/orchestration/policy tests failed.' }

    uv run --project aws-native --locked python -m unittest discover -s tests -v
    if ($LASTEXITCODE -ne 0) { throw 'Original MDEP V1 regression tests failed.' }
}
finally {
    Pop-Location
}

Write-Output 'AWS-01/AWS-02 offline validation passed. No AWS resources were deployed.'
