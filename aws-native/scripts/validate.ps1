[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$terraformRoot = Join-Path $repositoryRoot 'aws-native\terraform'

if (-not (Get-Command terraform -ErrorAction SilentlyContinue)) {
    throw 'terraform is required (>= 1.9.0).'
}
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    throw 'python is required (>= 3.11).'
}

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
    python -m unittest discover -s aws-native/tests -v
    if ($LASTEXITCODE -ne 0) { throw 'AWS-native Python policy tests failed.' }
}
finally {
    Pop-Location
}

Write-Output 'AWS-01 offline validation passed. No AWS resources were deployed.'
