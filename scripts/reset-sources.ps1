[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'

docker compose down --volumes --remove-orphans
docker compose up --build --wait
