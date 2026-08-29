# MDEP-7 Runbook — Source Lab Operations

## Prerequisites

Run from the repository root on a machine with Docker Compose and Python. PostgreSQL is accessed inside the container, so a host `psql` installation is not required when Docker is available. The original implementation host did **not** have Docker or `psql`; only the non-container validation listed below was completed there.

## Start, stop, reset, inspect

```powershell
# Start both services and wait for health checks
docker compose up --build --wait

# Reset local lab data: removes only this Compose volume, then rebuilds services
.\scripts\reset-sources.ps1

# Inspect deterministic database rows
docker compose exec postgres psql -U lab -d commerce -c "SELECT * FROM commerce.orders ORDER BY order_id;"

# Inspect REST health and page 1
Invoke-RestMethod 'http://localhost:8080/health'
Invoke-RestMethod 'http://localhost:8080/v1/exchange-rates?page=1&page_size=2'

# Stop services without deleting the volume
docker compose down
```

Expected: `/health` returns `status: ok`; exchange-rate page 1 contains two items and `next_page` equals 2; reset restores three rows in each source table.

## Validate and test

```powershell
# Executes two resets, row counts, WAL setting, mutation/FK checks, REST checks, and fixture checks
.\scripts\validate-sources.ps1

# REST unit tests only; can run without Docker
python -m unittest discover -s source-data/rest-api -p test_*.py
```

Expected: the complete script ends with `MDEP-7 source validation passed.` The Python suite contains three pagination tests.

## Reproduce failure scenarios

```powershell
# Expected PostgreSQL foreign-key failure
Get-Content -Raw source-data/postgres/examples/foreign-key-violation.sql |
  docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U lab -d commerce

# Controlled source insert/update/delete
Get-Content -Raw source-data/postgres/examples/cdc-mutations.sql |
  docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U lab -d commerce

# Add a source field; intentionally excluded from reset seed
Get-Content -Raw source-data/postgres/examples/add-customer-loyalty-tier.sql |
  docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U lab -d commerce

# HTTP failure behavior
Invoke-WebRequest 'http://localhost:8080/v1/locations?scenario=retryable' -SkipHttpErrorCheck
Invoke-WebRequest 'http://localhost:8080/v1/locations?scenario=rate_limit' -SkipHttpErrorCheck
Invoke-WebRequest 'http://localhost:8080/v1/locations?scenario=timeout' -TimeoutSec 1
```

Expected: foreign-key insert fails; retryable returns 503 and `Retry-After: 1`; rate limit returns 429 and the same header; timeout client exits before the server's three-second delayed response.

## Recover and verify recovery

After mutations or the schema-addition exercise, run `reset-sources.ps1`. Then inspect counts and run the validation script. The reset deletes the local Compose volume, so it is recovery for this synthetic lab—not a production data-recovery technique.

## Troubleshooting

| Symptom | Likely cause | Action |
| --- | --- | --- |
| `docker` not recognized | Docker Desktop/CLI not installed or not on PATH | Install/start Docker on a suitable development machine, then rerun Compose validation |
| `--wait` never completes | a service failed health check or port is occupied | run `docker compose logs`; check ports 5432/8080 |
| `psql` statement fails | expected FK exercise or prior mutation state | reset, then run the specific SQL file again |
| Pagination returns 400 | invalid page/page_size | use page >= 1 and page_size 1–100 |
| REST timeout test does not time out | client timeout is >= 3 seconds | use `-TimeoutSec 1` |
| fixture unexpectedly parses | wrong fixture path | use `invalid/device-reference-malformed.json` |

## Validation record

Validated on the implementation host: REST unit tests (3/3), live health/pagination/503/429/timeout behavior, fixture inspection, PowerShell syntax, and diff checks. Not validated there: Docker Compose startup/reset, PostgreSQL counts/mutations/FK exercise, and runtime `SHOW wal_level`, because Docker and `psql` were absent.
