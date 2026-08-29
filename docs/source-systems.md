# Local Source Systems

**Status:** IMPLEMENTED for MDEP-7. No ingestion or downstream processing is included.

## Services

| Service | Address | Purpose |
| --- | --- | --- |
| PostgreSQL 16 | `localhost:5432` | reproducible `commerce` schema and source mutations |
| REST source | `http://localhost:8080` | deterministic exchange-rate and location reference data |

## Commands

Run these commands from the repository root in PowerShell.

```powershell
docker compose up --build --wait
.\scripts\reset-sources.ps1
.\scripts\validate-sources.ps1
docker compose exec postgres psql -U lab -d commerce -c "SELECT * FROM commerce.orders ORDER BY order_id;"
Invoke-RestMethod 'http://localhost:8080/v1/exchange-rates?page=1&page_size=2'
docker compose down
```

`reset-sources.ps1` removes the Compose PostgreSQL volume and recreates both services. It is intentionally destructive to local lab data only.

## PostgreSQL source

The `commerce` schema has `customers`, `products`, `orders`, and `payments`. It contains three deterministic seed rows per table. `orders.customer_id` and `payments.order_id` are foreign keys. The service starts with `wal_level=logical`, which is a prerequisite for the later Debezium Story; this Story does not create a connector.

Use the following files for controlled source changes:

- `source-data/postgres/examples/cdc-mutations.sql` — one INSERT, UPDATE, and DELETE.
- `source-data/postgres/examples/foreign-key-violation.sql` — an expected failed insert.
- `source-data/postgres/examples/add-customer-loyalty-tier.sql` — an additive schema-change exercise, excluded from resets.

## REST source

`GET /v1/exchange-rates` and `GET /v1/locations` accept `page` and `page_size` (default `1` and `2`). Both return `items`, `total_items`, and `next_page`.

Append one of these deterministic scenarios to either endpoint:

| Query | Expected result |
| --- | --- |
| `scenario=retryable` | `503` with `Retry-After: 1` |
| `scenario=rate_limit` | `429` with `Retry-After: 1` |
| `scenario=timeout` | delays for three seconds before returning the normal payload |

## File fixtures

| Fixture | Purpose |
| --- | --- |
| `source-data/files/valid/product_categories.csv` | valid CSV reference data |
| `source-data/files/valid/product_categories_duplicate.csv` | byte-for-byte duplicate content |
| `source-data/files/invalid/product_categories_invalid.csv` | duplicate key, null key, and bad numeric type |
| `source-data/files/valid/device-reference.json` | valid JSON reference data |
| `source-data/files/invalid/device-reference-invalid.json` | semantic null/type/reference errors |
| `source-data/files/invalid/device-reference-malformed.json` | syntactically malformed JSON |
| `source-data/files/scenarios/expected-location-overrides.csv` | intentionally absent missing-file scenario |

## Validation record

`scripts/validate-sources.ps1` is the full deterministic validation: it resets twice, checks counts and `wal_level`, executes CDC mutations, tests pagination/503/429, and checks fixture scenarios. It must be run with Docker available. The REST unit tests can run without Docker:

```powershell
python -m unittest discover -s source-data/rest-api -p test_*.py
```

### Validation performed on 2026-08-30

- REST unit tests: 3 passed.
- REST service started locally with Python; `/health`, paginated exchange rates, `503` retryable response, `429` rate-limit response, and the client timeout scenario were exercised successfully.
- Fixture inspection passed: 3 valid CSV rows, 4 invalid CSV rows, 2 valid JSON rows, malformed JSON rejection, matching duplicate-file hashes, and the intentionally absent file path.
- PostgreSQL container start/reset, row-count, mutation, and constraint validation were **not run** on this host because neither Docker nor `psql` is installed. The Compose configuration and the complete validation script are included for execution on a Docker-capable development machine.
