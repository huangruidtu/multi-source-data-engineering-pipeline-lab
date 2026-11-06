# MDEP-70 AWS-04 evidence

Evidence boundary: **LOCAL DUCKDB FIXTURE BRIDGE VERIFIED; REDSHIFT RUNTIME
DEFERRED**. No AWS resource was created or changed.

## Executed Gold behavior

The pinned environment uses dbt Core 1.11.17, dbt-duckdb 1.11.1,
dbt-redshift 1.11.2 and DuckDB 1.4.2. The stateful integration performs actual
seed, view, table, incremental and test materializations.
The final `dbt test` discovered 17 models, six seeds and 35 tests and completed
with `PASS=35`, `WARN=0`, `ERROR=0`.

Observed initial orders:

| Order | Source amount | DKK amount | Evidence |
| --- | ---: | ---: | --- |
| `ord-100` | EUR 100.00 | 748.00 | EUR/DKK 7.48 |
| `ord-200` | DKK 50.00 | 50.00 | missing customer retained |
| `ord-300` | EUR 30.00 | null | missing rate retained and flagged |

The incremental scenarios observed one insert, one newer order update, two FX
corrections and one explicitly verified hard delete. One FX correction retained
the same `retrieved_at` but carried newer AWS-03 extraction/ingestion provenance;
EUR 100.00 changed from DKK 748.00 to 750.00 in both fact and daily mart. A 9.99
rate with older provenance changed neither. Exact order replay and stale order
replay each changed zero logical rows. Removing an order from an ordinary feed
deleted zero rows. The delete operation failed without snapshot proof, accepted
a 25% verified delete, and rejected a 100% mass-delete attempt.

Final current-state reconciliation was three source keys and three Gold keys,
zero source/Gold duplicate keys, one customer orphan, zero missing FX after the
correction, DKK source total 50.00, EUR source total 140.00, and known converted
total DKK 1,100.00. Daily sales remained grouped by date and source currency.

## Redshift and infrastructure evidence

- `dbt parse --target redshift` succeeds with the pinned Redshift adapter.
- Static tests verify the external-schema bootstrap maps only the existing
  `ref_exchange_rates` and `ref_locations` tables and never issues table or Glue
  database creation.
- `fct_orders` declares Redshift `merge`, `order_id` uniqueness, strict newer-row
  selection and no implicit delete hook.
- The Redshift role now lists/reads only `iceberg/*`, has scoped Glue reads and
  KMS decrypt only through S3.
- Redshift remains absent by default. Explicit enablement still requires private
  networking and a budget recipient, fixes base/max capacity at 8 RPUs, and adds
  a monthly 16 RPU-hour usage limit with `deactivate` breach action.

## Deferred

- No Redshift endpoint, credential, AWS-05 commerce tables or authorized AWS
  deployment exists, so live Redshift compile/run, Spectrum scans, Glue/KMS IAM
  enforcement and query cost are not runtime-verified.
- DuckDB fixtures are a bounded execution bridge, not production data and not a
  substitute Silver writer.
- Full-snapshot certification is an operational control passed to the deletion
  macro; a production scheduler must derive it from an independently verified
  source snapshot rather than a human convention.
- AWS-03 does not yet publish `ref_product_categories`, so the product dimension
  retains `category_code` (including the missing-reference fixture) without
  fabricating a category relationship.
