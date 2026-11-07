# AWS-07 integrated evidence

## Result and evidence boundary

GitHub Actions run
[#42358301810](https://github.com/huangruidtu/multi-source-data-engineering-pipeline-lab/actions/runs/42358301810)
completed successfully at commit `d10b874427a312279f1ed8300b3652474fd2f360`.
Its `integrated-local-demo` job established the observed PyArrow Parquet,
manifest-v2, Spark 3.5.6/Iceberg 1.10.0, AWS-05 DMS/Iceberg and AWS-04
dbt/DuckDB results below. The follow-up boundary correction routes those same
deterministic reference inputs through the existing AWS-02 REST extractor and
`IngestionPipeline` instead of constructing `SourceRecord` objects. An
executable regression covers publication, manifest verification, identical
replay and changed-content conflict through that path, and the PR requires the
integrated job to rerun. The baseline run also passed offline validation, the
independent real Spark/Iceberg suite and Gold/dbt validation.

This is `LOCAL_END_TO_END_VERIFIED`, not `AWS_RUNTIME_VERIFIED`. The HTTP
transport is a deterministic in-process client; PostgreSQL, DMS, Kinesis, S3,
Glue, Redshift and IAM are not contacted. Terraform remains code and offline
validation evidence only. No AWS resource was deployed.

## One-command demonstration

The exact command used by CI is:

```bash
uv run --project aws-native/spark --locked python aws-native/integration/run_demo.py
```

To retain evidence files locally, pass a new, non-existing work directory:

```bash
uv run --project aws-native/spark --locked python aws-native/integration/run_demo.py --work-dir /tmp/mdep-aws07-demo
```

Prerequisites are Linux, Java 17, Python 3.11, `uv` 0.11.21, the locked
`aws-native/spark` environment, the locked Python 3.12 `aws-native/gold`
environment, and network access for the first Maven resolution of
`org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0`. No AWS credentials
are needed. CI installs both locked environments before executing the command.

The retained directory contains:

- `objects/`: immutable Bronze Parquet, commit manifests and the deliberately
  corrupt-manifest fixture;
- `warehouse/`: HadoopCatalog Iceberg metadata and data files;
- `exports/initial/bridge-manifest.json` and
  `exports/final/bridge-manifest.json`: exact table/file checksums, row counts,
  business-key counts and the shared run identity;
- `mdep-aws07.duckdb`: the actual dbt target;
- `aws07-gold-evidence.json`: Gold rows, commands and reconciliation;
- `aws07-evidence.json`: the combined machine-readable report.

The final stdout line begins `AWS07_END_TO_END_EVIDENCE=` so CI evidence can be
captured without relying on a workstation file.

## Observed reconciliation

The green integrated job reported:

| Check | Observed result |
| --- | ---: |
| Valid Bronze commit manifests | 3 |
| Deliberately invalid manifest artifacts | 1 |
| Reference input / unique winners | 4 / 3 |
| CDC input events | 18 |
| CDC decisions | 12 inserts, 2 deletes, 2 duplicates, 1 stale, 1 conflict |
| CDC quarantine rows | 2 |
| Final source / Gold order keys | 4 / 4 |
| Source / Gold duplicate keys | 0 / 0 |
| DKK source total | 75.00 |
| EUR source total | 100.00 |
| Known converted DKK total | 835.00 |
| Missing-FX / orphan-customer orders | 1 / 1 |
| Exact dbt replay changed rows | 0 |
| Explicit verified tombstone deletes | 1 |
| dbt models / tests executed | 17 / 35 |

The EUR order is `100.00`. It first lands in Gold at `750.00` with rate 7.50,
then becomes `760.00` after the newer Silver rate 7.60. Final Gold also contains
DKK `50.00`, a DKK orphan-customer order of `25.00`, and a USD `10.00` order
whose DKK amount remains `NULL`. Daily known converted sales therefore reconcile
to `760.00 + 50.00 + 25.00 = 835.00`.

The initial and final bridge manifests each verified two customers, two
products, six durable order-ledger rows, two payments, one exchange rate and one
location. Gold sees four live orders after one pre-Gold tombstone, then deletes
one already-materialized order only after the explicit verified-tombstone gate.

## Failure and recovery evidence

- Replaying an identical Bronze logical interval returns `already_published`;
  changed business content under that same identity returns `conflict` and does
  not overwrite the committed object.
- A malformed commit manifest fails as `manifest_unreadable_or_malformed`
  before Silver changes. The prior location count remains unchanged.
- An exact CDC replay is `duplicate`; the same source position with different
  content is `conflict` and quarantined; a stale update after delete is `stale`
  and cannot resurrect the order.
- Replaying the same verified Bronze batch leaves the Iceberg winner unchanged.
- The Silver-to-Gold bridge rejects missing files, unexpected tables, path
  escapes, checksum/count/key mismatches and mixed run identities before dbt.
- A Gold incremental replay changes zero rows. Absence never implies deletion;
  a delete requires a durable AWS-05 tombstone plus the explicit
  `cdc_tombstones_verified` gate.
- Missing FX and orphan relationships are retained as evidence instead of being
  silently dropped.

## Runtime limitations and optional AWS smoke gate

Local HadoopCatalog proves Spark/Iceberg table semantics, not Glue Catalog,
S3FileIO, Lake Formation or IAM. DuckDB proves the actual dbt SQL and
incremental/reconciliation logic, not Redshift query planning, permissions or
cost. The deterministic REST client proves the production extractor/pipeline
boundary but not external endpoint authentication, throttling or networking.

An AWS smoke test is optional and must be separately approved. Before it can
run, require an explicit sandbox account and region, a reviewed plan, protected
GitHub environment approval, budget recipient, private networking and source
authorization, reviewed opt-in flags, exact manifest keys, a bounded test
dataset, evidence/log retention, and a reviewed cleanup plan. Enabling or
starting Lambda, Glue, DMS/Kinesis, or Redshift remains a separate human action;
this repository has no automatic apply or smoke deployment.
