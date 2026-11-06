# MDEP-69 AWS-03 evidence

Evidence boundary: **LOCAL LINUX SPARK/ICEBERG VERIFIED; AWS RUNTIME DEFERRED**.
No AWS resource was created or changed.

## Reproducible run

- GitHub Actions run: [42223262873](https://github.com/huangruidtu/multi-source-data-engineering-pipeline-lab/actions/runs/42223262873)
- Runtime: Linux, Java 17, Python 3.11, PySpark 3.5.6, Iceberg Spark runtime 1.10.0.
- Real integration result: 2 tests passed in 30.822 seconds; full job passed in
  52 seconds.
- Offline job in the same run: Terraform format/init/validate, 10 Terraform
  tests, Lambda packaging, 48 AWS-native tests and 62 V1 tests passed. The one
  native-Windows Airflow parse skip is executed on Linux CI in that job.

The integration creates AWS-02 Parquet and manifest v2, verifies both
checksums, transforms with Spark, executes Iceberg `MERGE INTO`, selects the
Silver state, reads Iceberg snapshot metadata, and exercises exact replay,
newer/equal-version update, stale replay, duplicate quarantine, invalid country,
optional fields and post-commit acknowledgment recovery. Corrupt and orphan
objects are rejected before Silver writes.

The latest run prints machine-readable `AWS03_ICEBERG_EVIDENCE` so sample rows,
reconciliation metrics, quarantine totals and snapshot operations are retained
in the CI log rather than inferred from documentation.

Observed state was EUR/DKK `7.4800000004` and EUR/USD `1.0800000000` at the
declared compound grain. The first batch reconciled four inputs to two valid
candidates, two invalid rows, one duplicate/one unique winner and one insert.
The newer batch inserted one key and updated one; both exact replay and stale
replay reported one unchanged key. Snapshot operations were `append`,
`overwrite`, `overwrite`; exact and stale no-op replays created no empty
snapshot. Four retry-safe quarantine rows remained across rate and location
scenarios.

## Deferred

- Glue 5.1 execution, Glue Catalog commits, S3FileIO, IAM and KMS are coded but
  not runtime-verified because deployment was not authorized.
- Manifest verification reads the complete bounded object (128 MiB default).
  A production-scale design should verify immutable multipart checksums without
  loading the full object in driver memory.
- There is no automatic Glue trigger; an approved orchestrator must pass
  explicit committed manifest keys.
