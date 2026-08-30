# MDEP-11 runbook

`processing/flink/Dockerfile` is the reproducible package path. It builds on `flink:1.20.0-scala_2.12-java17`, installs `apache-flink==1.20.0`, downloads `flink-connector-kafka-3.3.0-1.20.jar` and `iceberg-flink-runtime-1.20-1.6.1.jar`, and enables Flink's bundled `flink-s3-fs-hadoop-1.20.0.jar`. Supply normal AWS/S3A credentials and a writable bucket; this repository does not create one.

```powershell
$env:AWS_ACCESS_KEY_ID = '<your access key>'
$env:AWS_SECRET_ACCESS_KEY = '<your secret>'
./scripts/validate-mdep-11-flink-cdc.ps1 -Bucket '<your bucket>'
```

The validator builds the exact image, starts dependencies, checks Connect and JobManager, then submits the Python job directly:

```text
flink run -py /opt/flink/usrlib/mdep/flink_cdc_job.py
```

It has no placeholder JAR. After submission, create a PostgreSQL customer insert/update/delete, replay lower-LSN and exact records, send a malformed envelope and allow a tombstone. Inspect `bronze/cdc/<entity>/event_date=...`, `quarantine/cdc/...`, `mdep.silver.core_*`, checkpoint recovery, and Iceberg snapshots.

This is a reproducible **implementation path**, not runtime evidence. Docker, S3 credentials, connector startup, and physical Iceberg commits are unvalidated on the current host.
