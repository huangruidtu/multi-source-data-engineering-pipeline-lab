# MDEP-9 runbook

## Prerequisites and input

Use Java, Spark 3.5, and an internet/artifact-cache path for `iceberg-spark-runtime-3.5_2.12:1.6.1`. First generate MDEP-8 Bronze inputs with its documented runtime, or point at an existing `BRONZE_LOCAL_ROOT`. The MDEP-8 Docker runtime is implemented but its full Docker/Airflow/PostgreSQL acceptance run remains unvalidated.

For a local learning run, use the same HadoopCatalog with a filesystem warehouse. For V1-like storage, replace both locations with an accessible `s3a://` URI and AWS credentials; this Story does not create a bucket.

```powershell
$bronze = "file:///$((Resolve-Path build/mdep-8-bronze).Path -replace '\\','/')"
$warehouse = "file:///$((Resolve-Path build).Path -replace '\\','/')/mdep-9-iceberg"
./scripts/run-mdep-9-silver.ps1 -BronzeRoot $bronze -Warehouse $warehouse -LogicalDate 2025-02-01 -Inspect -SkewExercise
```

Expected output includes both entity valid/quarantine counts, formatted plans, partition counts, and Iceberg snapshot rows. The job should reject no valid MDEP-7 REST fixtures; add intentionally invalid Bronze rows to exercise rejection.

## Inspect and replay

Run the same command twice. The second execution should keep one logical row per natural key after the Iceberg merge. Inspect Bronze files, Quarantine output, and Iceberg metadata locally:

```powershell
Get-ChildItem -Recurse build/mdep-8-bronze/bronze
Get-ChildItem -Recurse build/mdep-8-bronze/quarantine/silver
Get-ChildItem -Recurse build/mdep-9-iceberg
```

For a bounded incremental exercise, choose MDEP-8 `ingested_at` timestamps and invoke Spark directly:

```powershell
spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1 processing/spark/silver_batch.py --bronze-root $bronze --warehouse $warehouse --logical-date 2025-02-01 --start 2025-02-01T00:00:00Z --end 2025-02-02T00:00:00Z --inspect
```

The start is inclusive and end exclusive. This is a processing/landing boundary—not a streaming watermark.

## Fault and evolution exercises

Copy a Bronze Parquet record with the same natural key and an older `retrieved_at`/`updated_at`: expect it in Quarantine as the non-winner. Set a rate to `0`, currency to the same base/quote, blank `location_id`, invalid timestamp, or country `XX`: expect the exact rule reason and original payload preserved. Run:

```powershell
./scripts/run-mdep-9-silver.ps1 -BronzeRoot $bronze -Warehouse $warehouse -AdditiveSchemaEvolution -Inspect
```

This adds only `source_note`; inspect the table schema and snapshot metadata afterward. Do not use it for destructive changes.

## Troubleshooting

`ClassNotFoundException: IcebergSparkSessionExtensions` means the Iceberg package was not supplied or its Spark/Scala version mismatches. `No FileSystem for scheme s3a` means the Hadoop AWS filesystem/JAR/credentials are absent; use a local `file:///` exercise or configure the approved S3A client. `Path does not exist` means MDEP-8 Bronze has not produced the selected source/entity path. A merge failure after an existing additive column usually means staged columns do not match the table; rerun with the same evolution flag for that experiment.

## Actual validation state in this implementation turn

Python compilation, five standard-library contract tests, and PowerShell parser validation succeeded. Full validation items 1–18 are **UNVALIDATED** here because the host has no Java, `spark-submit`, Docker, or S3/Iceberg runtime. Run the commands above in a suitable environment before describing runtime behavior as observed.
