# AWS-03 interview explanation

## 45-60 seconds

> AWS-02 publishes immutable Parquet and makes it visible by writing a manifest
> last. AWS-03 never scans Bronze blindly: it receives an explicit manifest,
> verifies its version, deterministic identity, safe path, row count and both
> checksums, and only then gives that exact Parquet file to Spark. The shared
> transformation code normalizes and validates exchange rates and locations,
> deterministically quarantines bad or duplicate rows, and executes a real
> Iceberg `MERGE INTO`. Freshness starts with the source business timestamp, so
> a late-arriving old record cannot overwrite newer Silver state; ingestion time
> and finally the hash only resolve otherwise tied versions. Locally this runs
> Spark 3.5.6 and Iceberg 1.10 with HadoopCatalog. The opt-in AWS job pins Glue
> 5.1 and swaps in GlueCatalog plus S3FileIO. Linux CI proves real Iceberg
> commits and replay behavior; AWS execution remains honestly deferred.

## Code trace

1. [`publication.py`](mdep_ingestion/publication.py) writes real Parquet, then
   `_COMMITTED.json` as the visibility boundary.
2. [`manifest.py`](mdep_silver/manifest.py) distrusts the manifest's `data_key`
   until identity/path/checksum/count verification succeeds.
3. [`spark_job.py`](mdep_silver/spark_job.py) applies DataFrame rules, stable
   deduplication, retry-safe quarantine and executable Iceberg `MERGE INTO`.
4. [`aws03_silver_job.py`](glue/aws03_silver_job.py) is the Glue entry point;
   [`glue.tf`](terraform/glue.tf) supplies GlueCatalog/S3FileIO configuration.
5. [`test_real_iceberg_pipeline.py`](spark/tests/test_real_iceberg_pipeline.py)
   publishes AWS-02 Bronze, verifies it, merges, queries tables and snapshots,
   tests replay/update/stale data, and simulates acknowledgment failure.

## Design choices

- **Glue rather than a managed EMR cluster:** this bounded scheduled reference
  workload benefits from a job runtime, capped workers and no idle cluster. EMR
  becomes attractive for continuously busy or deeply customized Spark fleets.
- **Iceberg rather than loose Parquet:** Parquet is the file format; Iceberg adds
  atomic snapshots, table metadata and row-level `MERGE`, making replay and
  current-state upserts inspectable instead of relying on file replacement.
- **Tie policy:** business timestamp, then source extraction timestamp, then
  ingestion timestamp, then hash. Hash is deterministic conflict resolution,
  never evidence that an older row is fresher. Missing snapshot rows are not
  deletes because no deletion contract exists for these feeds.
- **Failure boundary:** malformed/orphan/tampered Bronze fails before any write.
  Quarantine, business table and reconciliation are separate Iceberg commits;
  deterministic keys make recovery safe without claiming cross-table atomicity.

## Live demonstration

Run on Linux with Java 17:

```bash
export PYTHONPATH=aws-native
export PYSPARK_SUBMIT_ARGS='--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0 pyspark-shell'
uv sync --project aws-native/spark --locked --python 3.11
uv run --project aws-native/spark --locked python -m unittest discover -s aws-native/spark/tests -v
```

The log prints `AWS03_ICEBERG_EVIDENCE` with Silver rows, reconciliation counts,
quarantine count and snapshot operations, plus `AWS03_INTEGRITY_EVIDENCE` for
orphan/tamper rejection. In the demonstrated first rate batch: input `4`, two
quality-accepted rows, two invalid rows, one duplicate, one unique winner and
one insert. Replay
produces one unchanged key and no new snapshot. The later batch inserts USD and
updates DKK; a subsequent stale batch is unchanged.

AWS documents the pinned mapping in [Glue versions](https://docs.aws.amazon.com/glue/latest/dg/release-notes.html)
and [Using Iceberg in Glue](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html):
Glue 5.1 uses Spark 3.5.6, Python 3.11, Java 17 and Iceberg 1.10.0.
