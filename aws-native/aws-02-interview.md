# AWS-02 interview evidence

## 30–60 second explanation

> I built one shared Python ingestion path for PostgreSQL snapshots, paginated
> REST APIs, and CSV/JSON reference files. Every run uses an explicit UTC
> `[start, end)` interval and a deterministic UUID, then preserves the business
> fields while adding provenance before writing real Snappy Parquet. Publication
> is data first and a conditional manifest last, so readers only see committed
> datasets. A retry verifies the stored checksum; identical content is a no-op,
> while changed content under the same identity is a conflict. Airflow runs the
> three source groups and passes only compact summaries. Terraform keeps the
> thin shared-code Lambda off by default and scopes it to the source, Bronze and
> quarantine prefixes. Everything here is offline tested; AWS runtime is still
> explicitly deferred.

## Actual code trace

1. `orchestration/dags/aws_bronze_ingestion.py` resolves logical date separately
   from the scheduled or explicit backfill interval.
2. `mdep_ingestion/pipeline.py` dispatches to `extract_postgres`, `extract_rest`
   or `extract_file_*`; the REST extractor completes every page before returning.
3. `models.enrich_record` adds the stable envelope without replacing business
   fields, and `serialization.to_parquet` uses PyArrow/Snappy.
4. `publication.BronzePublisher` writes deterministic `data.parquet` with
   `If-None-Match: *`, verifies bytes, then writes `_COMMITTED.json` last.
5. `list_committed` exposes only manifests whose referenced payload checksum
   matches the stored object.

The real commit boundary is the manifest, not a claim of multi-object S3
atomicity.

The timestamp fields are intentionally different: the interval selects logical
source data, `source_extract_ts` belongs to the source record, and `ingested_at`
is captured from the real orchestration run once and reused across retries.

## Likely follow-ups

**How did you make ingestion idempotent?** The run UUID is derived from source,
entity, logical date and interval. Both keys are deterministic. Conditional
writes prevent overwrite. Manifest v2 verifies the stored Parquet checksum and
a second source-content checksum that excludes only `ingested_at`, so an
independent identical replay preserves the first landing while changed business
or source-provenance content remains a conflict.

**What happens when REST pagination fails?** Pages are accumulated in memory
under page/record limits, but nothing is serialized or published until the
whole extraction succeeds. A permanent page-three failure writes deterministic
quarantine evidence and no canonical Bronze manifest.

**How do you handle retries and partial publication?** Airflow retries reuse the
same interval identity. If data exists without a manifest, the retry compares
the stored bytes and completes only a matching manifest. A manifest without
data or changed bytes becomes `conflict`; it is never silently overwritten.

## Reproducible failure/recovery evidence

```powershell
uv run --project aws-native --locked python -m unittest discover `
  -s aws-native/tests -p test_aws02_publication.py -v
```

Observed locally: seven tests passed, including interrupted manifest write,
matching orphan recovery, manifest/data inconsistency, identical replay,
changed content and simulated S3 409/412 behavior.
