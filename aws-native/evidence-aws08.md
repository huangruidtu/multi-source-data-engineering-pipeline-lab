# AWS-08 read-only backfill-validation evidence

## Result and claim boundary

AWS-08 adds one optional use case: an independent, read-only validator for an
approved historical reference-data interval. It consumes explicit AWS-02
commit-manifest keys, independently verifies their Parquet evidence, and
reconciles those historical rows against the current AWS-03 Iceberg state for
`ref_exchange_rates` and `ref_locations`. It does not run `MERGE`, update the
catalog, create checkpoints, or compete with AWS-03 as the canonical writer.

The real Linux command is:

```bash
uv run --project aws-native/spark --locked python aws-native/integration/run_backfill_validation.py
```

The expected claim is `LOCAL_SPARK_ICEBERG_VERIFIED` after the mandatory Linux
job succeeds. Native Windows Spark startup is not evidence: the available
Windows Hadoop/Netty runtime failed before the application started, so no
Windows Spark result is claimed. No AWS resource was deployed and EMR
Serverless remains `AWS_RUNTIME_DEFERRED`.

## Executed path

The executable regression runs the actual AWS-02 REST extractor and
`IngestionPipeline`, producing real PyArrow Parquet and manifest-v2 commit
objects. The existing AWS-03 verified-manifest reader and canonical writer then
create real Iceberg tables and snapshots with Spark 3.5.6 and Iceberg 1.10.0.
Only after that write completes does the independent AWS-08 validator read the
same explicit manifests and current Iceberg state.

The successful report must show:

- two verified manifests and both required reference entities;
- historical input counts and distinct business-key counts agreeing;
- each historical key either exactly present or superseded by a strictly newer
  state under the existing AWS-03 freshness expression;
- no duplicate or invalid historical/current keys;
- current snapshot IDs and source-content checksums in deterministic JSON;
- an identical validation replay producing byte-identical report JSON.

The suite then publishes a valid newer EUR/DKK rate through the existing
AWS-03 writer and proves that the earlier historical rate is classified as
`superseded_by_newer_count=1`, rather than falsely failing because current
Silver is newer.

## Fail-closed scenarios

The Linux regression independently exercises and expects bounded reason tokens
for:

- a missing historical key in current Silver;
- duplicate current business keys;
- invalid current reference values or provenance;
- duplicate historical business keys;
- invalid historical reference values;
- a corrupt or malformed commit manifest.

No failure message contains source records or payload content. A failed
validation produces no canonical Silver mutation. The optional EMR entry point
can publish only the deterministic validation report under
`validation/reports/<validation-run-identity>.json`; the exact replay is a
no-op, while different report bytes under the same identity fail as a conflict.

## Terraform and operational controls

`enable_emr_validation` defaults to `false`. Enabling it requires the approved
AWS account ID and a budget-alert recipient. Terraform pins EMR release
`emr-7.12.0`, which supplies Spark 3.5.6 and Iceberg 1.10.0, and creates no job
run. The application has no pre-initialized capacity, auto-stops after five
idle minutes by default, and is capped at 8 vCPU and 32 GB. The reviewed manual
job example uses one executor, one attempt, a 30-minute timeout, explicit
manifest keys, a bounded interval and a unique validation identity.

The execution role can read only the approved Bronze, Iceberg and AWS-08 code
prefixes plus the two reference tables in the Glue catalog. It can write only
`validation/reports/` and `validation/logs/`, through the one data KMS key. It
has no Glue mutation, delete, canonical Bronze or Iceberg write permission.

AWS documents EMR Serverless 7.12.0 as Spark 3.5.6 with Iceberg 1.10.0:
[release details](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/release-version-7120.html).
The manual invocation follows the documented Iceberg configuration and runtime
role boundaries: [Iceberg](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/using-iceberg.html),
[runtime roles](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/security-iam-runtime-role.html).

## Validation record

GitHub Actions run
[#42401528252](https://github.com/huangruidtu/multi-source-data-engineering-pipeline-lab/actions/runs/42401528252)
passed at commit `b6aafc2332b8dd66d06af274fb9ec4b86a89ea72`. Its Java 17
`real-spark-iceberg` job executed four existing AWS-02/AWS-03/AWS-05 real
Iceberg tests and then the new AWS-08 command. The command reported
`LOCAL_SPARK_ICEBERG_VERIFIED`, `canonical_writer_unchanged=true` and
`exact_replay_stable=true`.

Observed reconciliation was:

| Check | Initial | After newer rate |
| --- | ---: | ---: |
| Verified manifests | 2 | 2 |
| Exchange-rate input/current keys | 1 / 1 | 1 / 1 |
| Exchange-rate exact/superseded | 1 / 0 | 0 / 1 |
| Location input/current keys | 1 / 1 | 1 / 1 |
| Location exact/superseded | 1 / 0 | 1 / 0 |
| Unrelated current keys | 0 | 0 |

The initial Iceberg snapshot IDs were `8541346281469155116` for exchange rates
and `5735442714004848603` for locations. Publishing the newer 7.60 rate through
AWS-03 produced exchange-rate snapshot `2576763360621510994`; the unchanged
location snapshot remained the same. The original historical manifest then
reconciled as one strictly superseded rate, with no validator write.

The same job observed all six expected failure tokens:

- `historical_business_key_missing_from_current_silver`;
- `duplicate_current_business_key`;
- `invalid_current_reference_value_or_provenance`;
- `duplicate_historical_business_key`;
- `invalid_historical_reference_value`;
- `manifest_unreadable_or_malformed`.

The complete workflow also passed:

- AWS-native offline Python tests: 79 passed, 1 Windows Airflow skip;
- Terraform format, initialization, validation and 18 Terraform tests;
- original MDEP V1 regressions: 62 passed;
- real Spark/Iceberg regressions: 4 passed, followed by AWS-08 validation;
- Gold contracts, Redshift parse and stateful dbt/DuckDB integration;
- the AWS-07 real integrated local demo;
- `git diff --check`.

## Remaining limitations

Local HadoopCatalog proves the validator’s Spark/Iceberg semantics, not Glue
Catalog, S3FileIO, KMS, IAM or EMR Serverless runtime behavior. An approved AWS
smoke test would still need a reviewed plan, sandbox account/region, protected
environment approval, budget recipient, exact manifest keys and interval,
bounded runtime, evidence retention and explicit operator submission. This PR
does not deploy, apply Terraform or start an EMR job.
