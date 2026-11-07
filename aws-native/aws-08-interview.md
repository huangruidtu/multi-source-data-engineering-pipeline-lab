# AWS-08 interview pack

## 30–60 second project summary

> AWS-08 adds one deliberately narrow EMR Serverless use case: validating an
> approved historical reference-data backfill without becoming another Silver
> writer. The job accepts exact Bronze manifest keys, a catalog and namespace,
> a bounded interval and a validation UUID. It verifies real Parquet and
> manifest-v2 evidence, then uses the same freshness rule as AWS-03 to prove
> each historical exchange rate and location is either still exact or was
> legitimately superseded by newer current state. Linux CI runs the full path
> with Spark 3.5.6, Iceberg 1.10.0 and real snapshots. Terraform keeps EMR
> disabled by default, capped at 8 vCPU and 32 GB, and gives it read-only lake
> and catalog access plus report/log-only writes. No AWS runtime is claimed.

## Two-minute explanation

> The correctness problem is subtle: a historical backfill should not fail just
> because the current table has a newer valid value, but it also must not accept
> missing, corrupt, duplicated or incomparable state. The validator therefore
> starts from explicit commit manifests rather than scanning a prefix. It uses
> the existing manifest verifier, reconstructs the exact AWS-03 transformation,
> checks business-key uniqueness and schema/provenance, and joins historical
> rows to current Iceberg state.
>
> An equal record hash is an exact match. Otherwise the current row must be
> strictly newer under the existing AWS-03 business-version-first ordering. A
> missing key or a current state that is neither equal nor newer fails closed.
> The report contains only counts, checksums and snapshot IDs; it contains no
> payload values. Exact replay yields byte-identical JSON.
>
> The EMR boundary is intentionally operational, not automatic. Terraform
> uploads the script and package and can create a pinned 7.12.0 application and
> least-privilege runtime role, but creates no job run. An operator must review
> exact manifests, interval, identity and timeout, then submit the documented
> batch request. That keeps the feature useful for an interview while being
> honest about cost and the absence of an AWS deployment.

## Five-minute explanation

> I started by preserving writer ownership. AWS-03 remains the only writer for
> `ref_exchange_rates` and `ref_locations`. AWS-08 has no MERGE statement and
> its IAM role cannot write `iceberg/` or mutate Glue. Its only data-plane write
> is a deterministic validation report, plus EMR logs, under dedicated
> prefixes.
>
> The request itself is fail closed. Catalog and namespace are safe identifiers;
> the validation identity is a canonical UUID; the interval must be positive
> and no longer than 31 days; the manifest set is unique and bounded at 20;
> verified payload bytes are capped. Both required entities must be present and
> every manifest interval must fit inside the approved interval.
>
> The input does not bypass the production contracts. Linux CI sends a
> deterministic REST response through the real AWS-02 extractor and ingestion
> pipeline, so the test creates real Parquet and manifest-v2 objects. AWS-03
> verifies those objects and performs actual Iceberg MERGE operations. AWS-08
> then independently re-verifies the same manifests and reads the committed
> snapshots.
>
> For each entity, the validator applies the same transformation and the exact
> business key. It rejects transformation failures and duplicate historical
> keys. It also requires the exact expected current table schema, unique current
> keys, positive rates or valid locations, and complete provenance. Each input
> key must be exact or strictly older than the current row according to AWS-03’s
> existing freshness expression. That preserves the priority of business
> version over extraction and ingestion provenance.
>
> The test first reconciles an exact state, then publishes a later EUR/DKK rate
> and validates the original interval again. The older 7.50 value is accepted
> as superseded by 7.60. Exact validation replay produces identical JSON. The
> suite separately proves missing state, duplicate state, invalid values,
> duplicate historical input and corrupt manifests all fail with bounded reason
> tokens before any canonical write.
>
> For cost, the application does not exist unless explicitly enabled with an
> account ID and budget recipient. There is no pre-initialized capacity. It
> auto-stops, caps at 8 vCPU and 32 GB, uses one executor, one attempt and a
> 30-minute default timeout. This is local Spark/Iceberg evidence plus an
> AWS-ready configuration—not an EMR runtime claim.

## Fifteen-minute walkthrough

1. Open `mdep_silver/backfill_validation.py` and show the bounded request,
   required entities, exact schemas and report contract.
2. Show that `verify_committed_batch` runs before any Spark read and that the
   manifest set is explicit rather than discovered by prefix.
3. Trace `transform` and `newer_than_sql` reuse from AWS-03. Explain why sharing
   the canonical freshness expression prevents validator/writer drift.
4. Show the historical uniqueness and value checks, then current schema,
   uniqueness and provenance checks.
5. Walk through the join states: `exact`, `superseded`, `missing`, `conflict`.
   Only the first two are valid.
6. Run
   `uv run --project aws-native/spark --locked python aws-native/integration/run_backfill_validation.py`.
   Inspect the final `AWS08_BACKFILL_VALIDATION_EVIDENCE=` JSON.
7. Point out real Bronze files, Iceberg snapshots, exact replay and the later
   7.60 rate superseding the historical 7.50 rate.
8. Review the six fail-closed scenarios and confirm the canonical writer’s
   snapshots do not change during validation.
9. Open `emr_validation.tf`: application opt-in, no pre-initialized capacity,
   fixed maximum capacity, short auto-stop and separate runtime role.
10. Read the IAM actions aloud: scoped S3 reads, two-table Glue reads, one KMS
    key through S3, and writes only to validation reports/logs.
11. Open `start-job-run.example.json` and show the explicit identity, interval,
    manifests, one attempt, timeout and single executor.
12. Close with the evidence boundary: Linux HadoopCatalog validates code and
    Iceberg semantics; Glue/S3/IAM/EMR remain deployment-deferred.

## Concise follow-up answers

**Why EMR Serverless?** It is a bounded, on-demand fit for an occasional
Spark/Iceberg validation without provisioning a persistent cluster.

**Why not let it repair Silver?** That would create a second canonical writer
and ambiguous replay/ownership. AWS-03 already owns reference Silver.

**Why explicit manifests?** A reviewed manifest list makes the approved input
finite and auditable; prefix discovery could silently widen a backfill.

**How can an old row still pass?** It passes only when current state is exactly
equal or strictly newer under AWS-03’s existing freshness rule.

**What stops a stale or conflicting current row?** If hashes differ and current
is not strictly newer, validation fails as
`current_silver_not_equal_or_newer_than_historical_input`.

**How is replay deterministic?** The report is sorted compact JSON derived from
fixed manifests, counts, checksums and snapshot IDs. The same validation
identity accepts only byte-identical report content.

**Does the report expose data?** No. It contains identifiers, counts,
source-content checksums and snapshot IDs, not record values or payloads.

**What makes IAM least privilege?** It cannot mutate Glue or write Bronze or
Iceberg. It reads only approved lake/code prefixes and two catalog tables, and
writes only validation reports and logs.

**What bounds cost?** Disabled-by-default creation, budget-recipient gating, no
pre-initialized workers, 8-vCPU/32-GB maximum capacity, one executor, short
auto-stop, one attempt and a bounded timeout.

**Why is Windows failure acceptable?** The failure was in the unsupported local
Windows Hadoop/Netty startup environment, before the application ran. The
required evidence is the pinned Java 17 Linux job used by CI and EMR-compatible
Spark versions.

**What does local CI not prove?** Glue Catalog, S3FileIO, KMS, IAM policy
evaluation, EMR startup and AWS cost require an approved AWS smoke run.

**How would you operate an AWS smoke test?** Review a no-refresh plan, approve
the sandbox account and budget, inspect exact manifests and interval, submit
one manual batch job, capture report/log evidence, and stop the application;
never infer `terraform apply` or job approval from this PR.
