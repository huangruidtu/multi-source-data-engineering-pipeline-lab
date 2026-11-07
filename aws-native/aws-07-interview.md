# AWS-07 interview pack

## 30–60 second project summary

> I built an executable proof that joins the earlier AWS stories into one
> correctness path. A deterministic HTTP fixture goes through the real AWS-02
> REST extractor, contract checks, Parquet serializer and immutable Bronze
> manifest protocol. Spark 3.5.6 and Iceberg 1.10.0 then run the AWS-03 reference
> merges, while real DMS envelopes exercise the AWS-05 CDC ordering and
> tombstones. I export checksummed Silver snapshots into the actual AWS-04 dbt
> project on DuckDB, run 17 models and 35 tests, and reconcile four live orders
> to 835 DKK of known converted value. The demo also proves replay, conflict,
> corrupt-manifest, stale-event, missing-FX and explicit-delete behavior. It is
> honest local end-to-end evidence; no AWS service was deployed or claimed.

## Two-minute explanation

> The main design goal was not another mock pipeline. I wanted one command that
> executes the real correctness boundaries from source ingestion through Gold.
> The reference source starts at an offline HTTP client, but that client is fed
> into the production AWS-02 REST extractor and `IngestionPipeline`, so schema
> validation, business keys, source locators, truthful timestamps, PyArrow
> Parquet and manifest-last publication are all real code paths.
>
> AWS-03 refuses to read data unless the commit manifest and Parquet agree. It
> runs actual Spark and Iceberg MERGE operations for rates and locations. In
> parallel, the commerce data uses real AWS DMS-shaped records and the AWS-05
> writer, including duplicates, a same-position conflict, deletes and a stale
> update after a delete. Those tables are exported with checksums, counts,
> distinct business keys and one run identity.
>
> Gold verifies that bridge before loading it. The real dbt project then proves
> incremental replay, an FX correction from 7.50 to 7.60, missing-FX evidence,
> an orphan customer and an explicit CDC tombstone. Four live orders reconcile
> exactly: 75 DKK of DKK orders plus 100 EUR converted to 760 DKK, giving 835
> DKK known value. The classification is local end-to-end verified, because
> HadoopCatalog and DuckDB do not prove Glue, S3, Redshift or IAM runtime.

## Five-minute explanation

> I treat each layer as a contract and a recovery boundary.
>
> First, the demo creates three reference batches. The offline client returns a
> normal paged REST response, then the existing extractor validates every
> record and derives its business key and source locator. The existing pipeline
> enriches the rows and writes real Parquet. Data is written before the commit
> manifest, so consumers only see a batch after `_COMMITTED.json` exists. An
> identical retry becomes `already_published`; changed content under the same
> logical interval becomes a conflict. I also corrupt one manifest and prove it
> fails before Silver changes.
>
> Second, Spark 3.5.6 loads Iceberg 1.10.0 and executes the AWS-03 MERGE logic.
> The initial exchange-rate batch deliberately contains two identical records,
> so one deterministic winner lands and the duplicate is accounted for. A
> replay does not create a second business row. A later valid rate changes EUR
> to DKK from 7.50 to 7.60.
>
> Third, commerce current state follows the AWS-05 path. I construct genuine DMS
> JSON envelopes and parse them with the DMS adapter. The persistent Iceberg
> writer applies twelve inserts and two deletes, classifies two exact replays as
> duplicates, quarantines one same-position content conflict and rejects one
> stale update after a delete. Tombstones remain in Silver so an old event
> cannot resurrect a deleted order.
>
> Fourth, the bridge does not just point dbt at arbitrary files. It exports each
> Iceberg table and records every Parquet checksum, row count, distinct-key
> count and a common run identity. The Gold runner fails closed on missing or
> extra tables, unsafe paths, checksum mismatches, duplicate keys or mixed run
> identities. Only after verification does it expose the six Silver tables to
> DuckDB.
>
> Finally, the actual AWS-04 dbt models run incrementally. The first EUR order is
> 750 DKK. After the authoritative newer FX state it becomes 760 DKK even though
> the order itself did not change. Exact replay changes zero rows. Missing USD
> FX remains NULL, the missing customer remains observable, and absence from a
> partial feed never deletes data. A durable tombstone deletes one materialized
> order only when the operator sets the explicit verification gate.
>
> The output is both human-readable and machine-readable. It reports four
> source keys and four Gold keys, no duplicates, one missing-FX order, one
> orphan-customer order, and 835 DKK of known converted value. GitHub Actions
> runs this with Java 17 and separate locked Python environments. That is strong
> local evidence while keeping AWS deployment, permissions, networking and cost
> behind a separate approval boundary.

## Fifteen-minute walkthrough

Use this as a conversational sequence rather than a script to recite.

1. **Problem and promise.** Explain that separate unit-tested components can
   still disagree at their boundaries. AWS-07 supplies one executable path and
   one reconciliation report, while refusing to label local substitutes as AWS.
2. **Run the demo.** Execute
   `uv run --project aws-native/spark --locked python aws-native/integration/run_demo.py --work-dir /tmp/mdep-aws07-demo`.
   Point out the final `AWS07_END_TO_END_EVIDENCE=` JSON and the retained
   `aws07-evidence.json`.
3. **Source and Bronze.** Open `_ingest` in `integration/run_demo.py`. The only
   fake is network transport; `IngestionPipeline.rest` is production code.
   Discuss half-open logical intervals, actual ingestion time, source extraction
   time, deterministic identity and manifest-last visibility.
4. **Bronze failure behavior.** Show identical replay versus content conflict,
   then the deliberately corrupt manifest. Explain why data without a valid
   manifest is not committed input.
5. **Reference Silver.** Show `process_verified_batch`. Explain verification,
   deterministic deduplication, business-version-first freshness and Iceberg
   MERGE replay safety. Use the 7.50-to-7.60 rate correction as the example.
6. **CDC Silver.** Show the DMS adapter and `apply_iceberg_event`. Walk through
   ordered inserts, the exact duplicate, same-position conflict, delete replay
   and stale update after delete. Explain why a durable tombstone is safer than
   physical absence.
7. **Silver-to-Gold bridge.** Inspect either bridge manifest. Show table names,
   file SHA-256 values, row counts, distinct key counts and run identity. Explain
   the fail-closed checks before DuckDB views are created.
8. **Gold.** Show `fct_orders` and the verified tombstone macro. The FX change
   updates the converted amount without changing the order; missing FX remains
   NULL; partial feeds never infer deletes.
9. **Reconciliation.** Read the four/four key counts, zero duplicates, 75 DKK,
   100 EUR, 835 DKK converted, one missing FX and one orphan. Tie each number to
   a concrete row in `final_orders` or `daily_sales`.
10. **Evidence and limits.** Show the green integrated CI job, 17 dbt models and
    35 tests. Close by saying what remains unproved: S3/Glue/Redshift/IAM and
    real source networking require a separately approved AWS smoke test.

## Concise follow-up answers

**Why is the HTTP client still a fixture?** It removes external availability
and credentials from CI. The fixture stops at transport: the real REST
extractor, validation, enrichment, Parquet and publisher all execute.

**How do you prevent a partially written Bronze batch from reaching Silver?**
The manifest is written last and is the only commit boundary. Silver verifies
its identity, checksum, count and referenced Parquet before reading rows.

**What makes replay safe?** Bronze has deterministic logical keys and compares
source content; Iceberg MERGE uses deterministic business identity and
freshness; dbt increments only when authoritative source state is newer.

**Why not ignore all checksum mismatches?** A different physical checksum can
come from truthful ingestion provenance, but only identical source content is
an independent no-op replay. Changed business or source provenance is a real
conflict.

**How are CDC events ordered?** Comparable DMS source positions decide
freshness. Exact same-position content is a duplicate, different content at the
same position is quarantined, and a lower position is stale.

**Why keep tombstones?** They retain delete ordering so delayed events cannot
resurrect rows. Gold deletes only from verified tombstones, never because a
record is absent from a partial snapshot.

**How do you stop Silver/Gold mixing different runs?** Every exported table has
one bridge run identity. Gold verifies the expected table set, identity,
checksums, counts and distinct keys before creating source views.

**What happens when an FX rate is missing?** The source order is retained,
`order_total_dkk` stays NULL, and reconciliation increments the missing-rate
count. There is no silent zero or dropped order.

**What is the key reconciliation equation?** Final source keys equal final
Gold keys: four equals four, with zero duplicates. Known converted value is
760 EUR-derived DKK plus 50 and 25 native DKK, or 835 DKK.

**What does the demo not prove?** It does not prove AWS networking, IAM, KMS,
S3 conditional behavior, Glue Catalog/S3FileIO, Redshift performance or cost.
Those need an approved, bounded AWS smoke test.

**Why use separate Python environments?** Glue-compatible Spark runs on the
locked Python 3.11 environment, while the Gold/dbt project is locked to Python
3.12. Keeping them separate prevents an unrealistic combined runtime claim.

**What would you monitor in AWS?** Manifest/quarantine/conflict counts, DMS
latency and retained WAL, Kinesis iterator age and throttles, Glue failures and
duration, Iceberg snapshot growth, dbt freshness/test failures, Redshift usage
and budget alarms.
