# MDEP Code Deep-Dive & Interview Walkthroughs

This directory is a post-V1 learning layer. Each walkthrough starts from the
merged `main` source code and follows its decisions through tests, architecture,
failure behavior, and a spoken interview answer. It does not change the V1
architecture or upgrade any runtime-deferred claim into runtime validation.

## How to use this material

Read one walkthrough beside the referenced source and tests. First understand
the file's inputs, outputs, state, and invariants; then read its critical-block
reasoning. Finally, answer the included questions aloud before using the
30-second explanation as a compact interview rehearsal.

The authoritative V1 completion boundary remains
[`docs/finalization/`](../finalization/README.md). `docs/study/` is broader
supplementary learning material; this directory is a code-first bridge between
the repository and interview discussion.

## Status labels

| Label | Meaning |
|---|---|
| **MDEP IMPLEMENTED** | The behavior is present in current `main` source. |
| **MDEP OFFLINE TESTED** | The behavior has repository tests/static validation, without physical infrastructure execution. |
| **MDEP RUNTIME DEFERRED** | The implementation exists but Kafka, Flink, Spark, S3, Iceberg, Airflow, Snowflake, or similar physical execution was not V1 acceptance evidence. |
| **GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED** | Useful production context that must not be confused with MDEP source behavior. |

## Foundation batch

Recommended order:

1. [`bronze-batch-publication.md`](bronze-batch-publication.md) — deterministic batch identity, evidence, conditional publication, and retry behavior.
2. [`silver-batch.md`](silver-batch.md) — validation, within-batch winners, and safe Iceberg merge ordering.
3. [`cdc-model.md`](cdc-model.md) — Debezium normalization, WAL/transaction ordering, and current-state transitions.
4. [`flink-cdc-job.md`](flink-cdc-job.md) — the physical streaming topology that applies the CDC model.

This order builds from batch landing and batch Silver correctness to the CDC
semantic model and then its Flink topology. Each document contains direct links
to the source, tests, finalization architecture material, and interview topic
that should be read beside it.

## Scope boundary

Future batches may explain additional existing implementation files, but they
are learning documentation only. They do not expand MDEP V1 scope, add a
technology, modify application behavior, or replace a future V1.x runtime lab.
