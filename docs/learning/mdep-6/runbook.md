# MDEP-6 Runbook — Contract Review and Recovery Guidance

## Purpose

MDEP-6 has no runnable service. This runbook makes its documentation review reproducible and explains how later implementers should use the contract when something is ambiguous.

## Setup and inspection

From the repository root:

```powershell
Get-Content -Raw source-data/contracts/commerce-operations.md
Get-Content -Raw docs/decisions/0001-iceberg-catalog-and-snowflake-access.md
Get-Content -Raw docs/planning/jira-dependencies.md
```

Expected result: a source-to-layer matrix with exactly one canonical Silver writer for every listed logical dataset and an ADR that names S3-backed HadoopCatalog.

## Validation walkthrough

Use this checklist before implementing a dependent Story.

1. Pick `orders`; confirm PostgreSQL is authoritative, batch writes only Bronze snapshots, Flink CDC is the canonical Silver writer, and dbt later consumes it as a fact.
2. Pick `exchange_rates`; confirm Airflow is the Bronze writer and Spark is the canonical Silver writer.
3. Pick `payment.failed`; confirm `event_id` is immutable, `aggregate_id` determines keyed ordering, and it lands in `evt_payments`, not `core_payments`.
4. Pick a duplicate file row; confirm it may remain in Bronze and is deduplicated or rejected by future Silver processing.

## Failure reproduction and recovery procedure

| Symptom | Diagnose | Recovery / verification |
| --- | --- | --- |
| Two components propose to write `core_orders` | Check the ownership matrix | Keep CDC/Flink as canonical and restrict the batch path to Bronze; re-review the matrix |
| A source payload lacks a stable key | Check whether `record_hash` and source version are sufficient | Record an explicit contract decision before implementing an upsert |
| A breaking field change is proposed | Compare field meaning/type to the contract | Stop the dependent change, version/update the contract, plan migration and replay handling |
| A rejected record would be dropped | Inspect Quarantine contract fields | Retain raw payload/reference, location, time, and rejection reason before proceeding |
| Global Kafka ordering is assumed | Inspect the event envelope's ordering rule | Use per-`aggregate_id` reasoning and document any cross-key consistency requirement |

## Useful review searches

```powershell
rg -n "Canonical Silver writer|Duplicate rule|Open implementation decisions" source-data/contracts
rg -n "Decision|Alternatives|Validation boundary" docs/decisions
```

There is no start, stop, reset, test, or data recovery command because the Story intentionally introduced no runtime component.
