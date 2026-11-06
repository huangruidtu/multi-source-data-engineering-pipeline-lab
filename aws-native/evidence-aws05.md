# MDEP-71 / AWS-05 evidence

## Evidence boundary

| Claim | Status | Evidence |
| --- | --- | --- |
| DMS payload and ordering oracle | OFFLINE TESTED | `tests/test_aws05_cdc.py` uses real `data`/`metadata`, load/insert/update/delete and PostgreSQL LSNs |
| Persistent replay/delete correctness | OFFLINE TESTED | SQLite restart, duplicate, stale update/delete, equal conflict, incomparable LSN and tombstone tests |
| Canonical Iceberg writer | LOCAL LINUX CI TESTED | pinned Java 17, Spark 3.5.6, Iceberg 1.10.0 test performs real MERGE and inspects snapshots |
| DMS/Kinesis/Firehose infrastructure | CODED + TERRAFORM OFFLINE TESTED | disabled-by-default mock plans and policy assertions |
| AWS runtime | RUNTIME DEFERRED | no account resources were planned against live state, created or started |

## Correctness trace

1. The adapter rejects missing `stream-position`, transaction ID or transaction
   record ID for CDC operations.
2. The oracle compares snapshot phase, PostgreSQL LSN, then same-transaction
   record order. It returns `INCOMPARABLE` rather than inventing an order.
3. Exact position plus exact event content returns `DUPLICATE`; equal position
   plus different content returns `CONFLICT` and stable quarantine evidence.
4. A delete writes `is_deleted=true` with its coordinate. Gold filters the
   tombstone; later stale updates cannot recreate a live row.
5. Firehose uses a metadata extraction query on DMS `metadata.table-name` to
   write `bronze/cdc/<entity>/...`; failures have a separate error prefix.

## Demonstration

```text
uv run --project aws-native --locked python aws-native/scripts/run_cdc_demo.py
```

Expected decisions are insert, update, duplicate, delete, stale; final state is
a tombstone at `16/B6C70` with one quarantined stale event.

## Deliberate limitations

- AWS DMS, Kinesis and Firehose are not deployed. DMS endpoint connectivity,
  logical slot behavior and service metrics remain runtime-deferred.
- The account must already have the reviewed DMS VPC and CloudWatch service
  roles. Terraform does not create account-global broad managed-policy roles.
- Firehose archives source records only. A separately deployed consumer must
  invoke the canonical writer; AWS-05 does not silently turn Firehose into a
  second current-state owner.
- A new additive business field is retained in raw JSON and `payload_json`, but
  exposing it as a typed Gold column still requires a reviewed schema migration.
