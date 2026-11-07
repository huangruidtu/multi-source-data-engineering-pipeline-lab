# AWS-05 interview explanation

## 30–60 seconds

“AWS DMS reads PostgreSQL logical replication and emits its native
JSON_UNFORMATTED `data` and `metadata` records to one encrypted Kinesis shard.
Firehose independently archives newline-delimited raw records to entity
prefixes in S3; it is replay storage, not the state writer. The Python adapter
requires DMS transaction details and parses PostgreSQL LSNs. Per key, freshness
is LSN first and transaction record order second, never event time or Kinesis
arrival. Exact replay is a no-op, stale events are quarantined, and the same
position with different content fails closed. The canonical Spark writer MERGEs
business state and its checkpoint into one Iceberg v2 row. Deletes persist as
tombstones, so an old update cannot resurrect data. Terraform is opt-in, private,
KMS-encrypted, one-shard and small-instance bounded; this repository validates
it offline and in real local Spark/Iceberg CI, but does not claim an AWS run.”

## Code anchors

- `mdep_cdc/dms_adapter.py`: exact AWS DMS field names and operation mapping.
- `mdep_cdc/ordering.py`: the one source-position oracle.
- `mdep_cdc/state_store.py`: atomic durable reference state and quarantine.
- `mdep_cdc/iceberg_writer.py`: actual Iceberg v2 MERGE and tombstones.
- `terraform/cdc.tf`: DMS/Kinesis/Firehose settings, IAM, encryption and bounds.
- `spark/tests/test_real_iceberg_pipeline.py`: real merge, replay, conflict,
  stale-delete, tombstone, quarantine and snapshot evidence.

## Failure discussion

PostgreSQL primary keys are mandatory for update/delete capture. Monitor source
WAL retention and replication-slot lag because an unavailable DMS task can fill
the database volume. Monitor DMS task state and latency, Kinesis write/read
throttling and iterator age, Firehose delivery errors, S3 errors, and canonical
writer quarantine growth. A missing or incomparable source coordinate pauses
that key/event path; operators inspect raw Bronze plus quarantine and do not
guess freshness.
