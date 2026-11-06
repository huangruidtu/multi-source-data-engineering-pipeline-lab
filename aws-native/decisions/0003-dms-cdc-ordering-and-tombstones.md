# ADR-0003: DMS source positions and durable tombstones own CDC freshness

- Status: accepted for MDEP-71
- Date: 2026-11-06

## Decision

AWS-05 consumes the AWS DMS Kinesis `data`/`metadata` envelope directly. It does
not translate the V1 Debezium `r/c/u/d` envelope. Full-load `load` records form a
snapshot phase that is older than every CDC record. CDC `insert`, `update`, and
`delete` records are ordered first by parsed PostgreSQL `stream-position`, then
by `transaction-record-id` only when the transaction ID is the same.

Event timestamp, Kinesis sequence number, shard, arrival time, and transaction
ID alone are provenance, not freshness. Missing coordinates and equal LSNs from
different transactions are incomparable. Equal coordinates with different
content are conflicts. Both dispositions fail closed and retain quarantine
evidence; they never overwrite current state.

Each `core_*` Iceberg v2 table is a current-state ledger. A delete replaces the
business payload with a durable `is_deleted=true` row while preserving the last
source coordinate. Checkpoint and tombstone therefore commit in the same
Iceberg MERGE. Gold staging filters tombstones. This prevents a delayed update
from resurrecting a deleted key and avoids a cross-table crash window.

## Consequences

- Exact replay at one position is a no-op even when transport metadata differs.
- A new source position can apply identical business content because it is a
  distinct authoritative source event.
- Additive DMS fields remain in `payload_json` and the raw event even before a
  reviewed physical-column migration.
- Raw Firehose objects are immutable replay material, not AWS-02 manifest
  batches and not a canonical current-state writer.
- The offline SQLite writer is the executable ordering oracle; the real local
  Spark/Iceberg test proves the same decisions against actual MERGE snapshots.

## Rejected alternatives

- Event timestamps are not a PostgreSQL total order.
- Kinesis sequence numbers order a transport shard, not the source log.
- Physically deleting rows discards the coordinate needed to reject stale
  resurrection.
- Treating every equal coordinate as duplicate would hide source conflicts.
