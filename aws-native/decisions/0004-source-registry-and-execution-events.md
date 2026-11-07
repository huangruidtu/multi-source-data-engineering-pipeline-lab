# ADR-0004: A strict registry configures existing execution engines

- Status: accepted for MDEP-72
- Date: 2026-11-07

## Decision

Source onboarding is a validated JSON registry entry, not copied pipeline code.
The registry may select only existing REST, CSV and JSON extractor/entity
combinations from the MDEP contract. It contains operational identifiers,
bounds, ownership and a local path or endpoint environment-variable name; it
cannot contain credentials or literal REST endpoints.

The runner dispatches to the existing contract validator, Parquet serializer,
Bronze publisher and quarantine publisher. `BatchContext` remains the only
logical-run identity implementation. To prevent two configured instances from
claiming the same deterministic identity, only one enabled source may own a
given `(source_type, source_entity)` pair.

Execution boundaries emit `mdep-execution-event-v1` JSON with bounded safe
fields. Raw records, customer values, credentials and endpoint URLs are absent.
CloudWatch filters match only event version plus low-cardinality status and add
no metric dimensions.

## Consequences

- Adding an approved instance is a config review plus tests; shared extraction
  and publication code does not change.
- Devices demonstrate Bronze onboarding only. AWS-03 still does not own a
  canonical `ref_devices` table.
- Retry, backfill, orphan recovery and conflict behavior remain the AWS-02
  manifest protocol. CDC recovery remains ADR-0003; AWS-06 adds no checkpoint.
- Platform metrics are opt-in and require the existing Lambda log group and a
  budget recipient. No new deployment role or OIDC permission is introduced.
