# MDEP-72 / AWS-06 evidence

## Evidence boundary

| Capability | Status | Evidence |
| --- | --- | --- |
| Strict source registry | OFFLINE TESTED | JSON Schema plus semantic/path/endpoint/identity tests |
| Devices config-to-Bronze onboarding | LOCAL INTEGRATION VERIFIED | real file extraction, PyArrow Parquet, local immutable objects and committed manifest |
| Replay/backfill/recovery | LOCAL INTEGRATION VERIFIED | no-op replay, distinct backfill, interrupted-manifest recovery and verified reprocessing |
| Structured execution events | OFFLINE TESTED | actual extraction/publication/quarantine/reprocessing events parsed in tests |
| CloudWatch filters/alarms | CODED + TERRAFORM OFFLINE TESTED | mock plans match version/status fields and `notBreaching` missing-data policy |
| Cost/cleanup preview | OFFLINE TESTED | configuration inventory performs no deletion and does not claim billing |
| AWS runtime and alarms | AWS RUNTIME DEFERRED | no Terraform apply or AWS data-plane call |

## Observed local acceptance

`run_platform_demo.py` produced:

- registered source: `devices-json-reference`;
- first publication: `published`, 2 rows, verified physical checksum;
- exact/independent replay: `already_published`, same logical run UUID;
- simulated data-write/manifest interruption: `failed`, then `published` with
  `recovered_matching_orphan_data` and unchanged data bytes;
- selected historical interval: a second committed manifest;
- invalid devices input: `failed` plus deterministic quarantine key;
- changed content under the first identity: `conflict`;
- reprocessing: committed manifest verified before the callback ran.

## Recovery matrix

| Scenario | Executable policy |
| --- | --- |
| A normal run | registry extracts, validates, serializes, writes data, then commit manifest |
| B extraction fails | no Bronze manifest; original evidence goes to quarantine |
| C data succeeds/manifest fails | data remains an invisible orphan |
| D retry | same interval/source fields reproduce the same UUID and complete the orphan |
| E historical backfill | selected half-open interval produces its own deterministic UUID |
| F reprocess manifest | checksum/row count are verified before the selected processor runs |
| G duplicate replay | returns `already_published`; no second object is created |
| H conflicting replay | changed source-content checksum returns `conflict`; immutable data remains |

CDC recovery continues to use AWS-05 LSN ordering and durable tombstones. No
second CDC state/checkpoint was added.

## Limitations

- The registry is intentionally small: approved existing REST and reference-file
  entities only; PostgreSQL remains explicitly orchestrated.
- Devices are demonstrated through committed Bronze. There is no claim of an
  AWS-03 `ref_devices` Silver writer.
- CloudWatch configuration was planned with a mocked provider, not exercised in
  an AWS account. Metrics imply possible CloudWatch charges but no numeric bill
  is presented as fact.
