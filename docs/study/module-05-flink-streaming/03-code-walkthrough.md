# Module 05 — Code walkthrough

Reading order: `processing/flink/cdc_model.py` → `tests/test_flink_cdc_model.py` → `processing/flink/flink_cdc_job.py` → `tests/test_flink_topology.py` → `scripts/validate-mdep-11-flink-cdc.ps1`.

`parse_debezium` validates entity/envelope/LSN and produces `CdcEvent`. `key_identity` prevents cross-entity key collision. `version_decision` is the pure test oracle. The topology creates a `KafkaSource` per topic, writes Bronze before parsing, emits malformed input to a side-output quarantine, assigns watermarks, keys valid events, and uses `CdcStateApplier`. It handles `d` by emitting a delete and clearing current state; tombstones are archive/compaction markers and do not mutate Silver. Earlier topology/config-only work was replaced by this actual wiring, but physical job submission remains unvalidated.
