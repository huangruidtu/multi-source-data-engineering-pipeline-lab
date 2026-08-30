# 05 — Flink streaming workbook

Attempt this file before [the matching solutions](solutions/05-flink-streaming-solutions.md). Record work in a copy of [the session template](training-records/05-flink-streaming-session-template.md).

## FS-01 — Malformed event trace
- **Difficulty:** Intermediate
- **Task type:** TRACE / INCIDENT / INTERVIEW EXPLANATION
- **Source files to inspect:** `processing/flink/flink_cdc_job.py` (`KafkaSource`, `_raw_message`, `_bronze_values`, `DebeziumParser`, side outputs); `processing/flink/cdc_model.py`; `tests/test_flink_topology.py`; `docs/code-deep-dive/flink-cdc-job.md`.
- **Scenario:** Kafka emits a raw value on `mdep.commerce.orders` that is not valid JSON: `{"op":"u", bad-json`.
- **Concrete inputs/runtime facts:** Source starts at earliest and uses value-only `SimpleStringSchema`; raw branch is created before parser; parser catches JSON/contract errors and outputs the quarantine tag; the state branch follows only parsed events through watermark then `key_by`.
- **Engineering deliverables:** trace first receiver; raw Bronze disposition; whether `CdcEvent` exists; parser/side-output behavior; whether keyBy/ValueState/Silver receive mutation; why parser failure differs from stale/replay; one static/regression test; 30–60 second English explanation.
- **Constraints:** Do not claim filesystem, Kafka, or Iceberg write ran. Quarantine is evidence, not silent discard.
- **Competency trained:** Evidence-first streaming and parsing/state boundaries.
- **Workspace:** `First stage: ___ | Bronze: ___ | CdcEvent: ___ | Quarantine: ___ | State/Silver: ___ | Test: ___ | Interview: ___`

## FS-02 — Keyed-state collision code review
- **Difficulty:** Senior
- **Task type:** CODE REVIEW / TEST DESIGN
- **Source files to inspect:** `processing/flink/flink_cdc_job.py` (`key_by`, `CdcStateApplier`); `processing/flink/cdc_model.py` (`key_identity`); relevant Flink model/topology tests and Deep-Dives.
- **Scenario:** Reviewer changes state keying to `key_by(lambda e: e.primary_key)`.
- **Concrete inputs:** customer event `customers:1001`, source LSN `0/400`, `after={customer_id:"1001", customer_status:"active"}`; order event `orders:1001`, source LSN `0/500`, `after={order_id:"1001", order_status:"shipped"}`.
- **Engineering deliverables:** explain collision; which `last_version`/`current_row` state could corrupt; explain why primary key alone is unsafe; relate `entity:primary_key` to `key_identity`; corrected strategy; regression test design; production consequence; 30–60 second English review answer.
- **Constraints:** Do not claim composite Kafka keys; preserve MDEP entity ownership/current `key_identity` semantics.
- **Competency trained:** Partitioned state isolation and review of streaming keys.
- **Workspace:** `Collision: ___ | Corruption: ___ | Correct key: ___ | Test: ___ | Consequence: ___ | Review answer: ___`

## FS-03 — Exactly-once claim review
- **Difficulty:** Senior
- **Task type:** ARCHITECTURE REVIEW / INCIDENT DESIGN / INTERVIEW EXPLANATION
- **Source files to inspect:** `processing/flink/flink_cdc_job.py` (checkpoint constants, `enable_checkpointing`, `CheckpointingMode.EXACTLY_ONCE`, checkpoint config, restart strategy); `tests/test_flink_topology.py`; `processing/flink/cdc_model.py`; `docs/code-deep-dive/flink-cdc-job.md`.
- **Scenario:** Project slide states: “EXACTLY_ONCE configuration proves end-to-end exactly once.” You must approve, reject, or qualify it.
- **Concrete facts:** checkpoint interval is 30,000 ms; mode requested is `EXACTLY_ONCE`; timeout is 120,000 ms; minimum pause is 5,000 ms; topology contains Kafka source, ValueState, filesystem archive/quarantine and Iceberg sinks. Tests are static/unit tests, not job submission evidence.
- **Engineering deliverables:** separate **MDEP IMPLEMENTED**, **MDEP OFFLINE TESTED**, **MDEP RUNTIME DEFERRED**; say what config proves/does not prove; design runtime experiment and one failure scenario; give approved interview wording; explain why “configured for exactly once” is safer.
- **Constraints:** Do not claim observed checkpoint completion, restore, offset recovery, Iceberg atomic commit, or end-to-end duplicate/loss proof.
- **Competency trained:** Delivery-semantics claims and evidence-driven engineering judgment.
- **Workspace:** `Implemented: ___ | Offline tested: ___ | Deferred: ___ | Experiment: ___ | Failure test: ___ | Interview wording: ___`
