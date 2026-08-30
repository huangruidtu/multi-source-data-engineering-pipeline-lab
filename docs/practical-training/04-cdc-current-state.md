# 04 — CDC current-state model
Read: `processing/flink/cdc_model.py`, `tests/test_flink_cdc_model.py`, `docs/code-deep-dive/cdc-model.md`.

## CS-01 — TRACE
**Difficulty:** Intermediate. **Inputs:** accepted `orders:1001` update LSN `0/200`; candidate delete LSN `0/180`. **Deliverable:** decision, state mutation, invariant, regression test. **Competency:** stale-delete safety.
## CS-02 — INCIDENT
**Difficulty:** Senior. **Inputs:** equal LSN; null transaction IDs/orders; null partition/offset on both; payloads differ. **Deliverable:** decision, why not replay, risk of guessing newer, two production improvements. **Competency:** conservative conflict handling.
## CS-03 — CODE REVIEW
**Difficulty:** Senior. **Snippet:** `return NEWER if event.kafka_offset > last.kafka_offset else LOWER_LSN`. **Deliverable:** reject it, explain partition-scoped offset, and state actual precedence. **Competency:** source versus transport ordering.
