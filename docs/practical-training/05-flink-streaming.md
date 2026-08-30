# 05 — Flink streaming topology
Read: `processing/flink/flink_cdc_job.py`, `tests/test_flink_topology.py`, `docs/code-deep-dive/flink-cdc-job.md`.

## FS-01 — TRACE
**Difficulty:** Intermediate. **Scenario:** malformed CDC JSON arrives. **Deliverable:** trace raw Bronze, parser, quarantine, and current-state effects. **Competency:** evidence-first streaming.
## FS-02 — CODE REVIEW
**Difficulty:** Senior. **Snippet:** `key_by(lambda e: e.primary_key)`. **Deliverable:** show collision example, corrected key, and state consequence. **Competency:** keyed-state isolation.
## FS-03 — ARCHITECTURE
**Difficulty:** Senior. **Claim:** “EXACTLY_ONCE configuration proves end-to-end exactly once.” **Deliverable:** review claim; separate implemented configuration, offline-tested semantics, and runtime-deferred proof. **Competency:** bounded claims.
