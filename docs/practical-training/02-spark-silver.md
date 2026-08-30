# 02 — Spark Silver workbook

Attempt this file before [the matching solutions](solutions/02-spark-silver-solutions.md). Record work in a copy of [the session template](training-records/02-spark-silver-session-template.md).

## SS-01 — Stale replay trace
- **Difficulty:** Intermediate
- **Task type:** TRACE / TEST DESIGN / INTERVIEW EXPLANATION
- **Source files to inspect:** `processing/spark/contracts.py` (`VERSION_FIELDS`, `version_order_key`, `incoming_is_newer`); `processing/spark/silver_batch.py` (`split_valid_and_quarantine`, `merge_iceberg`); `tests/test_silver_contracts.py`; `docs/code-deep-dive/silver-batch.md`.
- **Scenario:** Existing `locations` Silver state is newer than an incoming Bronze replay whose payload/hash differs.
- **Concrete rows:** natural key `location_id=loc-100`; existing `{updated_at:2026-08-20T00:00:00Z, source_extract_ts:2026-08-20T01:00:00Z, ingested_at:2026-08-20T01:05:00Z, record_hash:aaaa, location_name:"Copenhagen Hub"}`; incoming `{updated_at:2026-07-01T00:00:00Z, source_extract_ts:2026-08-21T01:00:00Z, ingested_at:2026-08-21T01:05:00Z, record_hash:zzzz, location_name:"Old Hub"}`.
- **Engineering deliverables:** natural key; both tuples; lexicographic result; `incoming_is_newer` result; MERGE action; final row; why hash differs but cannot update; invariant; regression test; 30–60 second English explanation.
- **Constraints:** locations order is `updated_at → source_extract_ts → ingested_at → record_hash`; no Spark/Iceberg runtime claims.
- **Competency trained:** stale replay prevention and deterministic current state.
- **Workspace:** `Key: ___ | Existing tuple: ___ | Incoming tuple: ___ | Decision: ___ | MERGE: ___ | Invariant: ___ | Test: ___`

## SS-02 — Hash-driven MERGE bug review
- **Difficulty:** Senior
- **Task type:** CODE REVIEW / IMPLEMENTATION / TEST DESIGN
- **Source files to inspect:** `processing/spark/contracts.py`; `processing/spark/silver_batch.py` (`merge_iceberg`); `tests/test_silver_contracts.py`; `docs/code-deep-dive/silver-batch.md`.
- **Scenario:** Reviewer proposes `WHEN MATCHED AND s.record_hash <> t.record_hash THEN UPDATE`.
- **Concrete rows:** existing location `{updated_at:2026-08-20, source_extract_ts:2026-08-20T01:00Z, ingested_at:2026-08-20T01:05Z, record_hash:"aaaa"}`; replay `{updated_at:2026-07-01, source_extract_ts:2026-08-22T01:00Z, ingested_at:2026-08-22T01:05Z, record_hash:"bbbb"}`.
- **Engineering deliverables:** why inequality proves payload difference not freshness; regression path; correct ordering predicate in words/pseudocode; hash position for locations and exchange rates; protecting test; production consequence; interview explanation.
- **Constraints:** This is an implementation correctness bug fixed in code, not runtime production evidence. Hash is final deterministic tie-breaker only.
- **Competency trained:** safe MERGE design and code review.
- **Workspace:** `Finding: ___ | Regression: ___ | Correct predicate: ___ | Tests: ___ | Interview: ___`

## SS-03 — Tie-breaker and replay test design
- **Difficulty:** Intermediate
- **Task type:** TEST DESIGN / TRACE
- **Source files to inspect:** `processing/spark/contracts.py` (`incoming_is_newer`); `tests/test_silver_contracts.py`; `docs/code-deep-dive/silver-batch.md`.
- **Scenario:** You need two pure contract tests before relying on physical Iceberg MERGE.
- **Concrete inputs:** A existing/incoming same location `updated_at=2026-08-20T00:00Z`, same `source_extract_ts=2026-08-20T01:00Z`; existing `ingested_at=01:05Z`, hash `aaaa`; incoming `ingested_at=02:00Z`, hash `bbbb`. B exact replay: every tuple field identical. **Senior variation:** same first three fields but incoming hash `zzzz`, existing hash `aaaa`.
- **Engineering deliverables:** for A/B setup, tuples, expected `incoming_is_newer`, invariant and prevented bug; for variation, deterministic result and why hash is not business freshness; name test locations.
- **Constraints:** Use actual pure helper behavior; do not claim physical Spark/Iceberg execution.
- **Competency trained:** evidence-driven tie-breaker/replay test design.
- **Workspace:** `Test A: ___ | Test B: ___ | Variation: ___ | Invariant: ___ | Bug prevented: ___`
