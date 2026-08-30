# 04 — CDC current-state workbook

Attempt this file before reading [the matching solutions](solutions/04-cdc-current-state-solutions.md). Record work separately in [the session template](training-records/04-cdc-current-state-session-template.md).

## CS-01 — Stale delete must not regress state

- **Difficulty:** Intermediate
- **Task type:** TRACE / TEST DESIGN / INTERVIEW EXPLANATION
- **Source files to inspect:** `processing/flink/cdc_model.py` (`version_decision`, `apply_current_state`); `tests/test_flink_cdc_model.py` (`test_higher_lsn_accepts_and_lower_lsn_rejects`, `test_newer_delete_by_transaction_order_and_stale_delete`); `docs/code-deep-dive/cdc-model.md`.
- **Scenario:** `orders:1001` has a newer accepted update. A delayed delete is delivered later, with a larger Kafka offset but an older PostgreSQL source position.
- **Concrete event data:**

```text
last accepted: entity=orders, key=1001, op=u, source_lsn=0/200,
transaction_id=tx-500, total_order=8, topic=mdep.commerce.orders,
partition=1, offset=400, after={order_id: "1001", order_status: "shipped"}

candidate: entity=orders, key=1001, op=d, source_lsn=0/180,
transaction_id=tx-420, total_order=3, topic=mdep.commerce.orders,
partition=1, offset=999, before={order_id: "1001", order_status: "created"}, after=null
```

- **Engineering deliverables:** decision; exact `apply_current_state` result; state/version mutation result; protected invariant; regression-test setup/assertions; 30-second English answer.
- **Constraints:** Do not privilege `op=d`; do not use offset as freshness; do not claim Kafka/Flink runtime execution.
- **Competency trained:** Source-version ordering, stale-delete prevention, executable regression reasoning.
- **Workspace:** `Decision: ___ | Return value: ___ | State: ___ | Version: ___ | Invariant: ___ | Test: ___ | Interview answer: ___`

## CS-02 — Equal-position conflict is not a replay

- **Difficulty:** Senior
- **Task type:** INCIDENT / ARCHITECTURE DECISION
- **Source files to inspect:** `processing/flink/cdc_model.py` (`_same_known_transport`, `version_decision`); `tests/test_flink_cdc_model.py` (`test_unknown_transport_is_not_an_exact_replay`, `test_same_lsn_without_transaction_or_transport_is_conservative_conflict`); `docs/code-deep-dive/cdc-model.md`.
- **Scenario:** The current PyFlink source is value-only. Two different order payloads have the same source LSN, but neither transaction nor transport metadata can establish ordering or replay identity.
- **Concrete event data:**

```text
last:      source_lsn=0/300, transaction_id=null, total_order=null,
           topic=mdep.commerce.orders, partition=null, offset=null,
           after={order_id: "1001", order_status: "shipped"}
candidate: source_lsn=0/300, transaction_id=null, total_order=null,
           topic=mdep.commerce.orders, partition=null, offset=null,
           after={order_id: "1001", order_status: "cancelled"}
```

- **Engineering deliverables:** decision/state behavior; why not `EXACT_REPLAY`; why `None == None` is not identity; guessing risk; two improvements labeled **GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED**; compact on-call update.
- **Constraints:** Do not equate same LSN with replay; do not claim current source preserves real Kafka metadata; do not invent conflict-resolution code.
- **Competency trained:** Conservative state safety under incomplete evidence.
- **Workspace:** `Decision: ___ | Evidence missing: ___ | Guessing risk: ___ | Improvement 1/2: ___ | On-call update: ___`

## CS-03 — Review a transport-ordering bug

- **Difficulty:** Senior
- **Task type:** CODE REVIEW / IMPLEMENTATION / TEST DESIGN
- **Source files to inspect:** `processing/flink/cdc_model.py` (`version_decision`); `tests/test_flink_cdc_model.py` (`test_partition_number_alone_is_not_freshness`, replay/conflict tests); `docs/code-deep-dive/cdc-model.md`.
- **Scenario:** A reviewer proposes this replacement:

```python
def version_decision(event, last):
    if last is None:
        return VersionDecision.NEWER
    if event.kafka_offset > last.kafka_offset:
        return VersionDecision.NEWER
    return VersionDecision.LOWER_LSN
```

- **Concrete inputs:** A: candidate LSN `0/180`, partition 1/offset 999; last LSN `0/200`, partition 1/offset 400. B: same LSN, candidate partition 9/offset 1; last partition 1/offset 999.
- **Engineering deliverables:** review findings; partition-local/source-versus-transport explanation; current-state bug for A/B; corrected precedence/pseudocode; two regression tests; 60-second English review answer.
- **Constraints:** Preserve existing MDEP semantics; partition number is not freshness; physical Kafka metadata is runtime deferred in current source.
- **Competency trained:** Code review, ordering-contract preservation, targeted test design.
- **Workspace:** `Finding: ___ | Bug A/B: ___ | Correct ordering: ___ | Tests: ___ | Review answer: ___`
