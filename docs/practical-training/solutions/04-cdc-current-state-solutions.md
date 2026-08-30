# Solutions — 04 CDC current-state workbook

## CS-01
**Correct result:** `LOWER_LSN`; `apply_current_state` returns `lower_lsn_ignored`; neither state nor accepted version changes.

**Step-by-step / exact logic:** `0/180 < 0/200`, so `version_decision` returns `LOWER_LSN` before transaction or transport comparison. In `apply_current_state`, any decision other than `NEWER` returns before `state.pop` can execute for delete.

**Invariant:** A lower-LSN event must not mutate current state or accepted version for an entity-key, regardless of operation/offset.

**Common wrong answer:** Apply delete because offset 999 is higher, or because delete is special. Offset is not source freshness and delete has no bypass.

**Production consequence:** A late delete could erase newer Silver state; replacing version metadata would corrupt later comparisons.

**Suggested regression test:** accept update `0/200`, apply delete `0/180`, assert `lower_lsn_ignored`, `shipped` remains, and `versions[key]` remains the update.

**Interview answer:** “I apply deletes only after the same source-version comparison as updates. A lower PostgreSQL LSN is ignored completely, so it cannot remove the row or replace the accepted version.”

**Senior follow-up:** Proving checkpoint/restart preservation needs fault-injection runtime validation, which is **MDEP RUNTIME DEFERRED**.

## CS-02
**Correct result:** `EQUAL_POSITION_CONFLICT`; `apply_current_state` ignores it and leaves state/version unchanged.

**Step-by-step / exact logic:** equal LSN cannot resolve order; null transaction IDs do not enter same-transaction ordering; `_same_known_transport` requires non-null partition and offset on both events. `None == None` is absence of evidence, not identity.

**Invariant:** An unresolved equal source position cannot regress current state.

**Common wrong answer:** Call it replay because fields are equal, or choose the changed payload as newer. Differing payload makes guessing less defensible.

**Production consequence:** Guessing can corrupt current state and hide source-contract problems.

**Suggested regression test:** same-LSN/different-payload events with null metadata return conflict and leave the prior state/version intact.

**Interview answer:** “Without transaction order or known transport identity, same LSN is ambiguous. I do not call it replay and do not apply it; preserving known-good state is safer than guessing.”

**Senior follow-up:** **GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED:** retain stable event identity/real transport metadata and send conflict metrics plus quarantine/triage evidence.

## CS-03
**Correct result:** Reject the proposal.

**Step-by-step / exact logic:** Kafka offset is partition-local delivery position, not PostgreSQL chronology. A lets LSN `0/180` regress `0/200`; B shows partition/offset do not form global source order. Actual precedence: no last/higher LSN; lower LSN; same known transaction `total_order`; identical known topic/partition/offset replay; otherwise conflict.

**Invariant:** Transport topology never overrides source ordering.

**Common wrong answer:** Compare offsets across partitions or treat offsets as global time.

**Production consequence:** Cross-partition or replay delivery can arbitrarily regress state.

**Suggested regression tests:** lower LSN plus higher offset remains `LOWER_LSN`; same LSN with changed transport is conflict, matching partition non-freshness coverage.

**Interview answer:** “Kafka offsets are partition-local delivery positions, not PostgreSQL source versions. I use WAL LSN first, transaction order only for same known transactions, and transport coordinates only to prove exact replay.”

**Senior follow-up:** **GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED:** a metadata-preserving source improves replay identification but cannot replace source-order semantics.
