# 08 — End-to-end incidents
Read: `docs/finalization/end-to-end-data-flow.md`, `docs/code-deep-dive/master-map.md`, linked source/tests.

## E2E-01 — INCIDENT
**Difficulty:** Senior. **Scenario:** REST page 3 fails after pages 1–2; someone proposes publishing pages 1–2 and fixing later. **Deliverable:** identify owner, reject/accept proposal, trace retry/Bronze/reconciliation impact. **Competency:** cross-layer partial-landing reasoning.
## E2E-02 — TRACE
**Difficulty:** Senior. **Scenario:** CDC update LSN 500 is current; delete LSN 420 arrives; later dbt run executes. **Deliverable:** trace all affected layers and final Silver/Gold state; identify runtime-deferred steps. **Competency:** delete semantics end-to-end.
## E2E-03 — INCIDENT
**Difficulty:** Senior. **Scenario:** Gold and Silver counts match, but anti-join finds 10 different order IDs. **Deliverable:** explain why counts are insufficient, assign investigation layers, and propose evidence to save. **Competency:** business-key reconciliation.
