# Module 00 — Interview Q&A

### Why are there batch and CDC paths?
**Direct answer:** They solve different source and latency semantics. **Deep explanation:** Batch preserves bounded snapshots/files/API pages; CDC preserves mutations from WAL. **MDEP example:** Spark owns exchange rates/locations while Flink owns PostgreSQL current state. **Why this design:** avoids a dual Silver writer. **Follow-up:** How do you reconcile them? Compare compatible business keys/grains. **Senior extension:** enforce ownership in deployment/catalog policy. **Weak answer:** “CDC is always better.”

### Why Bronze, Silver, Gold?
**Direct answer:** evidence, trusted state, and consumer semantics are different jobs. **Deep explanation:** history can contain duplicates; current state must converge; Gold has an analytical grain. **MDEP example:** CDC Bronze retains tombstone evidence while `fct_orders` is current state. **Follow-up:** Why not equal row counts? History and state differ. **Senior extension:** define reconciliation exceptions. **Weak answer:** treating Bronze as clean.

### What is implemented versus proven?
**Direct answer:** code/configuration and static contracts are present; physical integrations are blocked. **MDEP example:** MDEP-13 says `BLOCKED`, not passed, without evidence. **Follow-up:** What closes it? a run-specific evidence bundle. **Senior extension:** evidence freshness and release gates. **Weak answer:** claiming a Compose file proves a run.
