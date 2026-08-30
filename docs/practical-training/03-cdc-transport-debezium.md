# 03 — CDC transport and Debezium
Read: `ingestion/cdc/contracts.py`, `debezium-postgres-connector.json`, `tests/test_cdc_contracts.py`.

## CT-01 — CONFIG REVIEW
**Difficulty:** Intermediate. **Scenario:** connector uses `publication.autocreate.mode=disabled` and `slot.drop.on.stop=false`. **Deliverable:** name prerequisite, recovery benefit, and WAL/disk risk. **Competency:** source-side ownership/trade-off.
## CT-02 — TRACE
**Difficulty:** Foundation. **Inputs:** topic `mdep.commerce.orders`, key `{order_id: "1001"}`, envelope `{op:"d", after:null}`. **Deliverable:** classify operation, key contract, and distinguish resulting tombstone if enabled. **Competency:** business versus transport semantics.
## CT-03 — INCIDENT
**Difficulty:** Senior. **Scenario:** connector stopped for days and PostgreSQL disk rises. **Deliverable:** explain likely slot/WAL mechanism, next checks, and what is MDEP runtime deferred. **Competency:** operational reasoning.
