# Solutions — 04 CDC current-state
## CS-01
`LOWER_LSN`; neither version nor state changes. A bypassed delete would erase newer state. Regression test accepts LSN 200 update then asserts LSN 180 delete is `lower_lsn_ignored` and row remains.
## CS-02
`EQUAL_POSITION_CONFLICT`: null metadata cannot prove same replay. Guessing newer can regress/corrupt state. **GENERAL / NOT IMPLEMENTED:** preserve real Kafka metadata/stable event ID; emit conflict metric/quarantine workflow.
## CS-03
Kafka offsets are ordered only within a partition and do not represent PostgreSQL source chronology. Actual rule is LSN, same-transaction total order, exact known transport identity, else conflict.
