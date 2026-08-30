# Code Deep-Dive: `ingestion/cdc/contracts.py`

**Source of truth:** [`ingestion/cdc/contracts.py`](../../ingestion/cdc/contracts.py).

## Read beside
- **Source:** [`contracts.py`](../../ingestion/cdc/contracts.py)
- **Tests:** [`tests/test_cdc_contracts.py`](../../tests/test_cdc_contracts.py)
- **Architecture:** [`docs/finalization/architecture-implementation-mapping.md`](../finalization/architecture-implementation-mapping.md)
- **Interview topics:** [`cdc-model.md`](cdc-model.md), [`flink-cdc-job.md`](flink-cdc-job.md)

## 1. Why this file exists
This is the small MDEP-10 transport-boundary contract: which PostgreSQL tables may emit CDC, how topics/key fields map, and which Debezium operation shapes are acceptable.
## 2. Where it sits in the architecture
It sits between connector-produced Kafka messages and downstream consumers. It is intentionally upstream of the richer Flink current-state semantic model.
## 3. Inputs / outputs / state
Inputs are table name, key dictionary, or Debezium value dictionary. Outputs are topic/key tuples/operation classifications. It owns no state.
## 4. Important symbols
`CDC_TABLES`, `PRIMARY_KEYS`, `OPERATIONS`, `topic_name`, `primary_key`, `classify_envelope`.
## 5. Execution flow
A consumer can validate table scope, derive a topic name, verify a key contains the table’s primary key, and classify `r/c/u/d` before handing a valid payload to a richer parser.
## 6. Function-by-function walkthrough
`CDC_TABLES` names only customers, products, orders, and payments. `topic_name` rejects anything else and creates `mdep.commerce.<table>` by default. `primary_key` uses table-specific field tuples and rejects incomplete Kafka keys. `classify_envelope` maps `r` to snapshot read, `c/u/d` to create/update/delete, rejects unknown codes, and requires `after=null` for delete.
## 7. Critical code-block reasoning
The delete check preserves a transport fact before downstream logic: treating a delete with an after-image as a normal update would undermine current-state deletion semantics. This file does not compare LSNs, transactions, offsets, or state—that is deliberately delegated to `cdc_model.py`.
## 8. Correctness invariants
- Only four commerce tables are CDC-owned.
- Every accepted key contains the correct table primary key.
- Operations are restricted to Debezium `r/c/u/d`.
- Delete event after-image is null.
## 9. Failure behavior
Unknown table, incomplete key, unsupported operation, and malformed delete raise `ValueError`; callers must quarantine/reject rather than manufacture a topic/key meaning.
## 10. Tests that protect the behavior
[`tests/test_cdc_contracts.py`](../../tests/test_cdc_contracts.py) tests topic/key scope and operation/delete semantics, plus reads the actual connector JSON. **MDEP OFFLINE TESTED.**
## 11. What is not implemented / runtime deferred
**MDEP RUNTIME DEFERRED:** connector registration, real Kafka key observation, source snapshot/change traffic, and schema evolution through Connect.
## 12. Production concepts beyond current code
**GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED:** schema registry contracts, header validation, consumer DLQ policies, and compatibility/version negotiation.
## 13. Common misunderstandings
This is not a full Debezium parser and does not establish current-state freshness. Topic naming is a scope contract, not proof a topic was created.
## 14. Interview questions
**Why separate this from the Flink parser?** It keeps stable transport ownership/key/operation rules small and reusable, while the Flink model owns LSN/transaction ordering and state transitions.
## 15. 30-second spoken explanation
“`contracts.py` defines the CDC boundary, not the whole streaming semantics: exactly four commerce tables, topic/key mapping, and legal Debezium operation/delete shape. The stateful LSN logic intentionally lives later in the Flink CDC model.”
## 16. Senior follow-up discussion
Discuss schema evolution: primary-key and delete-envelope assumptions should be contract-tested across producer changes; adding a table requires an explicit ownership decision, connector inclusion, and downstream state/table design.
