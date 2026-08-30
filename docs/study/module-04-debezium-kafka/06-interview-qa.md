# Module 04 — Interview Q&A

### CDC versus polling?
**Direct answer:** CDC reads committed database change history; polling periodically compares/selects state. **Deep explanation:** CDC retains mutation intent/order position, while polling can miss/delete ambiguity. **MDEP example:** Debezium reads PostgreSQL WAL for four tables. **Why:** stream current-state changes. **Follow-up:** Does CDC remove duplicates? no. **Senior extension:** slot lag and backfill strategy. **Weak answer:** CDC is automatically exactly once.

### How do you enable transaction metadata?
**Direct answer:** configure `provide.transaction.metadata=true`, then treat it as additional metadata. **Deep explanation:** PostgreSQL boundaries, Debezium metadata and Kafka partition order are separate. **MDEP example:** MDEP-10 corrected the prior `include.transaction` mistake. **Follow-up:** global ordering? no, Kafka only orders within partition. **Senior extension:** same-LSN transaction order downstream. **Weak answer:** claiming BEGIN/END observed here.

### What does a replication slot risk?
**Direct answer:** lag can retain WAL and exhaust source disk. **MDEP example:** `mdep_debezium_slot`. **Follow-up:** monitoring? slot lag/current LSN/connector status. **Senior extension:** incident runbook and failover. **Weak answer:** delete slot casually.
