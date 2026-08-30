# Module 04 — Interview Q&A

### CDC versus polling?
**Direct answer:** CDC reads committed database change history; polling periodically compares/selects state. **Deep explanation:** CDC retains mutation intent/order position, while polling can miss/delete ambiguity. **MDEP example:** Debezium reads PostgreSQL WAL for four tables. **Why:** stream current-state changes. **Follow-up:** Does CDC remove duplicates? no. **Senior extension:** slot lag and backfill strategy. **Weak answer:** CDC is automatically exactly once.

### How do you enable transaction metadata?
**Direct answer:** configure `provide.transaction.metadata=true`, then treat it as additional metadata. **Deep explanation:** PostgreSQL boundaries, Debezium metadata and Kafka partition order are separate. **MDEP example:** MDEP-10 corrected the prior `include.transaction` mistake. **Follow-up:** global ordering? no, Kafka only orders within partition. **Senior extension:** same-LSN transaction order downstream. **Weak answer:** claiming BEGIN/END observed here.

### What does a replication slot risk?
**Direct answer:** lag can retain WAL and exhaust source disk. **MDEP example:** `mdep_debezium_slot`. **Follow-up:** monitoring? slot lag/current LSN/connector status. **Senior extension:** incident runbook and failover. **Weak answer:** delete slot casually.

## Connector and envelope questions

### What does Kafka Connect add to Debezium?

**Direct answer:** Kafka Connect is the managed connector runtime; Debezium is the PostgreSQL source connector running in it. **Deep explanation:** a worker hosts connector instances and their tasks, configuration and offsets. **MDEP implementation:** Compose runs the `connect` service; the validator registers the JSON through Connect REST. **Why this design:** no custom capture application is required. **Failure implication:** worker, connector and task failures have different blast radius. **Likely follow-ups:** Where are offsets stored? What is a task? **Senior extension:** use distributed workers and replicated internal topics. **Common weak answer:** calling Debezium the Kafka broker.

### What does `op=r` mean?

**Direct answer:** it is a Debezium initial snapshot read, not a normal post-start insert. **Deep explanation:** it supplies a baseline row before WAL streaming. **MDEP implementation:** `snapshot.mode=initial`; `classify_envelope` maps `r` to snapshot read. **Why this design:** current-state consumers need a starting row. **Failure implication:** snapshot records can have different metadata availability. **Likely follow-ups:** What happens if snapshot fails? **Senior extension:** snapshot consistency/rebootstrap policy. **Common weak answer:** treating `r` as retry of `c`.

### What is the purpose of `before` and `after`?

**Direct answer:** they describe prior and resulting row images for a mutation. **Deep explanation:** update needs both semantics; delete has `after=null`, while initial snapshot has `before=null`. **MDEP implementation:** contract test rejects delete with a non-null after image. **Why this design:** downstream can apply or audit a change deliberately. **Failure implication:** ignoring delete shape can leave ghost state. **Likely follow-ups:** What is a tombstone? **Senior extension:** payload-size and replica-identity trade-offs. **Common weak answer:** assuming after is always present.

### Why one topic per source table?

**Direct answer:** it keeps entity ownership, key contract and consumers understandable. **Deep explanation:** `mdep.commerce.orders` has different business fields and current-state target than payments. **MDEP implementation:** `topic.prefix` plus four-table include list. **Why this design:** compact V1 contract boundaries. **Failure implication:** a shared topic would need stronger routing/schema discipline. **Likely follow-ups:** How would you scale topics? **Senior extension:** topic governance and compatibility policy. **Common weak answer:** claiming a topic is a relational table.

### What happens when Connect restarts?

**Direct answer:** it should resume using source slot and Connect offset state, but consumers must tolerate replay. **Deep explanation:** source and Connect persistence are separate recovery domains. **MDEP implementation:** validator explicitly restarts Connect; `slot.drop.on.stop=false`. **Why this design:** demonstrate realistic resume risk. **Failure implication:** lost offsets/slot can force recovery decisions. **Likely follow-ups:** What if slot disappears? **Senior extension:** tested rebootstrap plus reconciliation. **Common weak answer:** “restart means no duplicates.”

### What does RF=1 imply in this lab?

**Direct answer:** one broker holds the only replica, so broker loss is a transport outage. **Deep explanation:** replication factor, leader/follower and ISR resilience need multiple brokers. **MDEP implementation:** Compose internal-topic replication settings are all one. **Why this design:** minimize V1 infrastructure. **Failure implication:** no HA evidence. **Likely follow-ups:** What would production change? **Senior extension:** RF/ISR/acks/capacity design. **Common weak answer:** treating KRaft as HA by itself.

### How would you monitor this CDC path?

**Direct answer:** monitor source slot/WAL, connector/task health, Kafka lag and downstream quality/replay evidence. **Deep explanation:** each boundary fails differently. **MDEP implementation:** validator queries `confirmed_flush_lsn`, current WAL and connector status. **Why this design:** evidence-first validation. **Failure implication:** lag can become disk pressure before analysts notice stale data. **Likely follow-ups:** Which alert is blocking? **Senior extension:** SLOs and runbooks. **Common weak answer:** only monitoring consumer lag.
