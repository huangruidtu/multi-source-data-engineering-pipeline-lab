# Module 07 — Interview Q&A

### Why are row counts insufficient for reconciliation?
**Direct answer:** layers represent different semantics and grain. **Deep explanation:** Bronze can have duplicates/history, Silver current rows, Gold aggregates/exceptions. **MDEP example:** MDEP-13 uses business-key anti-joins and checks instead of equality. **Why chosen:** avoid false failures. **Follow-up:** What do you compare? bounded keys, duplicates, nulls, aggregates and exceptions. **Senior extension:** SLA and evidence freshness. **Weak answer:** “counts match, so it is correct.”

### What was the false-PASSED bug?
**Direct answer:** runner semantics could report success without durable successful execution evidence. **Deep explanation:** process exit/result must map to evidence path and status. **MDEP example:** MDEP-13 hardens exit-code evidence. **Follow-up:** BLOCKED vs NOT_RUN? missing prerequisite versus not attempted. **Senior extension:** immutable evidence and release gates. **Weak answer:** treating an invoked command as a pass.

### What is still unvalidated?
**Direct answer:** physical batch, Spark/Iceberg, Debezium/Kafka, Flink/S3, Snowflake/dbt and cross-system reconciliation. **MDEP example:** RD-08–RD-13. **Follow-up:** How close them? execute matrix in capable environment and retain evidence. **Senior extension:** evidence-expiry policy. **Weak answer:** hiding debt.
