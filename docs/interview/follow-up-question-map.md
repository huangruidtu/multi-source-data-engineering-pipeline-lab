# MDEP Follow-up Question Map

Use this after the [2 / 5 / 15 minute explanations](project-explanation-2-5-15.md). Every answer is deliberately short: answer directly, give two reasons, then stop. Review the linked source before extending an answer.

## Top 15 Must-Nail Follow-Ups

| ID | Question | Why it is must-nail |
| --- | --- | --- |
| Q01 | What problem did MDEP solve? | frames the portfolio honestly |
| Q02 | Why batch plus CDC? | establishes architecture judgment |
| Q05 | How is Bronze idempotent? | tests reliability fundamentals |
| Q09 | How do you prevent stale Spark replay? | tests freshness correctness |
| Q13 | What are publication and replication slot? | tests CDC operational knowledge |
| Q17 | How does version_decision work? | core CDC semantics |
| Q18 | What happens to delete LSN 420 after update LSN 500? | tests safety under replay |
| Q21 | Why ValueState instead of a Python dictionary? | tests streaming-state understanding |
| Q25 | Why one Silver writer per dataset? | tests ownership model |
| Q26 | Why Snowflake and dbt if Silver exists? | tests analytics boundary |
| Q27 | Why does incremental merge not solve deletes? | tests dbt correctness |
| Q30 | Testing versus reconciliation? | tests reliability maturity |
| Q31 | Why are matching counts insufficient? | tests data reasoning |
| Q36 | Why did you not run full E2E? | tests truthful scope control |
| Q37 | What would you productionize first? | tests prioritization |

## 1. Project framing and architecture

### Q01 — What problem were you trying to solve? **HIGH**
**Triggered by:** “I built MDEP as a Commerce and Operations project.” **Why interviewer asks:** scope and intent. **Direct answer (20–40 sec):** “I used one coherent project to explore source ingestion, CDC current state, Bronze/Silver/Gold ownership, and analytical modeling. The goal was implementation depth and interview reasoning, not a collection of tools.” **Senior deep-dive:** bounded V1; source ownership; failure evidence. **Key invariant / takeaway:** architecture decisions must serve one data truth. **Review sources:** [script](project-explanation-2-5-15.md), [baseline](../closure/v1-baseline.md).

### Q02 — Why combine batch and CDC? **HIGH**
**Triggered by:** “There are batch and CDC paths.” **Why interviewer asks:** processing-model selection. **Direct answer (20–40 sec):** “Batch handles bounded reference/API/file ingestion and replay; CDC handles PostgreSQL commerce changes and current state. They complement each other, but they do not both write the same Silver dataset.” **Senior deep-dive:** latency; backfill; ownership matrix. **Key invariant / takeaway:** separate paths need explicit writer boundaries. **Review sources:** [flow](../finalization/end-to-end-data-flow.md), [map](../code-deep-dive/master-map.md).

### Q03 — Why not one engine for everything? **MEDIUM**
**Triggered by:** “Spark owns references and Flink owns CDC.” **Why interviewer asks:** trade-offs. **Direct answer (20–40 sec):** “Spark fits bounded batch validation and merge work; Flink fits keyed, stateful event application. The choice is based on semantics, not claiming one engine is universally better.” **Senior deep-dive:** state; scheduling; operational cost. **Key invariant / takeaway:** tool ownership follows data behavior. **Review sources:** [Spark](../code-deep-dive/silver-batch.md), [Flink](../code-deep-dive/flink-cdc-job.md).

### Q04 — What was your role? **HIGH**
**Triggered by:** “I built this project.” **Why interviewer asks:** ownership and honesty. **Direct answer (20–40 sec):** “I built MDEP as a learning and portfolio implementation. I made and documented its bounded design choices, then used tests and exercises to validate semantics offline; I do not present it as a production system I operated.” **Senior deep-dive:** decisions; review evidence; deferred lab. **Key invariant / takeaway:** portfolio work must not be presented as production history. **Review sources:** [scope](../planning/v1-scope.md), [expanded](project-explanation-expanded.md).

## 2. Batch, Airflow, and Bronze

### Q05 — How is Bronze idempotent? **HIGH**
**Triggered by:** “Bronze has deterministic identity.” **Why interviewer asks:** retry safety. **Direct answer (20–40 sec):** “A BatchContext derives an ingestion ID from logical date, UTC interval, source, and entity. That produces a deterministic Bronze key, and conditional publication makes a retry converge on the same logical output.” **Senior deep-dive:** backfill; manifests; object-store races. **Key invariant / takeaway:** retry reruns one logical operation. **Review sources:** [Bronze](../code-deep-dive/bronze-batch-publication.md), [BI-03](../practical-training/01-batch-ingestion-bronze.md).

### Q06 — Is Airflow retry enough for idempotency? **HIGH**
**Triggered by:** “Airflow retries the batch.” **Why interviewer asks:** orchestration boundary. **Direct answer (20–40 sec):** “No. Airflow schedules a retry, but data idempotency comes from reusing the same context and deterministic destination. A random output key on retry would create duplicate logical landings.” **Senior deep-dive:** catchup versus backfill; task retry policy. **Key invariant / takeaway:** scheduler retry is not business identity. **Review sources:** [DAG](../code-deep-dive/airflow-bronze-dag.md), [BI-03](../practical-training/01-batch-ingestion-bronze.md).

### Q07 — What if REST page three fails? **HIGH**
**Triggered by:** “All pages are collected before publication.” **Why interviewer asks:** partial-data handling. **Direct answer (20–40 sec):** “The extractor raises before Bronze publication; pages one and two stay only in memory. I retry the same bounded operation after preserving failure evidence, rather than publishing an incomplete canonical source set.” **Senior deep-dive:** source consistency; retryable status; reconciliation. **Key invariant / takeaway:** complete bounded extract or no canonical object. **Review sources:** [extractors](../code-deep-dive/source-extractors.md), [E2E-01](../practical-training/08-end-to-end-incidents.md).

### Q08 — Why quarantine instead of dropping bad input? **MEDIUM**
**Triggered by:** “Bronze preserves rejection evidence.” **Why interviewer asks:** data-quality judgment. **Direct answer (20–40 sec):** “Dropping invalid input loses the evidence needed to diagnose or replay it. Quarantine retains the payload, reason, locator, and context while keeping invalid data out of canonical Silver.” **Senior deep-dive:** triage ownership; retention; alert thresholds. **Key invariant / takeaway:** rejection must remain observable. **Review sources:** [Bronze](../code-deep-dive/bronze-batch-publication.md), [BI-02](../practical-training/01-batch-ingestion-bronze.md).

## 3. Spark Silver

### Q09 — How do you prevent stale Spark replay? **HIGH**
**Triggered by:** “Spark uses lexicographic freshness.” **Why interviewer asks:** merge correctness. **Direct answer (20–40 sec):** “The in-batch winner and target MERGE use the same full version tuple. For locations that is updated time, extract time, ingestion time, then hash, so an older business version cannot replace newer Silver.” **Senior deep-dive:** business clock; tie-breakers; Iceberg merge. **Key invariant / takeaway:** target and batch ordering must agree. **Review sources:** [Spark](../code-deep-dive/silver-batch.md), [SS-01](../practical-training/02-spark-silver.md).

### Q10 — Why is record_hash not freshness? **HIGH**
**Triggered by:** “Hash is the final tie-breaker.” **Why interviewer asks:** replay bug awareness. **Direct answer (20–40 sec):** “A hash says payloads differ, not which version is newer. MDEP only compares it after the business, extraction, and ingestion evidence ties; otherwise an old changed replay could regress state.” **Senior deep-dive:** deterministic conflict choice; governance. **Key invariant / takeaway:** payload difference is not temporal evidence. **Review sources:** [Spark](../code-deep-dive/silver-batch.md), [SS-02](../practical-training/02-spark-silver.md).

### Q11 — Why MERGE and quarantine? **MEDIUM**
**Triggered by:** “Spark creates Silver current/reference tables.” **Why interviewer asks:** validation lifecycle. **Direct answer (20–40 sec):** “MERGE updates one current row per approved business key; quarantine holds invalid or non-winning evidence. That lets Silver stay trustworthy without erasing why an input was excluded.” **Senior deep-dive:** schema evolution; snapshot inspection. **Key invariant / takeaway:** valid current state and rejected evidence have different destinations. **Review sources:** [Spark](../code-deep-dive/silver-batch.md), [SS-03](../practical-training/02-spark-silver.md).

## 4. CDC transport, Debezium, and Kafka

### Q12 — Why Debezium instead of polling? **HIGH**
**Triggered by:** “PostgreSQL changes go through Debezium.” **Why interviewer asks:** CDC rationale. **Direct answer (20–40 sec):** “Debezium reads database change history through logical decoding, so change events retain operation and source-position context. Polling is a different batch pattern and is not the canonical current-state path in MDEP.” **Senior deep-dive:** snapshot; schema changes; source load. **Key invariant / takeaway:** CDC carries change semantics, not just later snapshots. **Review sources:** [connector](../code-deep-dive/debezium-postgres-connector.md), [CT-01](../practical-training/03-cdc-transport-debezium.md).

### Q13 — What are publication and replication slot? **HIGH**
**Triggered by:** “The connector uses explicit publication and slot.” **Why interviewer asks:** PostgreSQL CDC knowledge. **Direct answer (20–40 sec):** “The publication defines which source-table changes are exposed. The slot tracks consumer progress so Debezium can resume; preserving it helps recovery but can retain WAL if progress stops.” **Senior deep-dive:** `restart_lsn`; `confirmed_flush_lsn`; source ownership. **Key invariant / takeaway:** resume state has source-disk cost. **Review sources:** [connector](../code-deep-dive/debezium-postgres-connector.md), [CT-03](../practical-training/03-cdc-transport-debezium.md).

### Q14 — Why disable publication auto-create? **MEDIUM**
**Triggered by:** “Publication scope is explicit.” **Why interviewer asks:** source governance. **Direct answer (20–40 sec):** “It prevents a connector from silently changing source replication scope. The required publication is an explicit database prerequisite that can be reviewed with its table ownership.” **Senior deep-dive:** privileges; migrations; change approval. **Key invariant / takeaway:** CDC scope is not a connector side effect. **Review sources:** [JSON](../../ingestion/cdc/debezium-postgres-connector.json), [CT-01](../practical-training/03-cdc-transport-debezium.md).

### Q15 — Delete versus tombstone? **HIGH**
**Triggered by:** “Tombstones are enabled.” **Why interviewer asks:** transport/business separation. **Direct answer (20–40 sec):** “The Debezium `op=d, after=null` record is the business delete candidate. A later null-value Kafka tombstone supports log compaction; MDEP treats it as transport semantics, not a second business deletion.” **Senior deep-dive:** keys; replay; compaction. **Key invariant / takeaway:** tombstone and delete are distinct events. **Review sources:** [CDC model](../code-deep-dive/cdc-model.md), [CT-02](../practical-training/03-cdc-transport-debezium.md).

## 5. CDC current-state semantics

### Q16 — Why is raw CDC not current state? **HIGH**
**Triggered by:** “Flink owns current-state Silver.” **Why interviewer asks:** event/state distinction. **Direct answer (20–40 sec):** “Events can arrive late, replay, or be deletes. MDEP first decides whether an event is allowed to change the last accepted version, then applies that decision to current state.” **Senior deep-dive:** changelog versus state; replay. **Key invariant / takeaway:** syntax alone never grants state mutation. **Review sources:** [CDC model](../code-deep-dive/cdc-model.md), [CS-01](../practical-training/04-cdc-current-state.md).

### Q17 — How does version_decision work? **HIGH**
**Triggered by:** “LSN is first.” **Why interviewer asks:** exact semantics. **Direct answer (20–40 sec):** “It compares PostgreSQL source LSN first; within a known same transaction it can use total order; known identical transport identity proves exact replay. Missing evidence produces a conservative conflict, not a guess.” **Senior deep-dive:** source versus transport clocks. **Key invariant / takeaway:** source position dominates transport position. **Review sources:** [CDC model](../code-deep-dive/cdc-model.md), [CS-02](../practical-training/04-cdc-current-state.md).

### Q18 — What happens to delete LSN 420 after update LSN 500? **HIGH**
**Triggered by:** “Stale deletes are protected.” **Why interviewer asks:** safety. **Direct answer (20–40 sec):** “It is `LOWER_LSN`, so it is ignored before state deletion. The accepted version and the current order remain at LSN 500; delete has no special privilege.” **Senior deep-dive:** Gold propagation; regression test. **Key invariant / takeaway:** a delete must win the same ordering. **Review sources:** [CS-01](../practical-training/04-cdc-current-state.md), [E2E-02](../practical-training/08-end-to-end-incidents.md).

### Q19 — Same LSN means replay, right? **HIGH**
**Triggered by:** “Same transaction order can resolve ties.” **Why interviewer asks:** ambiguity handling. **Direct answer (20–40 sec):** “No. Same LSN can contain distinct changes, especially inside a transaction. Only known identity/order evidence can establish replay; otherwise MDEP returns `EQUAL_POSITION_CONFLICT` conservatively.” **Senior deep-dive:** transaction metadata; quarantine metrics. **Key invariant / takeaway:** equal position is not equal event. **Review sources:** [CDC model](../code-deep-dive/cdc-model.md), [CS-02](../practical-training/04-cdc-current-state.md).

### Q20 — Why entity:primary_key? **MEDIUM**
**Triggered by:** “State is keyed safely.” **Why interviewer asks:** state-key design. **Direct answer (20–40 sec):** “A customer and order can share the same textual ID. Prefixing the entity prevents state collision while retaining the business key inside each entity.” **Senior deep-dive:** composite keys; partitioning. **Key invariant / takeaway:** state identity must match entity semantics. **Review sources:** [CDC model](../code-deep-dive/cdc-model.md), [CS-03](../practical-training/04-cdc-current-state.md).

## 6. Flink streaming

### Q21 — Why ValueState instead of a Python dictionary? **HIGH**
**Triggered by:** “Flink holds keyed current state.” **Why interviewer asks:** runtime guarantees. **Direct answer (20–40 sec):** “A dictionary is only local memory. ValueState is managed per key and participates in checkpoint/restart recovery; the dictionary model is only a pure semantic oracle for tests.” **Senior deep-dive:** repartitioning; state backends; recovery. **Key invariant / takeaway:** semantic oracle and durable runtime state are different roles. **Review sources:** [Flink](../code-deep-dive/flink-cdc-job.md), [CDC model](../code-deep-dive/cdc-model.md).

### Q22 — What happens to malformed CDC messages? **MEDIUM**
**Triggered by:** “The job has a parser and side output.” **Why interviewer asks:** bad-data behavior. **Direct answer (20–40 sec):** “The parser rejects invalid envelope/entity/LSN semantics and routes evidence to a side-output/quarantine path. It should not manufacture current state from malformed input.” **Senior deep-dive:** DLQ policy; observability. **Key invariant / takeaway:** invalid event evidence is retained, not applied. **Review sources:** [Flink](../code-deep-dive/flink-cdc-job.md), [Module 05](../practical-training/05-flink-streaming.md).

### Q23 — Did you prove exactly-once and restart recovery? **HIGH**
**Triggered by:** “The job configures checkpoints.” **Why interviewer asks:** truth boundary. **Direct answer (20–40 sec):** “I implemented the topology and checkpoint-related configuration, but I do not claim end-to-end exactly-once or recovery proof. Those runtime exercises are explicitly deferred; V1 validates the semantic model offline.” **Senior deep-dive:** sink commits; checkpoint evidence; failure drill. **Key invariant / takeaway:** configuration is not runtime proof. **Review sources:** [Flink](../code-deep-dive/flink-cdc-job.md), [debt](../closure/runtime-debt-register.md).

## 7. Iceberg and ownership

### Q24 — Why Iceberg? **HIGH**
**Triggered by:** “Silver is Iceberg.” **Why interviewer asks:** table-format rationale. **Direct answer (20–40 sec):** “It is the canonical Silver table format for the intended Spark/Flink interoperability, merge/current-state semantics, schema evolution, and external Snowflake access. Physical cross-engine integration is deferred.” **Senior deep-dive:** catalog; snapshots; storage/compute separation. **Key invariant / takeaway:** Iceberg is canonical Silver, not merely a file format. **Review sources:** [setup](../code-deep-dive/snowflake-iceberg-setup.md), [expanded](project-explanation-expanded.md).

### Q25 — Why one Silver writer per dataset? **HIGH**
**Triggered by:** “Spark and Flink have different ownership.” **Why interviewer asks:** source of truth. **Direct answer (20–40 sec):** “Spark writes batch reference Silver and Flink writes CDC commerce state. If both upserted the same current-state table, version rules and source-of-truth ownership would become ambiguous.” **Senior deep-dive:** routing matrix; multi-writer conflict. **Key invariant / takeaway:** one dataset has one canonical writer. **Review sources:** [flow](../finalization/end-to-end-data-flow.md), [E2E](../practical-training/08-end-to-end-incidents.md).

## 8. Snowflake, dbt, and Gold

### Q26 — Why Snowflake and dbt if Silver exists? **HIGH**
**Triggered by:** “Snowflake accesses Silver externally.” **Why interviewer asks:** analytics boundary. **Direct answer (20–40 sec):** “Silver is technically trustworthy storage; Snowflake/dbt make business-facing dimensions, facts, and marts. Snowflake reads external Silver, while dbt owns Gold rather than becoming a second Silver writer.” **Senior deep-dive:** grain; external access; warehouse roles. **Key invariant / takeaway:** analytics consumption is not Silver ownership. **Review sources:** [dbt Gold](../code-deep-dive/dbt-gold-models.md), [SD-01](../practical-training/06-snowflake-dbt.md).

### Q27 — Why does incremental merge not solve deletes? **HIGH**
**Triggered by:** “fct_orders is incremental.” **Why interviewer asks:** dbt semantics. **Direct answer (20–40 sec):** “Merge upserts selected source rows, but absent source rows are not automatically target deletes. `fct_orders` uses a separate anti-join post-hook to remove target keys missing from enriched current state.” **Senior deep-dive:** applied/updated clocks; full refresh. **Key invariant / takeaway:** selection is not deletion synchronization. **Review sources:** [Gold](../code-deep-dive/dbt-gold-models.md), [SD-02](../practical-training/06-snowflake-dbt.md).

### Q28 — Why rebuild fct_payments? **MEDIUM**
**Triggered by:** “Payments is a table rebuild.” **Why interviewer asks:** cost/correctness trade-off. **Direct answer (20–40 sec):** “At this scale, rebuild makes payment delete and order-relink correctness simpler than adding another incremental delete contract. Incremental is a performance choice, not a maturity badge.” **Senior deep-dive:** scale threshold; partitions. **Key invariant / takeaway:** choose the simplest correct lifecycle. **Review sources:** [Gold](../code-deep-dive/dbt-gold-models.md), [SD-02](../practical-training/06-snowflake-dbt.md).

### Q29 — How do you handle missing FX and orphans? **HIGH**
**Triggered by:** “Uncertainty is preserved.” **Why interviewer asks:** analytical correctness. **Direct answer (20–40 sec):** “A missing DKK rate leaves converted value null and flags `missing_dkk_rate`; it is not converted to zero. Left joins retain orphan payments, while warning-level relationship checks signal the broken link without deleting evidence.” **Senior deep-dive:** remediation; exception metrics. **Key invariant / takeaway:** preserve uncertainty and signal it. **Review sources:** [intermediate](../code-deep-dive/dbt-intermediate-models.md), [SD-03](../practical-training/06-snowflake-dbt.md).

## 9. Validation and reconciliation

### Q30 — Testing versus reconciliation? **HIGH**
**Triggered by:** “I separate validation from reconciliation.” **Why interviewer asks:** reliability depth. **Direct answer (20–40 sec):** “Testing asks whether a check executed and passed. Reconciliation asks whether layers express the same intended business truth despite history, current-state, and aggregate-grain differences.” **Senior deep-dive:** evidence bundle; ownership. **Key invariant / takeaway:** passing tests do not replace cross-layer comparison. **Review sources:** [runner](../code-deep-dive/validation-runner-reconciliation.md), [VR-02](../practical-training/07-validation-reconciliation.md).

### Q31 — Why are matching counts insufficient? **HIGH**
**Triggered by:** “I use anti-joins.” **Why interviewer asks:** data debugging. **Direct answer (20–40 sec):** “Counts can match while ten Silver keys are missing in Gold and ten stale Gold keys replace them. I compare key sets, then attributes, then facts aggregated to mart grain.” **Senior deep-dive:** R03/R05/R08/R09. **Key invariant / takeaway:** count equality is not key-set equality. **Review sources:** [reconciliation](../../validation/reconciliation/README.md), [E2E-03](../practical-training/08-end-to-end-incidents.md).

### Q32 — Why check LASTEXITCODE? **MEDIUM**
**Triggered by:** “The runner prevents false PASS.” **Why interviewer asks:** automation correctness. **Direct answer (20–40 sec):** “A native process can exit nonzero without a PowerShell exception. The runner records stdout, stderr, exit code, and working directory, and classifies a nonzero code as FAILED.” **Senior deep-dive:** self-test exit 7; evidence retention. **Key invariant / takeaway:** no exception is not native-command success. **Review sources:** [runner](../code-deep-dive/validation-runner-reconciliation.md), [VR-01](../practical-training/07-validation-reconciliation.md).

## 10. Failure and recovery

### Q33 — How would you handle WAL growth? **HIGH**
**Triggered by:** “A preserved slot can retain WAL.” **Why interviewer asks:** safe operations. **Direct answer (20–40 sec):** “I would inspect connector status and PostgreSQL slot activity, restart/confirmed LSNs, WAL growth, and disk headroom before touching the slot. Dropping it may lose resume position and require snapshot/reconciliation.” **Senior deep-dive:** alerting; source-owner escalation. **Key invariant / takeaway:** do not destroy recovery state blindly. **Review sources:** [CT-03](../practical-training/03-cdc-transport-debezium.md), [E2E](../practical-training/08-end-to-end-incidents.md).

### Q34 — What would you preserve before repairing a Silver/Gold mismatch? **MEDIUM**
**Triggered by:** “I use a hypothesis tree.” **Why interviewer asks:** incident discipline. **Direct answer (20–40 sec):** “I preserve bounded interval, run ID, query text, exact anti-join key sets, row snapshots, timestamps, counts, and the repair decision. Then I isolate missing selection from stale Gold or upstream Silver truth.” **Senior deep-dive:** R03/R08/R09/R10/R05. **Key invariant / takeaway:** evidence before mutation. **Review sources:** [E2E-03](../practical-training/08-end-to-end-incidents.md), [VR-02](../practical-training/07-validation-reconciliation.md).

## 11. Scope, trade-offs, and productionization

### Q35 — What is actually implemented and tested? **HIGH**
**Triggered by:** “V1 is implementation and offline validation.” **Why interviewer asks:** portfolio credibility. **Direct answer (20–40 sec):** “The repository has batch, Spark, CDC/Flink, Snowflake/dbt, validation, and reconciliation implementations. Focused Python/static tests validate their contracts; I distinguish that from physical integrated runtime evidence.” **Senior deep-dive:** tests by boundary; evidence matrix. **Key invariant / takeaway:** implementation and runtime acceptance are separate dimensions. **Review sources:** [baseline](../closure/v1-baseline.md), [expanded](project-explanation-expanded.md).

### Q36 — Why did you not run full E2E? **HIGH**
**Triggered by:** “Runtime is deferred.” **Why interviewer asks:** judgment under constraint. **Direct answer (20–40 sec):** “For V1 I prioritized complete source-grounded implementation and offline correctness over pretending that an unavailable local stack was production proof. The deferred matrix defines the exact V1.x commands, environments, and evidence needed next.” **Senior deep-dive:** runtime debt; acceptance criteria. **Key invariant / takeaway:** deferred is not passed. **Review sources:** [debt](../closure/runtime-debt-register.md), [matrix](../../validation/mdep-13-validation-matrix.yml).

### Q37 — What would you productionize first? **HIGH**
**Triggered by:** “V1 has deliberate limitations.” **Why interviewer asks:** prioritization. **Direct answer (20–40 sec):** “First I would execute and retain runtime evidence for the existing paths, then add secrets/IAM/RBAC, connector and pipeline monitoring, alerting, recovery drills, and integration CI. I would not add unrelated platforms before proving the current design.” **Senior deep-dive:** SLOs; HA; cost. **Key invariant / takeaway:** validate before expanding tool scope. **Review sources:** [baseline](../closure/v1-baseline.md), [scope](../planning/v1-scope.md).

### Q38 — Which choices are lab-specific? **MEDIUM**
**Triggered by:** “This is interview-oriented V1.” **Why interviewer asks:** production maturity. **Direct answer (20–40 sec):** “The bounded stack, value-only PyFlink limitation, parallelism-one lab setup, local publisher, and deferred runtime exercises are lab-specific. The ownership, idempotency, ordering, evidence, and reconciliation principles transfer to production.” **Senior deep-dive:** scaling; managed services; HA. **Key invariant / takeaway:** concepts transfer even when lab mechanics do not. **Review sources:** [expanded](project-explanation-expanded.md), [debt](../closure/runtime-debt-register.md).

### Q39 — How would you handle schema evolution? **MEDIUM**
**Triggered by:** “Iceberg supports additive evolution.” **Why interviewer asks:** change safety. **Direct answer (20–40 sec):** “MDEP keeps schema changes additive in its Spark exercise and treats contracts as explicit. In production I would version producer/consumer contracts, test compatibility, and validate downstream models before accepting a changed source.” **Senior deep-dive:** Iceberg metadata; dbt contracts; rollout. **Key invariant / takeaway:** schema change is a compatibility decision, not just a new column. **Review sources:** [Spark](../code-deep-dive/silver-batch.md), [master map](../code-deep-dive/master-map.md).

### Q40 — What would you monitor first? **HIGH**
**Triggered by:** “Runtime validation is next.” **Why interviewer asks:** operational priorities. **Direct answer (20–40 sec):** “I would monitor source/connector health and slot/WAL growth, bounded ingestion success and quarantine volume, Silver freshness/duplicate keys, Gold/dbt results, and reconciliation exceptions. The specific SLOs are production work, but the evidence points are already defined.” **Senior deep-dive:** lag; freshness; error budgets. **Key invariant / takeaway:** observe business correctness as well as process health. **Review sources:** [matrix](../../validation/mdep-13-validation-matrix.yml), [VR-03](../practical-training/07-validation-reconciliation.md).

## Common Follow-Up Chains

1. “I use Debezium for CDC.” -> Why Debezium? -> What is a publication? -> What is a slot? -> What if connector progress stops? -> How would you prove recovery?
2. “Spark merges into Silver.” -> How is newer decided? -> Why not hash? -> What does stale replay do? -> How is it tested?
3. “Flink owns current state.” -> Why not a dictionary? -> What is keyBy? -> How do stale deletes behave? -> Did you prove checkpoint recovery?
4. “dbt builds Gold.” -> Why incremental orders? -> How are deletes handled? -> Why rebuild payments? -> How do you reconcile Gold?
5. “Bronze is replayable.” -> What if REST page three fails? -> Why not publish partial data? -> What makes retry safe? -> How do you prove completeness?
6. “I use reconciliation.” -> Why not counts? -> What is an anti-join? -> How do deletes differ from freshness? -> What evidence do you retain?
7. “V1 is runtime deferred.” -> What did you implement? -> What did offline tests prove? -> What is next? -> What would production change?
