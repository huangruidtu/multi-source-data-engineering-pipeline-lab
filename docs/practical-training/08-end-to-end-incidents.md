# 08 — End-to-end incidents workbook

Attempt this file before [the matching solutions](solutions/08-end-to-end-incidents-solutions.md). Record work in a copy of [the incident session template](training-records/08-end-to-end-incidents-session-template.md). This is a cross-layer reasoning exercise over implemented code and offline/static evidence, not a claim that an end-to-end runtime incident was executed.

## Reusable incident method

Use this compact sequence for every task and in interviews:

1. **Symptom** — state the observable problem without presuming cause.
2. **Scope** — bound the logical interval, business keys, owners, and affected layers.
3. **Hypotheses** — list competing explanations before selecting one.
4. **Evidence** — preserve source facts, logs/query text, keys, timestamps, and status before mutation.
5. **Isolation** — compare the correct source of truth and layer semantics to eliminate hypotheses.
6. **Recovery** — choose the smallest safe action that preserves canonical history/current state.
7. **Reconciliation** — compare business keys, attributes, deletes, duplicates, and aggregates at their intended grains.
8. **Proof of correctness** — retain the bounded scope, evidence bundle, explicit decision, and post-recovery checks.
9. **Communication** — state what is implemented/offline tested, what evidence was actually observed, and what remains runtime deferred.

For every incident, name the source of truth, owner of the affected layer, current-state invariant, evidence captured before mutation, recovery boundary, reconciliation after recovery, and unproven runtime claims.

## E2E-01 — Partial REST extraction and Bronze incident

- **Difficulty:** Senior
- **Task type:** INCIDENT RESPONSE / CROSS-LAYER TRACE / RECOVERY DESIGN
- **Architecture path involved:** REST source -> `fetch_paginated_json` -> batch pipeline/context -> deterministic Bronze publication -> Airflow retry -> Spark batch Silver -> reconciliation.
- **Source files/docs to inspect:** `ingestion/batch/extractors.py` (`fetch_paginated_json`); `ingestion/batch/pipeline.py` (`land_rest`, context/publisher use); `ingestion/batch/bronze.py` (`BatchContext`, `bronze_key`, conditional publication); `orchestration/dags/bronze_ingestion.py`; `processing/spark/silver_batch.py`; `validation/reconciliation/README.md`; [end-to-end data flow](../finalization/end-to-end-data-flow.md); Modules [01](01-batch-ingestion-bronze.md), [02](02-spark-silver.md), and [07](07-validation-reconciliation.md).
- **Incident timeline / concrete facts:**

```text
T0  REST page 1 succeeds: items A–D; next_page=2.
T1  REST page 2 succeeds: items E–H; next_page=3.
T2  REST page 3 returns HTTP 500 for every configured retry.
T3  fetch_paginated_json raises RuntimeError; records A–H only exist in process memory.
T4  Airflow retries the same logical date and [start,end) interval.
T5  All three pages succeed; complete source set A–L is returned.
T6  The batch pipeline attempts deterministic Bronze publication.
T7  Spark processes canonical Bronze into batch/reference Silver.
T8  Reconciliation is prepared for the same bounded interval.
```

Someone proposes at T3: “Publish pages 1–2 now. We can fix page 3 later.”
- **Primary symptom:** A bounded paginated REST extraction cannot complete, while a partial data set is available in memory.
- **Candidate hypotheses:** transient source HTTP failure; retryable upstream throttling/outage; wrong pagination continuation; downstream publication defect; a prior canonical object for the same `BatchContext`; source data changing during paging (**GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED** unless evidence supports it).
- **Required evidence:** requested URLs/page numbers, retry count/status and response/error text, logical date and half-open data interval, `BatchContext` fields/`ingestion_id`, intended deterministic `bronze_key`, publication result, source record/business-key set after successful retry, Silver validation/quarantine results, and reconciliation run ID/query/exceptions.
- **Engineering deliverables:**
  1. Identify the immediate failing owner/component and state whether pages A–H may become canonical Bronze. Explain why.
  2. Trace T0–T8 and explain why `fetch_paginated_json` accumulating all pages before return protects the publication boundary.
  3. Explain why partial Bronze would make Silver completeness, reruns, downstream aggregates, and business-key reconciliation ambiguous.
  4. Explain why Airflow retry must reuse the same logical work identity, and how `BatchContext`, deterministic keying, and conditional/absent-object publication protect retry and backfill behavior.
  5. Choose the safest recovery action. Specify what evidence is retained from the failed attempt and what must be checked before allowing Spark/downstream processing.
  6. Give a proof-of-correctness plan: bounded interval, one deterministic Bronze identity, no partial canonical object, source/business-key reconciliation, Silver expected-current-state/duplicate/null checks, and an explicit decision.
  7. Give a 30–60 second interview explanation.
- **Recovery / rollback decision:** Decide whether to abort before canonical publication, retry the same operation, or investigate an already-published conflicting object. Do not invent deletion of unknown artifacts; preserve evidence before any remediation.
- **Reconciliation / proof-of-correctness plan:** Use a source snapshot or source-extract evidence for the bounded interval; compare appropriate source/Silver keys and attributes, record excluded/quarantined records, then verify Silver’s expected current state rather than a bare Bronze count.
- **Truth-boundary constraints:** No REST, Airflow, S3/Parquet, Spark, or reconciliation runtime execution is asserted. Code/configuration intent is `MDEP IMPLEMENTED`; focused contracts are `MDEP OFFLINE TESTED`; physical retries, publication, Spark runs, and cross-layer reconciliation are `MDEP RUNTIME DEFERRED`.
- **Competency trained:** partial-landing prevention, orchestration-versus-data idempotency, and bounded recovery evidence.
- **Learner workspace/template:**

```text
Symptom/scope: ___
Immediate owner: ___
Can A–H publish canonically? ___
Hypotheses/evidence: ___
Stable BatchContext/key: ___
Recovery decision: ___
Downstream risk if partial publishes: ___
Reconciliation/proof plan: ___
Unproven runtime claims: ___
Interview explanation: ___
```

## E2E-02 — Stale CDC delete through Gold

- **Difficulty:** Senior
- **Task type:** CROSS-LAYER STATE TRACE / CORRECTNESS REVIEW / RECONCILIATION DESIGN
- **Architecture path involved:** PostgreSQL/Debezium transport -> Kafka -> Flink parser -> `version_decision` -> keyed current state -> CDC Silver Iceberg -> Snowflake external Silver -> dbt enrichment/fact -> Gold mart -> reconciliation.
- **Source files/docs to inspect:** `ingestion/cdc/contracts.py`; `ingestion/cdc/debezium-postgres-connector.json`; `processing/flink/cdc_model.py` (`parse_debezium`, `version_decision`, `apply_current_state`); `processing/flink/flink_cdc_job.py`; `warehouse/snowflake/01_setup.sql`; `analytics/dbt/models/staging/stg_orders.sql`; `analytics/dbt/models/intermediate/int_orders_enriched.sql`; `analytics/dbt/models/marts/fct_orders.sql`; `analytics/dbt/models/marts/mart_daily_sales.sql`; `validation/reconciliation/README.md`; Modules [03](03-cdc-transport-debezium.md), [04](04-cdc-current-state.md), [05](05-flink-streaming.md), [06](06-snowflake-dbt.md), and [07](07-validation-reconciliation.md).
- **Incident timeline / concrete facts:**

```text
T0 accepted current event: entity=orders, key=1001, op=u, source_lsn=0/500,
   after={order_id:"1001", order_status:"shipped"}.
T1 later candidate: topic=mdep.commerce.orders, key={order_id:"1001"},
   op=d, before={order_id:"1001", order_status:"created"}, after=null,
   source_lsn=0/420.
T2 a downstream Gold/dbt lifecycle is requested after the CDC event path.
```

- **Primary symptom:** A syntactically valid delete arrives after a newer accepted update and could be mistaken for a current-state deletion.
- **Candidate hypotheses:** malformed delete envelope; valid but stale source event; newer real delete; transport replay; version-ordering implementation defect; downstream Gold stale/deletion-sync defect. Do not collapse these hypotheses.
- **Required evidence:** topic/key/envelope fields, source LSN and transaction/transport metadata when present, pre-event accepted version and current row, `version_decision` result, resulting state/Silver key, dbt source/fact key state, fct_orders selection/delete-hook evidence, and R03/R08/R09 results with run ID and exception keys.
- **Engineering deliverables:**
  1. Decide whether T1 is a valid Debezium business-delete envelope and distinguish envelope validity from authorization to mutate current state.
  2. Apply actual MDEP ordering semantics to `0/420` versus accepted `0/500`; state the `VersionDecision`, `apply_current_state` result, and final intended state.
  3. Trace the consequence across Flink keyed state, Silver `core_orders`, external Silver access, dbt intermediate/fact/mart. Answer whether `fct_orders` post-hook should remove `orders:1001` in this scenario and why its delete synchronization depends on upstream Silver truth.
  4. Contrast this stale delete with a newer/accepted delete. Explain what must be true before a current row and then a Gold fact can disappear.
  5. Choose R03, R08, R09 and any other relevant actual checks to prove no regression; distinguish transport success evidence from semantic-current-state correctness evidence.
  6. State each boundary as `MDEP IMPLEMENTED`, `MDEP OFFLINE TESTED`, or `MDEP RUNTIME DEFERRED`, then give a 30–60 second interview explanation.
- **Recovery / rollback decision:** Do not manually delete Gold to “match” the candidate. Preserve event/version evidence, reject/ignore the stale transition according to the semantic decision, and investigate only if current-state/Silver evidence shows an actual regression.
- **Reconciliation / proof-of-correctness plan:** Retain `orders:1001` source/event evidence and LSNs; show the expected Silver/Gold key is present, R09 duplicate result is zero, and R03 has no unexplained anti-join for the selected scope. Use R08 only for accepted source deletes, not as proof that every syntactic delete must propagate.
- **Truth-boundary constraints:** `MDEP IMPLEMENTED` includes parser/order/state and dbt delete-sync logic; `MDEP OFFLINE TESTED` includes pure CDC and static dbt contracts; Kafka/Flink/Iceberg/Snowflake/dbt execution and cross-layer results are `MDEP RUNTIME DEFERRED`.
- **Competency trained:** end-to-end delete correctness, transport/semantic separation, and source-of-truth-driven recovery.
- **Learner workspace/template:**

```text
Envelope validity / semantic eligibility: ___
VersionDecision and state result: ___
Silver and Gold final state: ___
Why post-hook does/does not delete: ___
Stale versus accepted delete: ___
Evidence/R03/R08/R09 plan: ___
Truth labels: ___
Interview explanation: ___
```

## E2E-03 — Counts match, keys do not

- **Difficulty:** Senior
- **Task type:** RECONCILIATION INCIDENT / HYPOTHESIS TREE / SAFE REPAIR DESIGN
- **Architecture path involved:** authoritative Silver current state -> Snowflake external Silver -> dbt staging/intermediate -> `fct_orders` incremental merge/post-hook -> Gold marts -> R03/R08/R09 reconciliation.
- **Source files/docs to inspect:** `validation/reconciliation/README.md`; `validation/reconciliation/snowflake-dbt.sql`; `validation/reconciliation/spark-iceberg.sql`; `analytics/dbt/models/marts/fct_orders.sql`; `analytics/dbt/models/intermediate/int_orders_enriched.sql`; `analytics/dbt/models/staging/stg_orders.sql`; `analytics/dbt/models/schema.yml`; `validation/quality-gates.yml`; Modules [02](02-spark-silver.md), [06](06-snowflake-dbt.md), and [07](07-validation-reconciliation.md).
- **Incident timeline / concrete facts:** A bounded run reports Silver `core_orders` count `900` and Gold `fct_orders` count `900`. R03’s business-key anti-join instead produces 10 `silver_not_gold` order IDs and 10 `gold_not_silver` order IDs. Preserve the exact two sets before any re-run or repair.
- **Primary symptom:** Count equality masks a nonempty, two-sided business-key mismatch.
- **Candidate hypotheses:** missing/late Gold incremental selection (`applied_at`/`updated_at` predicate); stale Gold row retained because the fct_orders anti-join post-hook did not execute; incorrect eligibility/exclusion logic; wrong bounded source/Silver scope; Silver current-state defect; duplicate or null key anomaly; manual/independent Gold mutation (**GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED** unless evidence supports it); query/template/model mismatch.
- **Required evidence:** run ID; bounded interval/scope; exact missing-in-Gold and extra-in-Gold key sets; Silver/Gold row snapshots; `updated_at`, `applied_at`, source/version evidence where available; compiled/query text and model configuration; post-hook execution evidence if runtime exists; R08 delete evidence; R09 duplicate output; R10 null/contract output; counts and explicit decision.
- **Engineering deliverables:**
  1. Explain the three distinct invariants: count equality is not key-set equality; key-set equality is not attribute equality; attribute equality is not aggregate correctness.
  2. Draw and follow this investigation tree: symptom -> key anti-join -> inspect missing-in-Gold keys -> inspect extra-in-Gold keys -> compare Silver current state -> inspect dbt selection/delete behavior -> inspect timestamps/version evidence -> determine fault class -> repair -> rerun reconciliation.
  3. For missing-in-Gold keys, explain how missed incremental selection, scope mismatch, or eligibility behavior could cause the outcome. For extra-in-Gold keys, explain how stale rows/deletion-sync failure, scope mismatch, or source/Silver truth issues could cause it.
  4. Explain how R03, R08, R09, and relevant R10/R05 checks are applied without jumping to a single root cause from counts alone.
  5. Specify what data must be preserved before repair, choose safe repair options appropriate to each fault class, and define post-repair proof criteria at key, attribute, and aggregate grains.
  6. Give a 30–60 second interview explanation.
- **Recovery / rollback decision:** Do not blindly truncate/rebuild Gold or change delete hooks from the symptom alone. Preserve the key sets and model/query evidence first; then select a bounded rerun, a controlled full rebuild, source/Silver repair, or a configuration fix only after isolating the responsible layer.
- **Reconciliation / proof-of-correctness plan:** Rerun R03 for exact key-set equality and documented exceptions; validate R08 for accepted deletes, R09 for zero duplicate Silver keys, R10 for required-field/null contracts, compare selected attributes, then run R05 at `order_date + currency` mart grain. Record post-repair query text, counts, keys, timestamps, decision, and remaining exceptions.
- **Truth-boundary constraints:** MDEP has implemented templates/model contracts and offline/static validation. No physical Silver/Snowflake/dbt anti-join, post-hook, rebuild, or repair is claimed. These are `MDEP RUNTIME DEFERRED` exercises.
- **Competency trained:** hypothesis-driven data incident response, key-level reconciliation, and safe repair communication.
- **Learner workspace/template:**

```text
Symptom and bounded scope: ___
Missing-in-Gold / extra-in-Gold keys: ___
Hypothesis tree: ___
First evidence to preserve: ___
Likely fault class and alternatives: ___
R03/R08/R09/R10/R05 plan: ___
Safe repair boundary: ___
Post-repair proof: ___
Unproven runtime claims: ___
Interview explanation: ___
```
