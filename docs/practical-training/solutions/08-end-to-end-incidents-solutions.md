# Solutions — 08 End-to-end incidents workbook

Read these only after attempting [the workbook](../08-end-to-end-incidents.md). Each answer is a source-grounded incident-analysis path, not evidence that the incident or a physical end-to-end run occurred.

## E2E-01 — Partial REST extraction and Bronze incident

**Correct answer:** The immediate failure belongs to the REST extraction boundary: `fetch_paginated_json` exhausts retries for page 3 and raises `RuntimeError`. Pages A–H must not become canonical Bronze. The extractor deliberately accumulates records in memory and returns only after every page succeeds, so `land_rest` cannot reach canonical publication with a successful-looking partial source set.

**Incident timeline:** At T0/T1 pages 1/2 are merely accumulated. At T2 all page-3 retries fail; T3 aborts before publication. At T4 an Airflow retry must reuse the same logical date, `[start,end)` interval, source/entity, and therefore the same `BatchContext.ingestion_id` and deterministic `bronze_key`. After T5 obtains A–L, T6 can conditionally create exactly the canonical object for that logical operation. Only then should T7 Spark consume the published Bronze input; T8 compares the bounded source/business-key set with expected Silver current state.

**Hypothesis tree:** First distinguish transient retryable HTTP failure from a permanently failing page, pagination bug, already-published conflicting logical object, or a source-consistency issue. Capture requested URL/page, HTTP status/error, retries, interval/context, intended key, and publication result before mutation. Do not infer a source snapshot or S3 behavior without actual evidence.

**Cross-layer reasoning:** Publishing A–H would make the Bronze object look canonical despite an unknown missing tail. Spark could validate and deduplicate the available records perfectly yet still construct incomplete reference state. A later “fix” would raise difficult questions: overwrite versus append, whether source keys absent from A–H mean deletion, which aggregates are incomplete, and how to reconcile a partial object against source truth. Airflow retry schedules execution; data idempotency comes from stable context/deterministic object identity and conditional publication.

**Correct recovery decision:** Abort before canonical publication, retain failed-extraction evidence, and retry the same bounded logical operation. If a canonical object already exists, inspect its metadata/key and do not blindly replace it. The safe recovery boundary is one complete extraction before one canonical publication.

**Reconciliation plan:** Record a run ID and interval; establish the complete source set after recovery; show one deterministic Bronze identity/no partial canonical object; compare appropriate business keys and source attributes into Silver, record quarantined/excluded keys, and check expected current-state uniqueness/null rules. Do not treat raw Bronze/Silver counts as enough.

**Proof-of-correctness criteria:** Complete pages for the declared interval, deterministic context/key, no partial canonical object, evidence of the failed attempt and successful bounded retry, business-key reconciliation, and expected Silver state. Physical REST/Airflow/S3/Spark/reconciliation execution remains `MDEP RUNTIME DEFERRED`.

**Correctness invariant:** A logically bounded source extraction either publishes one complete canonical Bronze object or publishes none; retries rerun the same logical operation.

**Common wrong answer:** Publish good pages now and append/fix page 3 later, or say Airflow retry alone prevents duplicate data.

**Production consequence:** Partial canonical landing creates silent incompleteness, ambiguous backfills, duplicate/replacement risk, and difficult downstream reconciliation.

**Interview-ready English answer:** “I never publish a paginated source partially. In MDEP the extractor returns only after all pages succeed, so a page-three failure aborts before canonical Bronze publication. An Airflow retry reuses the same interval and deterministic BatchContext/key; after a complete retry, I reconcile source business keys and expected Silver state before calling recovery complete.”

**Senior follow-up discussion:** In a production API, explicitly decide snapshot-consistency strategy, cursor semantics, rate-limit policy, source version watermark, and safe resumability. Those are **GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED** unless added with evidence.

## E2E-02 — Stale CDC delete through Gold

**Correct answer:** T1 is syntactically a valid Debezium business-delete envelope because `op='d'` and `after=null`; valid envelope shape does not authorize a current-state transition. Comparing the candidate LSN `0/420` with the accepted `0/500`, `version_decision` returns `LOWER_LSN`. `apply_current_state` returns `lower_lsn_ignored`: it changes neither the stored accepted version nor `orders:1001` current row.

**Incident timeline and cross-layer reasoning:** The transport boundary can classify topic/key/delete shape, then `parse_debezium` produces a candidate event. The semantic boundary applies source version ordering before state mutation. Since T1 loses, Flink keyed state continues to represent the update, and intended CDC Silver `core_orders` state retains `orders:1001`. External Snowflake Silver access and dbt staging/intermediate therefore still see the order in the intended design. `fct_orders`’ incremental merge/post-hook must not remove it: the post-hook removes Gold targets absent from `int_orders_enriched`, but the stale candidate must not make the authoritative Silver-derived input absent. `mart_daily_sales` consequently should continue to include its current fact under the model’s grain.

**Hypothesis tree:** Separate malformed delete, valid stale delete, newer accepted delete, replay, parser/version bug, and downstream delete-sync defect. Evidence starts with the exact topic/key/envelope/LSN and pre-event accepted version, then parser/decision output and current-state/Silver key, followed by dbt source/fact evidence. Transport receipt would only prove a message was delivered; semantic correctness requires proof that it was ordered and applied (or ignored) correctly.

**Correct recovery decision:** Do not manually delete Gold to follow a stale candidate. Preserve event/version evidence, allow the source-order rule to ignore it, and investigate if the current state actually regressed. A real accepted delete is different: it must be newer under MDEP ordering, remove current state/Silver truth, then become eligible for the dbt delete synchronization path.

**Reconciliation plan:** For the selected scope, R03 checks Silver orders against Gold facts; R09 checks current-state duplicate keys; R08 is applied to accepted source deletes to verify downstream absence/documented retention, not to every syntactically valid delete envelope. Retain key, LSN/transaction evidence, version decision, query text, counts, exceptions, and explicit result.

**Proof-of-correctness criteria:** `orders:1001` remains present in expected current state and intended Silver/Gold, no unexplained R03 anti-join exists, duplicate check is clean, and accepted-version evidence remains `0/500`. `cdc_model.py` logic and static dbt contracts are `MDEP IMPLEMENTED`/`MDEP OFFLINE TESTED`; Kafka/Flink/Iceberg/Snowflake/dbt physical flow and reconciliation are `MDEP RUNTIME DEFERRED`.

**Correctness invariant:** A delete is a state transition only after it wins the same source-version ordering as an update; a lower-LSN delete cannot remove newer current state or cause downstream Gold deletion.

**Common wrong answer:** “`op=d` means delete immediately,” “the Kafka delivery offset can rescue an older LSN,” or “delete the Gold row to be safe.”

**Production consequence:** A stale delete bypassing ordering causes irreversible-looking current-state/Gold regression and destroys the evidence needed to diagnose replay behavior.

**Interview-ready English answer:** “A valid Debezium delete is still only a candidate transition. For order 1001, LSN 420 loses to accepted LSN 500, so the model returns LOWER_LSN and changes neither state nor accepted version. Silver remains present, so the dbt orders post-hook has no upstream absence to synchronize; I reconcile key presence and version evidence rather than deleting Gold manually.”

**Senior follow-up discussion:** Validate the entire chain later with retained topic/partition/offset diagnostics, Flink checkpoint/state evidence, Iceberg snapshots, Snowflake/dbt run artifacts, and R03/R08 outputs. Those runtime proof points are not present in V1.

## E2E-03 — Counts match, keys do not

**Correct answer:** Equal count `900` on Silver and Gold is false reassurance. The two R03 exception sets demonstrate unequal business-key sets: 10 Silver keys are missing in Gold and 10 extra Gold keys are absent in Silver. Counts can cancel even when every exception is meaningful. Further, key-set equality would not prove attributes match, and attribute equality would not prove the mart aggregate is correct.

**Incident timeline/hypothesis tree:** Preserve the run ID, scope, exact anti-join keys, and row snapshots first. Branch missing-in-Gold keys toward missed incremental selection—`fct_orders` selects by `applied_at` or `updated_at` maxima—wrong scope, intended eligibility/exclusion, current Silver input defect, or query/model mismatch. Branch extra-in-Gold keys toward a source/current-state delete that failed to reach Gold, a post-hook lifecycle/execution issue, wrong scope, or stale Gold state. Before choosing either branch, inspect current Silver rows and timestamps/version evidence for every exception, then model configuration/compiled query/post-hook evidence if runtime is available.

**Evidence collection order:**

1. Preserve anti-join SQL, run ID, interval, counts, exact two key sets, and decisions.
2. Snapshot selected Silver/Gold rows and their `updated_at`/`applied_at` values; retain source/version evidence when available.
3. Run R09 for duplicate current-state keys and R10 required-null/contract anomalies.
4. Apply R08 only to known accepted source deletes, then inspect fct_orders selection and anti-join-delete behavior.
5. After key/attribute agreement, run R05 at `order_date + currency` mart grain; do not use it to substitute for R03.

**Cross-layer reasoning:** R03 establishes stated eligible Silver-to-Gold fact keys/values. R08 tests delete propagation semantics, R09 protects one-row-per-business-key current state, R10 identifies input contract exceptions, and R05 compares the fact aggregation with `mart_daily_sales`. A broken Gold delete hook can leave extra Gold keys; missed incremental selection can leave missing Gold keys. Neither conclusion follows from count equality alone. Before executing templates, verify query fields still agree with model contracts; template existence is not a passed run.

**Correct recovery decision:** Do not blindly truncate/rebuild Gold, alter a post-hook, or repair source rows solely from matching counts. After isolation, use the smallest bounded action: controlled dbt rerun/rebuild for proven Gold materialization defect; upstream correction/reprocess for proven Silver truth defect; query/scope correction for reconciliation error; or a code/configuration change followed by focused validation. Preserve all pre-repair evidence and document the decision.

**Reconciliation plan:** Post-repair, rerun R03 for key-set equality/documented exceptions, R08 for accepted deletes, R09/R10 for Silver quality, compare representative required attributes, then run R05 at mart grain. Save exact queries, scope, timestamps, counts, exception keys, repair decision, and post-repair outcome.

**Proof-of-correctness criteria:** Key anti-joins are empty or each exception is explicitly justified; required current-state keys are unique/non-null; accepted deletes have correct downstream representation; selected attributes agree; fact-to-mart aggregate reconciles at matching grain. Cross-system execution remains `MDEP RUNTIME DEFERRED`.

**Correctness invariant:** Count equality is not key-set equality; key-set equality is not attribute equality; attribute equality is not aggregate correctness. Each must be tested at its own semantic grain.

**Common wrong answer:** “Counts match, so no incident,” “the fault must be dbt,” or “rebuild Gold immediately.” These skip evidence preservation and competing source/Silver/model hypotheses.

**Production consequence:** Offset key errors can persist behind green count dashboards, creating wrong customer/order results and untraceable financial aggregates.

**Interview-ready English answer:** “Matching Silver and Gold counts do not prove matching business truth. I anti-join keys first, preserve both exception sets and timestamps, then distinguish missing incremental selection from stale Gold deletion or an upstream Silver problem. After a bounded repair I prove key, attribute, delete, duplicate/null, and mart-grain aggregate correctness separately.”

**Senior follow-up discussion:** Define automated exception thresholds, ownership, approval for full rebuilds, an exception ageing policy, and immutable reconciliation bundles. These are **GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED** unless explicitly implemented and evidenced.
