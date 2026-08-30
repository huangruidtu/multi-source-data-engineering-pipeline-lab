# Solutions — 01 Batch ingestion and Bronze workbook

## BI-01
**Correct answer/trace:** 429 and 500 are retryable (`RETRYABLE_HTTP`); 429 honors `Retry-After`, here zero. Page 1/2 records accumulate only in memory. Exhausted page-3 500 raises `RuntimeError`; `land_rest` never reaches `publisher().publish`.

**Invariant:** a logically bounded extraction must not publish partial successful-looking Bronze data. **Common wrong answer:** publish page 1/2 then append. **Production consequence:** incomplete canonical landing makes retries/reconciliation ambiguous. **Test:** extend mocked opener with exhausted page-3 500 and assert failure/no publisher call. **Interview answer:** “I collect all pages before publication, so a late-page failure produces no partial Bronze object.” **Senior follow-up:** source snapshot consistency during paging is **GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED**.

## BI-02
**Correct answer/trace:** `file_identity` hashes bytes, so renamed same bytes are duplicate content. `land_files` adds `duplicate_file_content` quarantine evidence under the duplicate file context; it does not publish another canonical valid object.

**Invariant:** filename is locator, content hash is source-file identity; neither is a Silver business key. **Common wrong answer:** treat new filename as new data. **Production consequence:** duplicate source landing. **Test:** two equal-byte differently named files produce equal identity and one duplicate disposition. **Interview answer:** “I use content identity for file replay detection, while downstream natural keys still define business state.” **Senior follow-up:** hash collision/large-file strategy is general production design, not MDEP behavior.

## BI-03
**Correct answer:** random retry key creates duplicate logical operations and disconnects retry from logical date/interval. Reuse `context("{{ ds }}", source, entity)`, deterministic `bronze_key`, and conditional `put_if_absent`; manual historical run changes logical date intentionally. `catchup=False` disables automatic scheduler catchup, not explicit backfill.

**Invariant:** retry reruns the same logical operation. **Common wrong answer:** Airflow retries provide data idempotency. **Production consequence:** duplicate Bronze objects and non-reproducible backfill. **Test:** same context/publish twice yields `already_published`. **Interview answer:** “Airflow schedules retries; deterministic context and conditional publication make the data retry safe.” **Senior follow-up:** real Airflow/S3 behavior is **MDEP RUNTIME DEFERRED**.
