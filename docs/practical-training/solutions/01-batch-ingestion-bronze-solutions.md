# Solutions — 01 Batch ingestion and Bronze
## BI-01
No Bronze object publishes: `fetch_paginated_json` returns only after all pages; exhausted retryable 500 raises. Invariant: one published object represents complete logical extraction. Wrong answer: publish pages 1–2 then append later; that breaks deterministic retry/reconciliation. **MDEP IMPLEMENTED/OFFLINE TESTED**; live API behavior is deferred. Interview: “I fail before publication to avoid partial canonical landing.”
## BI-02
`file_identity` makes identical bytes a duplicate despite rename; `land_files` creates `duplicate_file_content` quarantine evidence. Test two differently named equal-byte files and assert only one canonical publication. Production implication: filename is locator, not identity.
## BI-03
Random retry keys create duplicate logical operations; Airflow retry is not idempotency. Use the same logical date → `BatchContext` → deterministic key and `put_if_absent`; explicit historical runs remain possible although scheduler catchup is disabled.
