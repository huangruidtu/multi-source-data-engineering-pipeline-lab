# Module 01 — Core concepts

## Data contract
1. **Definition:** an agreed shape, semantics, owner and quality expectation. 2. **Why:** downstream code needs more than columns. 3. **How:** declare fields, keys, timestamps and rejection rules. 4. **MDEP:** Commerce & Operations. 5. **Reference:** `source-data/contracts/commerce-operations.md`. 6. **Why used:** three source styles need one vocabulary. 7. **Misunderstanding:** a schema alone is a contract. 8. **Failure:** an unowned breaking change silently corrupts Gold. 9. **Production:** version and notify consumers. 10. **Interview:** explain ownership and compatibility.

## Keys, nulls, and provenance
Primary keys identify relational rows; foreign keys express source relationships; business keys identify downstream grain. Required fields reject an unusable record, while nullable fields preserve an honest unknown. `ingestion_id`, `source_locator`, `source_extract_ts`, `ingested_at`, and `record_hash` in `ingestion/batch/bronze.py` make a landing explainable. Hash identifies content; it does not make a record newer.

## Imperfect fixtures
`source-data/files/invalid/` and duplicate fixtures deliberately model malformed JSON, invalid CSV and duplicate content. This teaches a source boundary to retain bad evidence in quarantine rather than quietly repair it.
