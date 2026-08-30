# Module 01 — Interview Q&A

### What makes a data contract more than a schema?
**Direct answer:** it includes semantics, ownership, keys, quality and evolution expectations. **Deep explanation:** types cannot say who resolves a late record or whether a field is optional. **MDEP project example:** `commerce-operations.md` pairs entities with ownership and provenance. **Why chosen:** batch and CDC consumers need stable responsibilities. **Likely follow-up:** How evolve it? add compatible fields, version and test consumers. **Senior extension:** contract ownership/SLOs. **Weak answer:** “JSON Schema is the contract.”

### Why retain malformed data?
**Direct answer:** to make rejection auditable and replayable. **Deep explanation:** deletion hides source quality and prevents reprocessing after a fix. **MDEP example:** `quarantine_record` stores payload, reason and locator. **Follow-up:** Is quarantine Silver? No, it is failure evidence. **Senior extension:** set retention/access controls. **Weak answer:** silently defaulting invalid values.

### How do you model intentional bad data?
**Direct answer:** deterministic fixtures test boundary behavior. **MDEP example:** invalid JSON/CSV and duplicate category files. **Follow-up:** Is it production data? No, it is a repeatable failure lab. **Senior extension:** property-based contract tests. **Weak answer:** treating seed data as a production quality guarantee.
