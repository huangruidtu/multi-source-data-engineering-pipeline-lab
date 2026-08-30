# Module 01 — Code walkthrough

Reading order: `source-data/contracts/commerce-operations.md` → `source-data/postgres/schema.sql` → `source-data/postgres/seed.sql` → `source-data/files/scenarios/README.md` → `scripts/validate-sources.ps1`.

| File | Purpose, inputs/outputs, and what to notice |
| --- | --- |
| `commerce-operations.md` | dataset ownership, keys, fields, quality and evolution policy |
| `schema.sql` | `commerce` tables, primary/foreign keys and source constraints |
| `seed.sql` | deterministic domain rows for repeatable extraction |
| `rest-api/app.py` | local reference endpoints and pagination shape |
| `files/valid` / `files/invalid` | valid, duplicate, malformed and invalid fixtures |
| `bronze.py` | converts source records into the common provenance envelope |

Interview notice: source validation must not turn an invalid raw record into invisible loss; `quarantine_record` retains original payload and reason.
