# Code Deep-Dive: `warehouse/snowflake/01_setup.sql`
**Source of truth:** [`warehouse/snowflake/01_setup.sql`](../../warehouse/snowflake/01_setup.sql).
## Read beside
- **Source:** [`01_setup.sql`](../../warehouse/snowflake/01_setup.sql)
- **Tests:** [`tests/test_mdep12_warehouse_contract.py`](../../tests/test_mdep12_warehouse_contract.py)
- **Architecture:** [`docs/finalization/architecture-implementation-mapping.md`](../finalization/architecture-implementation-mapping.md)
- **Interview topics:** [`docs/finalization/interview-qa.md`](../finalization/interview-qa.md)
## 1. Why this file exists
It creates Snowflake access objects for externally managed Iceberg Silver and a Snowflake-native Gold schema.
## 2. Where it sits in the architecture
Iceberg Silver remains owned by Spark/Flink; Snowflake reads it externally; dbt writes Gold.
## 3. Inputs / outputs / state
Administrator role, S3 bucket/role placeholders, and Iceberg metadata paths enter. Warehouse/database/schemas/integration/external tables/role grants exit.
## 4. Important symbols
`MDEP_TRANSFORM_WH`, `MDEP`, `SILVER_EXT`, `GOLD`, external volume, catalog integration, six Iceberg tables, `MDEP_TRANSFORMER`.
## 5. Execution flow
An operator replaces angle-bracket placeholders, creates Snowflake objects, grants returned external-volume IAM identity access, then creates six external Iceberg table registrations.
## 6. Function-by-function walkthrough
This is SQL, so each block is declarative: `USE ROLE` requires intentional administration; warehouse auto-suspends/resumes; schemas separate external Silver from native Gold. External volume points to S3 Iceberg; catalog integration declares object-store Iceberg. Six table registrations bind metadata paths. Grants permit transformer read-only Silver and create/write Gold.
## 7. Critical code-block reasoning
The script never recreates Silver as Snowflake-native tables. `CREATE OR REPLACE ICEBERG TABLE` registers the external tables, while `MDEP.GOLD` is the dbt target. The comment requiring `DESC EXTERNAL VOLUME` IAM grant makes cross-account access an explicit prerequisite.
## 8. Correctness invariants
- Exactly six approved Silver tables are external.
- Silver is read-only for transformer; Gold is the write destination.
- No secret value is embedded.
## 9. Failure behavior
Bad metadata path/IAM/object-store integration prevents external access. SQL does not hide errors or infer production credentials.
## 10. Tests that protect the behavior
`test_mdep12_warehouse_contract.py` checks six tables, object-store catalog, external Silver separation, and absence of literal secret patterns. **MDEP OFFLINE TESTED.**
## 11. What is not implemented / runtime deferred
**MDEP RUNTIME DEFERRED:** Snowflake execution, IAM grant, S3 access, Iceberg metadata discovery, and dbt connection.
## 12. Production concepts beyond current code
**GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED:** least-privilege environment roles, network policies, cost monitors, and metadata-path promotion automation.
## 13. Common misunderstandings
External registration is not copying Silver into Snowflake. `CREATE OR REPLACE` is not a substitute for a controlled production migration process.
## 14. Interview questions
**Why external Silver but native Gold?** Silver stays owned by processing engines; Snowflake/dbt provides governed analytical modeling without duplicating ingestion ownership.
## 15. 30-second spoken explanation
“The setup script creates a small Snowflake boundary: external Iceberg registrations for six Silver tables and a native Gold schema for dbt. It deliberately separates read-only external Silver from transform-owned Gold, with S3 IAM access as an explicit runtime prerequisite.”
## 16. Senior follow-up discussion
Discuss metadata evolution and access rotation: validate each external metadata path, grant only necessary roles, and use deployment/promotion controls before replacing registrations.
