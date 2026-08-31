# MDEP Practical Engineering Task Bank

Reusable, source-grounded exercises for debugging, review, design, testing, and interview rehearsal. Read a task first; use its matching `solutions/` file only after attempting it. No task is runtime evidence.

**Modes:** (1) answer verbally then check the solution; (2) write a test/SQL/pseudocode patch separately, then compare; (3) answer the interview deliverable without repository notes.

## Training workflow

`Workbook → learner attempt → solution review → training record → repeat under interview time pressure`. Keep attempts in a dated copy of a [training-record template](training-records/README.md), not in canonical task or solution files. The [Module 08 incident record](training-records/08-end-to-end-incidents-session-template.md) uses incident-specific diagnosis, recovery, and proof fields.

**Complete practical-training set:** Modules 01–08 form the final practical-training path. **Recommended order:** 01 Batch/Bronze → 02 Spark Silver → 03 CDC Transport → 04 CDC Current-State → 05 Flink → 06 Snowflake/dbt → 07 Validation/Reconciliation → 08 End-to-End Incidents. For 30 minutes, do one CDC task, one Spark task, and one validation task. For 2 hours, complete 01, 04, 06, and 07. For full practice, complete every module and record missed invariants.

Truth labels: **MDEP IMPLEMENTED** = source/config exists; **MDEP OFFLINE TESTED** = unit/static contract evidence; **MDEP RUNTIME DEFERRED** = physical execution not claimed; **GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED** = discussion context only.

| Module | Focus | Solutions |
|---|---|---|
| [01](01-batch-ingestion-bronze.md) | sources → Bronze | [answers](solutions/01-batch-ingestion-bronze-solutions.md) |
| [02](02-spark-silver.md) | Bronze → batch Silver | [answers](solutions/02-spark-silver-solutions.md) |
| [03](03-cdc-transport-debezium.md) | PostgreSQL → Debezium/Kafka | [answers](solutions/03-cdc-transport-debezium-solutions.md) |
| [04](04-cdc-current-state.md) | LSN/current state | [answers](solutions/04-cdc-current-state-solutions.md) |
| [05](05-flink-streaming.md) | streaming topology | [answers](solutions/05-flink-streaming-solutions.md) |
| [06](06-snowflake-dbt.md) | Silver → Gold | [answers](solutions/06-snowflake-dbt-solutions.md) |
| [07](07-validation-reconciliation.md) | evidence and reconciliation | [answers](solutions/07-validation-reconciliation-solutions.md) |
| [08](08-end-to-end-incidents.md) | cross-layer incidents | [answers](solutions/08-end-to-end-incidents-solutions.md) |
