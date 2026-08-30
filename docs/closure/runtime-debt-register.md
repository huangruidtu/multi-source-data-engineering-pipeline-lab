# V1 runtime-deferred register

**Scope amendment — 2026-08-30:** these items no longer block final V1
completion. They remain an honest list of unexecuted physical integration work
for a future V1.x hands-on validation phase. No row is runtime-validated.

| debt_id | Story/component | Validation not executed and blocker | Class | Evidence/action | Blocks implementation closure? |
| --- | --- | --- | --- | --- |
| RD-08 | MDEP-8 Airflow/PostgreSQL/REST/files | Docker unavailable; DAG, rerun, backfill, Bronze/quarantine not physically observed | D | Compose logs, DAG/task logs, Parquet inspection | No; blocks runtime acceptance |
| RD-09 | MDEP-9 Spark/Iceberg | Java, spark-submit, artifact/input unavailable; Silver merge/snapshots not observed | A/D | Spark output, Iceberg snapshots, stale replay query | No; blocks runtime acceptance |
| RD-10 | MDEP-10 Debezium/Kafka | Docker unavailable; connector, WAL, topics, mutation/tombstone/transaction behavior not observed | A/D | connector/slot/topic/event evidence | No; blocks runtime acceptance |
| RD-11 | MDEP-11 Flink/Kafka/Iceberg/S3 | Docker, AWS credentials, bucket unavailable; job, checkpoint, state, sink commits not observed | A/D | job/checkpoint/archives/current-state evidence | No; blocks runtime acceptance |
| RD-12 | MDEP-12 Snowflake/dbt | dbt executable, credentials, object-store integration unavailable; compile/run/test/freshness not observed | A/D | dbt artifacts, Snowflake queries, reconciliation | No; blocks runtime acceptance |
| RD-13 | MDEP-13 E2E reconciliation | cross-system endpoints unavailable; template execution and failures not observed | B/D | run-specific summary and reconciliation exception outputs | No; blocks runtime acceptance |

Class A must resolve before claiming runtime acceptance. Class B may remain a documented V1 limitation. Class D is an external/environment blocker. None above is an unresolved implementation correctness defect.
