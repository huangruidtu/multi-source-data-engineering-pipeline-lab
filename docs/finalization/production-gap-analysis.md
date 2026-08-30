# Production Gap Analysis

These are recommendations, **not implemented V1 capabilities**.

| Area | Current V1 | Production likely requires |
| --- | --- | --- |
| Availability | local/single-node-oriented Compose design | multi-node HA, tested failover, capacity planning |
| Kafka | configured topics/contracts; no live cluster evidence | multi-broker replication, ISR/acks policy, ACLs, monitoring, partition sizing |
| Flink | parallelism 1; checkpoint/restart configuration | HA control plane, durable checkpoint storage, restore drills, backpressure and state-size monitoring |
| Kafka metadata | PyFlink value-only deserializer | metadata-capable/custom deserializer and end-to-end replay verification |
| Iceberg | Hadoop-style catalog design | managed/catalog service, compaction/expiration, snapshot governance, concurrent-writer policy |
| Object storage | optional S3 publisher/configuration | IAM, encryption, lifecycle, cross-account access, DR policy |
| Schema governance | code contracts and dbt declarations | registry/compatibility policy, change approval, lineage/catalog integration |
| Security | placeholders and no embedded secrets | secret manager, least-privilege IAM/RBAC, key rotation, audit logs |
| Observability | designed quality gates/evidence templates | metrics, logs, traces, alerting, data-quality SLOs, operational dashboards |
| Snowflake | external volume/catalog DDL template | governed roles, cost controls, metadata refresh/recovery, workload management |
| CI/CD | offline tests run locally | pipeline gates, dependency scans, ephemeral integration environment, promotion controls |
| DR | recovery reasoning only | documented RPO/RTO, restore rehearsals, region/account strategy |

## How to use this in an interview

Do not present this table as a backlog already implemented. Say: “V1 makes the
core correctness rules reviewable. Before production, I would first verify the
existing end-to-end path and then add operational capabilities in this order:
security, observability, durable state/catalog, HA, and repeatable integration
tests.” This sequence avoids adding product complexity before proving the current
contracts under real failure conditions.

## Non-goals retained

V1 does not introduce Databricks, Delta Lake, Redshift, BigQuery, Paimon, Fluss,
StarRocks, Dagster, Prefect, Airbyte, or Fivetran. Those may be valid elsewhere;
they are not remedies for the specific V1 gaps above.
