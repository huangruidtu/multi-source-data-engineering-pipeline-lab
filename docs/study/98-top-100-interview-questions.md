# Top 100 MDEP interview questions

Each answer is a concise starting point; use the module Q&A for the project-file mapping and spoken expansions.

## 1. Overall architecture

1. **What problem does MDEP solve?** It demonstrates reliable multi-source Commerce & Operations data flow; MDEP separates replayable evidence from analytical state. *Follow-up:* Which source owns each dataset?
2. **Describe the two paths.** Batch is sources→Airflow→Bronze→Spark→Iceberg; CDC is WAL→Debezium→Kafka→Flink→Iceberg. *Follow-up:* Where do they converge?
3. **Why one canonical writer?** Competing current-state writers cause conflict; Spark owns references and Flink owns CDC entities. *Follow-up:* How enforce it?
4. **ETL versus ELT?** MDEP transforms files to Silver with Spark and transforms Silver to Gold in Snowflake/dbt. *Follow-up:* Why locations differ?
5. **Bronze/Silver/Gold roles?** evidence, trusted state, consumer grain. *Follow-up:* Why not count them equally?
6. **Data lake versus lakehouse?** a lake stores files; Iceberg adds table metadata/snapshots. *Follow-up:* What does a warehouse add?
7. **Lakehouse versus warehouse?** Iceberg owns open Silver; Snowflake provides Gold compute/governance. *Follow-up:* Who writes Silver?
8. **Current state versus history?** Bronze retains mutations/history; Silver represents latest valid key state. *Follow-up:* How reconcile?
9. **Why preserve provenance?** locator/time/identity explain and replay a record. *Follow-up:* Which fields do MDEP landings add?
10. **What is runtime-unvalidated?** physical batch, Spark, CDC, Flink, Snowflake and reconciliation. *Follow-up:* How would you close debt?

## 2. Batch and Airflow

11. **What does Airflow own?** scheduling, dependencies, retry/backfill; not distributed transforms. *Follow-up:* How can it trigger Spark?
12. **What is a DAG?** a directed task dependency graph; MDEP’s DAG coordinates Bronze. *Follow-up:* What is a task failure?
13. **Logical date versus run time?** logical date identifies intended interval, not wall clock. *Follow-up:* Why does backfill need it?
14. **What is a data interval?** bounded source time range for a run. *Follow-up:* Is end inclusive?
15. **How is batch idempotent?** stable context derives stable key and conditional publication. *Follow-up:* What happens on retry?
16. **Why deterministic object paths?** reruns locate the same canonical object. *Follow-up:* What records the contents?
17. **What is the Bronze manifest?** count/hash/interval companion evidence. *Follow-up:* Is it a transaction log?
18. **How does REST pagination work?** collect pages by `next_page` before publish. *Follow-up:* Why avoid partial landing?
19. **How handle REST 429?** bounded retry and `Retry-After`. *Follow-up:* Why not retry forever?
20. **How handle REST 5xx?** retry only known transient codes, then fail cleanly. *Follow-up:* What metrics matter?
21. **How is PostgreSQL batch extraction bounded?** optional `updated_at` interval predicates. *Follow-up:* Why is CDC still needed?
22. **What does a backfill test?** repeatability across historical intervals. *Follow-up:* How avoid overlap?
23. **What is quarantine?** rejected evidence with payload/reason/locator. *Follow-up:* Is it a cleaned table?
24. **Airflow versus EventBridge-style scheduler?** Airflow adds DAG semantics/backfill/task state. *Follow-up:* When choose simpler scheduler?
25. **Why not transform in Airflow?** orchestration is not distributed data processing. *Follow-up:* What belongs in Spark?

## 3. Spark

26. **Driver versus executor?** driver plans/co-ordinates; executors run distributed tasks. *Follow-up:* Where does a shuffle occur?
27. **Transformation versus action?** transformations are lazy plan steps; actions execute. *Follow-up:* Give MDEP action examples.
28. **What is a DataFrame?** distributed tabular logical plan. *Follow-up:* Why inspect schema?
29. **What are jobs/stages/tasks?** action creates jobs, shuffles split stages, tasks process partitions. *Follow-up:* How diagnose slow task?
30. **What is partitioning?** split data for parallel work. *Follow-up:* Why can too many partitions hurt?
31. **What is a shuffle?** redistribute by key between stages. *Follow-up:* Which MDEP operations may shuffle?
32. **What is broadcast join?** replicate small side to avoid large shuffle. *Follow-up:* When not use it?
33. **What is skew?** uneven key distribution creates stragglers. *Follow-up:* How measure before salting?
34. **How deduplicate a batch?** natural key plus deterministic version order. *Follow-up:* Why not hash only?
35. **What does `explain` help with?** logical/physical plan and join/shuffle choices. *Follow-up:* What runtime evidence is absent?

## 4. Iceberg and lakehouse

36. **Why is Parquet insufficient for Silver?** files lack atomic table metadata/current-state MERGE. *Follow-up:* What remains useful about Parquet?
37. **What is Iceberg metadata?** table pointer/snapshot metadata describes manifests/files. *Follow-up:* Why atomic commit?
38. **What is a snapshot?** a consistent table version. *Follow-up:* How does it aid recovery?
39. **What is a manifest?** metadata listing data files/partition statistics. *Follow-up:* Why plan pruning?
40. **What is MERGE for?** apply inserts/updates to current state. *Follow-up:* What decides update eligibility?
41. **What is HadoopCatalog in MDEP?** S3-backed Iceberg catalog configuration. *Follow-up:* Is it runtime proven?
42. **Why only batch references in Spark Silver?** avoid CDC dual writer. *Follow-up:* Which entities are CDC-owned?
43. **How does MDEP prevent stale replay?** business timestamp→extract→ingest→hash. *Follow-up:* What is a no-op?
44. **Why is hash a tie-breaker?** different content does not prove freshness. *Follow-up:* Describe MDEP-9 bug.
45. **Iceberg versus Snowflake native table?** external open Silver versus warehouse-native Gold. *Follow-up:* Who manages storage?

## 5. CDC and Debezium

46. **What is WAL?** PostgreSQL write-ahead change log. *Follow-up:* Why logical decoding?
47. **What is a publication?** selected tables available for logical replication. *Follow-up:* Which MDEP tables?
48. **What is a replication slot?** retained source position for consumer. *Follow-up:* What risk does lag cause?
49. **CDC versus polling?** change history versus periodic state reads. *Follow-up:* Does CDC guarantee exactly once?
50. **What is snapshot mode initial?** emit baseline rows then stream changes. *Follow-up:* What op identifies snapshot?
51. **What are r/c/u/d?** snapshot/create/update/delete Debezium operations. *Follow-up:* What are before/after?
52. **What is a tombstone?** null Kafka compaction marker after delete. *Follow-up:* Does it delete Silver itself?
53. **How enable transaction metadata?** `provide.transaction.metadata=true`. *Follow-up:* Was it observed at runtime?
54. **What was the MDEP-10 bug?** `include.transaction` was incorrect for Debezium 3.0. *Follow-up:* How protected?
55. **What is source LSN?** database source position, distinct from Kafka offset. *Follow-up:* Why important downstream?

## 6. Kafka

56. **What is a topic?** named retained record stream. *Follow-up:* How are MDEP topics named?
57. **What is a partition?** ordered shard of a topic. *Follow-up:* Is partition number freshness?
58. **What is an offset?** position in one partition. *Follow-up:* Is it database order?
59. **Why key by primary key?** retain per-entity order/routing. *Follow-up:* Does that give global order?
60. **What is a consumer group?** coordinated consumers sharing topic partitions. *Follow-up:* What happens on rebalance?
61. **What ordering does Kafka guarantee?** only within a partition. *Follow-up:* Why transaction metadata differs?
62. **What is retention/replay?** old records remain available for consumers. *Follow-up:* What duplicate risk follows?
63. **What is at-least-once delivery?** processing may repeat after failure. *Follow-up:* How does MDEP converge?
64. **Why is Kafka not a current-state database?** it transports log records; state application is downstream. *Follow-up:* What owns state?
65. **Connector restart implication?** duplicate/replay and lag require idempotent consumers. *Follow-up:* What source risk remains?

## 7. Flink

66. **What makes a stream unbounded?** no final complete input. *Follow-up:* Why differs from Spark batch?
67. **JobManager versus TaskManager?** coordination versus task execution. *Follow-up:* What runs an operator?
68. **What does `keyBy` do?** routes a key to one logical state owner. *Follow-up:* What is MDEP’s key?
69. **What is ValueState?** persisted per-key state. *Follow-up:* Which two states exist?
70. **What is a checkpoint?** periodic recoverable snapshot. *Follow-up:* What does it not prove?
71. **What is a savepoint?** intentional managed snapshot for operations. *Follow-up:* When use it?
72. **How handle lower LSN?** reject stale mutation. *Follow-up:* Why not event time?
73. **How handle equal LSN?** compare same transaction total order, then exact transport replay. *Follow-up:* Unknown identity?
74. **What is event time?** time represented by event. *Follow-up:* What is processing time?
75. **What is a watermark?** progress estimate for event-time operations. *Follow-up:* Why not freshness?
76. **What is bounded out-of-orderness?** tolerated late event-time interval. *Follow-up:* Does it reorder WAL?
77. **What is backpressure?** slow downstream constrains upstream. *Follow-up:* What metrics investigate?
78. **What is rescaling risk?** state redistribution and key/load changes. *Follow-up:* Why savepoint?
79. **Exactly once versus at least once?** end-to-end exactly once needs source/checkpoint/sink proof. *Follow-up:* What is MDEP claim?
80. **How handle CDC delete/tombstone?** delete clears current row; tombstone is ignored for state. *Follow-up:* Is raw evidence retained?

## 8. Snowflake

81. **What is a virtual warehouse?** independent compute for queries. *Follow-up:* How reduce idle cost?
82. **Compute versus storage?** Snowflake separates elastic compute from persisted data. *Follow-up:* What storage is external here?
83. **What are micro-partitions?** Snowflake’s storage organization supporting pruning. *Follow-up:* Is it an Iceberg partition?
84. **What is an external volume?** configured object-store location/access for external Iceberg. *Follow-up:* What IAM is required?
85. **What is catalog integration?** Snowflake configuration to locate Iceberg metadata. *Follow-up:* Is it configured with placeholders?
86. **Why auto-suspend/auto-resume?** balance availability and credits. *Follow-up:* What must be measured?
87. **What is clustering/pruning?** reduce scanned data through physical organization/statistics. *Follow-up:* Is it runtime tuned here?
88. **Snowflake versus Databricks conceptually?** warehouse-centric SQL versus lakehouse platform; MDEP uses neither as replacement claim. *Follow-up:* Why excluded technology scope?

## 9. dbt and dimensional modeling

89. **What does `source()` do?** declares external source relation. *Follow-up:* What does `ref()` add?
90. **What does `ref()` do?** declares model dependency/DAG. *Follow-up:* Why useful for lineage?
91. **What is a fact grain?** exact meaning of one row. *Follow-up:* MDEP order grain?
92. **Business versus surrogate key?** business identifies entity; surrogate supports stable dimension joins. *Follow-up:* What makes it deterministic?
93. **SCD1 versus SCD2?** replace current attributes versus preserve history intervals. *Follow-up:* Which MDEP uses?
94. **Why fct_orders incremental?** bounded merge learning/cost case with delete sync. *Follow-up:* Why not payments?
95. **Why fct_payments rebuild?** deletion/relink correctness beats fragile watermark. *Follow-up:* Is incremental always better?
96. **What tests exist?** not-null, unique, accepted values and warning relationship. *Follow-up:* Why warning orphan payment?
97. **How handle missing FX?** visible missing-rate flag, no fabricated conversion. *Follow-up:* How would production remediate?

## 10. Reliability, validation and production

98. **Why are row counts insufficient?** history/current/aggregate layers have different semantics. *Follow-up:* What checks replace them?
99. **BLOCKED versus NOT_RUN?** required environment unavailable versus deliberately unattempted. *Follow-up:* What proves PASSED?
100. **What would you improve first in production?** execute and retain runtime evidence, then add scoped IAM/secrets, SLIs, lag/quality/cost monitoring, lifecycle and incident/DR controls. *Follow-up:* Which MDEP debt closes first?
