# V1 final spoken story

## 30 seconds

I built a multi-source data engineering lab with batch and CDC paths: Airflow/Spark handle batch Bronze-to-Silver, Debezium/Kafka/Flink handles PostgreSQL changes, and Snowflake/dbt produces Gold. The strongest part is the evidence model: I separate merged implementation and static tests from runtime integrations that have not yet run.

## 60 seconds

The design uses one canonical Silver writer per dataset, replayable Bronze, deterministic stale-event handling, and dbt-owned Gold. I found and fixed real review issues—stale hash-based merge regression, Debezium transaction configuration, a Flink topology stub, Gold schema suffix risk, overuse of incremental models, and a false-PASSED runner. Runtime work is openly registered instead of overstated.

## 2 minutes

Walk from sources through batch and CDC, explain Bronze history versus Silver current state versus Gold analytical semantics, then describe reconciliation by business key rather than row count. Explain that physical Docker/Spark/Flink/S3/Snowflake/dbt proof is still blocked on the host. This demonstrates production judgement: code, tests, evidence, and limitations are separate dimensions.

## 5 minutes

Start with contracts and ownership, then batch idempotency and CDC ordering. Spark compares full versions so replayed older data cannot regress state; Flink distinguishes LSN, Kafka transport, and transaction ordering. Iceberg is shared Silver; dbt models stated Gold grain, keeping payment orphan evidence visible. The MDEP-13 runner records stdout/stderr and exit codes for every attempt. My next step is a disposable runtime environment to execute the debt register, capture reconciliation evidence, and then make runtime-acceptance claims.

## Question sets

**Architecture (10):** Why one Silver writer? Why Bronze history? Why Iceberg? Why external Snowflake Silver? Why dbt Gold? Why CDC and batch? What is fact grain? How are deletes handled? Why HadoopCatalog? Where are ownership boundaries?

**Reliability (10):** How prove idempotency? How replay? How prevent stale overwrite? How recover offsets? What is checkpoint recovery? How quarantine? How reconcile? Why counts fail? What blocks release? How state unvalidated work?

**Data engineering (10):** CDC versus polling? Bronze/Silver/Gold? natural versus surrogate key? SCD choice? freshness? schema evolution? watermark versus LSN? external Iceberg trade-off? dbt test role? current versus historical state?

**Tradeoffs (10):** Flink versus Spark? incremental versus rebuild? physical delete versus soft delete? at-least-once versus exactly-once? catalog choice? external versus native Silver? managed versus local runtime? strict versus warning relationship? retry versus replay? V1 simplicity versus production hardening?
