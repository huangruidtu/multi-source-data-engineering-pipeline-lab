# MDEP-7 Interview Talking Points

## A. 30-second project summary

**English:** “This is a Commerce & Operations data-engineering lab designed around repeatable batch, CDC, and streaming exercises. MDEP-7 built the upstream test environment: a PostgreSQL source, deterministic REST data, CSV/JSON references, and intentionally bad inputs that later Bronze and Silver processing must handle.”

**中文：**“这是一个商业与运营数据工程实验项目，重点是可重复的批处理、CDC 和流处理练习。MDEP-7 构建了上游测试环境：PostgreSQL、确定性的 REST 数据、CSV/JSON 参考文件以及故意制造的坏数据。”

## B. 60-second Story summary

**English:** “I implemented four related source tables with primary/foreign keys, fixed seed data, and INSERT/UPDATE/DELETE examples for later CDC. I added a small local REST API with pagination, 503 retry, 429 rate-limit, and timeout scenarios. I also included duplicate-file, null, type, semantic JSON, malformed JSON, missing-file, foreign-key, and additive-schema exercises. The REST and fixtures were validated; the Compose-based PostgreSQL validation is documented but remained unexecuted on that host because Docker and psql were unavailable.”

**中文：**“我实现了四个有关联约束的源表、固定种子数据，以及给后续 CDC 使用的增删改示例；还实现了支持分页、503、429 和超时场景的本地 REST API。REST 和文件已验证，但该主机没有 Docker/psql，因此 PostgreSQL 容器验证尚未执行。”

## C. 2-minute technical deep dive / D. architecture explanation

**English:** “The design goal was not to build processing prematurely; it was to make source behavior inspectable and repeatable. PostgreSQL expresses relational truth with keys and constraints. The deliberate FK failure shows a source-side integrity failure. The mutation script supplies the exact changes a later Debezium connector should observe, but I do not claim CDC is running. For external-style reference data, I use a fixed local API so pagination and response failures are deterministic. The file fixtures separate parser failure, semantic contract failure, duplicate content, and missing arrival. In the full architecture, these are raw inputs: Airflow/Spark will handle batch landing and cleaning, while Debezium/Kafka/Flink will apply source changes and events.”

**中文：**“设计目标不是过早实现处理平台，而是让数据源行为可检查、可重复。PostgreSQL 用约束表达关系正确性；变更脚本提供后续 Debezium 应观察到的变化，但不声称 CDC 已运行。本地 API 让分页和失败可控；文件样例区分解析失败、语义失败、重复内容和缺失文件。”

## E. failure/recovery story

**English:** “If I mutate the source or run the additive schema exercise, I recover the lab by running the reset script. It removes the local Compose volume and reseeds the exact baseline. I would verify recovery with row counts, the mutation/FK checks, API paging/failure responses, and fixture checks. That is appropriate only for synthetic lab data; production recovery needs backups, migration discipline, and audited restore procedures.”

**中文：**“如果我修改了源数据或执行了加字段练习，我会运行 reset 脚本恢复实验环境。它删除本地 Compose volume 并重新写入固定种子数据。生产环境不能这样恢复，必须使用备份、迁移流程和审计。”

## F–H. trade-off, learning, production improvement

**English:** “I chose a standard-library API and small deterministic seeds over a public API or framework because the learning target is ingestion behavior, not framework features. I learned to distinguish transport failures from data-quality failures and parser failures from semantic failures. For production, I would add API authentication, secrets, checkpoint persistence, structured logging/metrics, data classification, backup/restore controls, and execute the currently pending PostgreSQL runtime validation.”

**中文：**“我选择标准库 API 和小型确定性种子数据，而不是公共 API 或框架，因为学习重点是摄取行为。生产环境会增加认证、密钥管理、检查点、日志指标、数据分级、备份恢复，并执行当前尚未完成的 PostgreSQL 运行时验证。”
