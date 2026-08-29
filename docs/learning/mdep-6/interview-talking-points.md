# MDEP-6 Interview Talking Points

## A. 30-second project summary

**English:** “This project is a Commerce & Operations data-engineering lab. It uses multiple source types and separates raw Bronze evidence, trusted Silver datasets, and dbt/Snowflake Gold models. My first implemented Story established contracts and ownership so later batch, CDC, and streaming paths do not create conflicting datasets.”

**中文：**“这是一个商业与运营数据工程实验项目，包含多种数据源，并区分 Bronze 原始层、Silver 可信层和 Snowflake/dbt 的 Gold 层。第一个已实现 Story 先定义数据契约和所有权，避免后续批处理、CDC 和流处理产生冲突。”

## B. 60-second Story summary

**English:** “For MDEP-6, I documented entity grain, keys, quality rules, batch metadata, Kafka event metadata, Debezium mutation semantics, and quarantine requirements. The important decision is one canonical Silver writer per logical dataset: batch snapshots preserve PostgreSQL evidence in Bronze, while the later Flink CDC path owns current state. I also recorded the planned S3-backed HadoopCatalog decision for shared Iceberg metadata.”

**中文：**“MDEP-6 记录了实体粒度、主键、质量规则、批处理和 Kafka 元数据、Debezium 变更语义及隔离区要求。关键决策是每个 Silver 数据集只有一个权威写入者：批处理只保留 Bronze 快照，后续 Flink CDC 负责当前状态。”

## C. 2-minute technical deep dive / D. architecture explanation

**English:** “I start from dataset ownership, not tools. `core_orders` is a current-state dataset keyed by `order_id`, so I assign it to CDC/Flink. A periodic PostgreSQL extract can still be useful, but only as a replayable Bronze snapshot. Business events are different: they have immutable `event_id`s and are stored one row per event; `aggregate_id` gives per-entity Kafka ordering, not global order. Invalid or duplicate input remains traceable: Bronze captures the payload, Silver validates it, and Quarantine retains the rejection reason and source locator. This gives later Spark/Flink jobs a consistent target without pretending those jobs exist already.”

**中文：**“我从数据集所有权而不是工具开始。`core_orders` 是按 `order_id` 的当前状态数据集，因此由 CDC/Flink 负责。定期 PostgreSQL 抽取仍有价值，但只作为可回放的 Bronze 快照。业务事件不同：每条事件有不可变的 `event_id`，`aggregate_id` 只保证同一实体的顺序，不保证全局顺序。”

## E. failure/recovery story

**English:** “A key failure I designed against is dual writing. If batch and CDC both upsert orders, an older snapshot can overwrite a newer CDC update. The recovery is architectural: keep the batch path Bronze-only, use source positions for CDC idempotency, and run any reconciliation as an explicit controlled process.”

**中文：**“我重点防止的失败是双写。若批处理和 CDC 都更新订单，旧快照可能覆盖新的 CDC 更新。解决方式是让批处理只写 Bronze，并将对账作为受控流程。”

## F–H. trade-off, learning, production improvement

**English:** “I chose HadoopCatalog to avoid adding Glue or a REST catalog in a learning V1, trading managed catalog features for simplicity. I learned that grain and ownership are practical reliability controls, not documentation polish. For production, I would add automated contract compatibility checks, data classification, access controls, metrics, and an explicit snapshot-to-CDC reconciliation runbook.”

**中文：**“我选择 HadoopCatalog 来避免在 V1 引入 Glue 或 REST catalog，代价是较少的托管能力。我学到粒度和所有权是可靠性控制，而不只是文档。生产环境会增加契约兼容性检查、数据分级、权限、指标和 CDC 对账流程。”
