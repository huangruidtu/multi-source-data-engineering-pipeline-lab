# MDEP-9 interview talking points

## A. 30-second summary

**English:** “I built the batch Bronze-to-Silver path for two reference datasets. Spark reads the replayable Parquet produced by the ingestion story, validates and deduplicates it, broadcasts a small country reference to enrich locations, preserves rejected rows in Quarantine, and merges valid rows into HadoopCatalog-managed Iceberg tables. I deliberately left PostgreSQL current state to the later CDC/Flink path.”

**中文：**“我实现了两个参考数据集从 Bronze 到 Silver 的批处理路径。Spark 读取可回放的 Parquet，做校验、去重和位置国家参考数据的广播关联；坏数据进入 Quarantine，正确数据 merge 到 HadoopCatalog 管理的 Iceberg 表。PostgreSQL 当前状态仍由后续 CDC/Flink 负责。”

## B. 60-second Bronze-to-Silver explanation

**English:** “Bronze is evidence, so MDEP-8 keeps source payloads and landing metadata even if they are imperfect. In MDEP-9, Silver takes responsibility for technical trust: I parse types, require keys, reject non-positive FX rates and unknown location countries, then select one deterministic winner per business key. A rejected or duplicate loser is not dropped; it keeps its original payload, source locator, ingestion identity, and reason. The merge key makes a rerun idempotent.”

**中文：**“Bronze 保留原始证据；Silver 才负责技术可信性。我解析类型、检查主键和业务规则，并为每个业务键选择可预测的唯一赢家。无效或重复行不删除，而是保留其来源信息和拒绝原因。Merge key 让重跑幂等。”

## C. Two-minute technical deep dive

**English:** “The job is intentionally small. It configures Iceberg SparkCatalog as HadoopCatalog with the approved S3-backed warehouse. It only exposes `ref_exchange_rates` and `ref_locations`; code rejects CDC-owned entity names. Exchange rates use date/base/quote as a composite key and are ordered by retrieved timestamp, extract timestamp, ingestion time, and record hash. Locations use location ID and the same deterministic pattern with updated time. The location country map is broadcast, which both enriches region and catches unknown countries. Valid rows use SQL MERGE; Iceberg commits a snapshot, so readers get a consistent table state. I also included an additive nullable column exercise and formatted plan/skew exercises, because I want to be able to explain the execution plan rather than treat Spark as a black box.”

**中文：**“实现刻意保持小而清晰：只写两个批处理拥有的参考表，用业务键和时间戳加 hash 确定去重赢家；国家参考表广播关联既用于 enrichment 也用于完整性检查。Iceberg MERGE 产生一致快照，并提供添加 nullable 字段和执行计划/skew 的练习。”

## D–L. Focused answers

**Spark execution / shuffle:** “DataFrame transformations are lazy; dedup windows and group-bys are wide transformations and shuffle by key. I inspect the formatted plan and partition counts before optimizing.” **中文：**“窗口和聚合会 shuffle；先看计划和任务指标再优化。”

**Dedup/idempotency:** “I first deduplicate inside the incoming batch, then apply the identical version ordering against the existing Silver row. Business time wins first, extraction and ingestion evidence break ties, and hash is only the last tie-breaker—not a freshness signal. That means an old replay with different content cannot regress Silver, while an exact replay is a no-op.” **中文：**“我先在输入批次内去重，再用相同的版本顺序与现有 Silver 比较。业务时间优先，抽取和入库时间用于平局，hash 只是最后的平局规则，不代表更新更晚；旧数据即使内容不同也不能覆盖新状态，完全重放则不操作。”

**Iceberg/schema evolution:** “Iceberg adds snapshots and manifest metadata beyond Parquet files. I demonstrate only an additive nullable field; destructive changes require a contract migration.” **中文：**“Iceberg 提供快照和 manifest；只演示向后兼容的新增字段。”

**Failure/quarantine:** “Bad rows are evidence. I preserve payload and lineage with a rule-specific reason, so an operator can diagnose and replay rather than guess what was dropped.” **中文：**“坏数据也要保留证据，才能诊断和回放。”

**Spark versus Flink / improvements:** “Spark owns bounded batch reference data; Flink will own unbounded CDC state and event-time behavior. At production volume I would measure skew and merge cost, compact small files, persist checkpoints, monitor quality/freshness/snapshots, and run the currently unavailable runtime acceptance checks.” **中文：**“Spark 处理有界批数据，Flink 处理 CDC 和事件时间；生产上要补齐指标、压缩、小文件、检查点和运行验证。”
