# MDEP-8 Interview Talking Points

## 30-second / 60-second summary

**English:** “MDEP-8 turns the project’s PostgreSQL, REST, CSV, and JSON sources into a replayable Bronze ingestion design. A compact Airflow DAG orchestrates source-specific tasks. Each logical date gets deterministic, source/entity/date-partitioned Parquet output with audit metadata, and bad inputs are retained in Quarantine rather than silently dropped. The local implementation validated the file path and retry logic; Docker/Airflow service execution remains an explicit follow-up.”

**中文：**“MDEP-8 将 PostgreSQL、REST、CSV 和 JSON 数据源设计成可回放的 Bronze 摄取路径。Airflow DAG 编排各数据源任务；每个逻辑日期生成确定性的 Parquet 路径和审计元数据，坏数据保留在 Quarantine。文件路径和重试逻辑已本地验证，Docker/Airflow 运行验证仍是后续工作。”

## 2-minute deep dive / architecture walkthrough

**English:** “I kept orchestration separate from extraction. The Airflow DAG has three parallel tasks and a final dependency. The task functions create a `BatchContext` from the logical date, which makes the interval, ingestion ID, and target key stable. REST pagination completes in memory before the one Parquet publication, so a failed page does not leave a canonical partial result. File input is intentionally separated per file after I found that mixing CSV and JSON rows could create the wrong Parquet schema. Invalid and missing cases write Quarantine evidence. PostgreSQL extraction supports snapshot and an `updated_at` interval exercise, but it only lands Bronze because CDC later owns current-state Silver.”

**中文：**“我把编排和抽取分离。DAG 有三个并行任务和最终依赖。任务从 logical date 创建稳定的 interval、ingestion ID 和目标路径。REST 在所有分页完成后才发布 Parquet，因此中途失败不会产生部分正式结果。CSV 和 JSON 按文件分开，避免 schema 混合。”

## Idempotency, retry/backfill, and failure story

**English:** “The idempotency boundary is a deterministic object key plus create-if-absent and a manifest. A retry or same-date rerun sees the existing object and returns `already_published`; it does not append another canonical dataset. A 429 is retried with `Retry-After`, while a malformed file is not retried as transport failure—it is retained with a reason. Backfill is simply running old logical dates, producing their corresponding partitions.”

**中文：**“幂等边界是确定性对象路径、只创建一次和 manifest。同日期重跑会返回 `already_published`。429 根据 `Retry-After` 重试；格式错误文件不当作网络重试，而是保留原因。回填就是运行历史 logical date。”

## Batch versus CDC / learning / production improvements

**English:** “Batch PostgreSQL output is evidence and recovery material in Bronze, not the Silver current-state owner. I learned that source alignment includes schema boundaries: valid CSV and JSON must not be forced into one inferred schema. In production I would add durable REST checkpoints, Airflow connections/secrets, schema contracts, S3 encryption and lifecycle rules, race-safe manifests, observability, and execute the currently unvalidated Airflow/PostgreSQL/S3 scenarios.”

**中文：**“PostgreSQL 批处理只在 Bronze 中保存证据和恢复材料，不拥有 Silver 当前状态。我学到 source-aligned 也包括 schema 边界。生产环境会增加持久检查点、密钥、契约、S3 加密生命周期、并发保护、可观测性，并完成未验证的运行场景。”

**English:** “For review, I added one Docker-based validation command rather than asking someone to reconstruct the runtime manually. I am clear that it is ready to run, but it was not executed on this Docker-less host.”

**中文：**“为了方便评审，我增加了一个 Docker 验证命令，而不是让开发者手动重建环境。我会明确说明：它已经可运行，但本机没有 Docker，因此没有执行。”
