# Module 00 — Interview talking points

**30 seconds:** “This is a Commerce & Operations pipeline with separate batch and CDC paths. I preserve replayable Bronze, use one canonical writer per Silver dataset, and model Gold in dbt.”  
中文：强调“职责边界”和“可回放”，不要说已经生产运行。

**60 seconds:** Add that Airflow coordinates bounded landing, Spark handles batch references, and Debezium/Kafka/Flink applies keyed CDC state to Iceberg; Snowflake/dbt produces Gold. Say the MDEP-9 stale-replay bug made version order explicit.  
中文：先直接回答，再用一个具体 bug 证明你理解正确性。

**Failure/trade-off story:** “A hash difference was initially treated as freshness. That could let an older replay overwrite newer state. I changed the rule to business timestamp, extraction evidence, ingestion evidence, then hash only as tie-breaker.”  
中文：hash 只识别内容，不代表新旧。

**Production improvement:** “I would run the documented validation matrix, store evidence, then add scoped alerting for lag, WAL retention, checkpoints, quality and warehouse cost.”  
中文：说明这是改进建议，不要假装已完成。
