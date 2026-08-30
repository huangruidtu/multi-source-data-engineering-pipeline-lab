# Module 05 — Interview talking points

**60 seconds:** “Flink consumes Debezium events, archives raw evidence, quarantines malformed input, then keys by entity and primary key. It stores the last applied version and current row, so replay, stale events and deletes have explicit behavior.” 中文：按 key 管理状态，不是按整个 topic。

**Ordering answer:** “LSN is first. Same LSN can need Debezium transaction order. Kafka offset proves only transport identity when topic/partition/offset are known; a partition number is not freshness.” 中文：这是高级面试关键点。

**Honesty:** “The topology and model tests are implemented; checkpoint and Iceberg sink behavior are runtime unvalidated.” 中文：不要说 exactly-once 已验证。
