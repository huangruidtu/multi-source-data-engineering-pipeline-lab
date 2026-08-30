# Module 04 — Interview talking points

**30 seconds:** “PostgreSQL WAL flows through a publication and replication slot to Debezium, then to keyed Kafka topics. Kafka provides retained, at-least-once transport; Flink decides current state.” 中文：Kafka 不是数据库顺序的唯一真相。

**Bug story:** “A review caught `include.transaction`; Debezium PostgreSQL 3.0 requires `provide.transaction.metadata`. I corrected the actual connector JSON and test. It is configured, but I do not claim runtime observation.” 中文：区分配置、预期和已验证。

**Production answer:** “I would monitor slot retention, connector health, topic lag and schema compatibility.” 中文：不要说当前已有完整监控。
