# MDEP-10 interview talking points

**30 seconds (English):** “I implemented a small WAL-based CDC transport path. PostgreSQL logical replication feeds Debezium in Kafka Connect, which produces one keyed Kafka topic per commerce table. It starts with a snapshot then streams inserts, updates, and deletes. I stopped at Kafka deliberately; Flink later owns current-state Silver.”

**中文：**“我实现了一个小型 WAL CDC 传输链路：PostgreSQL 逻辑复制到 Debezium/Kafka Connect，再按表写入带主键的 Kafka topic。先快照后持续捕获变更；Silver 当前状态留给后续 Flink。”

**Technical deep dive:** “The publication selects four tables and the replication slot preserves Debezium's LSN position. Topic names are `mdep.commerce.<table>`. Primary-key routing gives per-key partition order, never global order. The envelope carries before/after/source/op. Deletes have `after=null` and a tombstone is enabled. Restarts use Connect offsets, but I treat the system as at least once, so downstream state application must be idempotent.”

**中文：**“publication 选择表，slot 保留 LSN 进度；主键保证同 key 的分区顺序，但没有全局顺序。重启依赖 Connect offset，仍按至少一次处理，下游必须幂等。”

**Failure/schema/polling:** “A stopped connector can retain WAL through its slot, so I would monitor slot lag and disk. The exercise adds nullable `preferred_language`; additive fields must be propagated deliberately. Compared with polling, WAL CDC preserves insert/update/delete semantics and avoids scanning, but adds operational responsibility.”
