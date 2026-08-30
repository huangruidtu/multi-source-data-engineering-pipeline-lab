# MDEP-11 interview talking points

**30 seconds:** “I built the CDC state-application boundary: Flink reads Debezium records, archives original evidence to Bronze, and is the only canonical writer for PostgreSQL Silver current state. It uses managed LSN-first state keyed by entity plus primary key to ignore duplicates and stale replays.”

**中文：**“我实现了 CDC 状态应用边界：Flink 读取 Debezium 事件，保留 Bronze 原始证据，并作为 PostgreSQL Silver 当前状态的唯一写入者。它按照实体加主键使用托管的 LSN 优先状态，来忽略重复和过期重放。”

**Deep dive:** “A checkpoint captures Kafka source progress and keyed last-applied versions; a savepoint is for deliberate migration. LSN is database ordering. Kafka partition/offset can identify a replay, but a higher partition number is never newer source state. Event time/watermarks help late-event analysis, not CDC state ordering. A delete physically removes current state only if newer; its following tombstone is ignored for state.”

**Production:** “I would validate checkpoint-aligned Iceberg commits, connector jars, recovery and rescaling, monitor backpressure/lag/slot retention, and retain source positions for audit. Those runtime observations are still unvalidated in this lab.”
