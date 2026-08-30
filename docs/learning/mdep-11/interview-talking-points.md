# MDEP-11 interview talking points

**30 seconds:** “I built the CDC state-application boundary: Flink reads keyed Debezium records, archives the original events to Bronze, and is the only canonical writer for PostgreSQL Silver current state. It uses LSN-first per-key state to ignore duplicates and stale replays.”

**中文：**“我实现了 CDC 状态应用边界：Flink 读取带主键的 Debezium 事件，保留 Bronze 原始证据，并作为 PostgreSQL Silver 当前状态的唯一写入者。它按每个 key 使用 LSN 优先的状态来忽略重复和过期重放。”

**Deep dive:** “A checkpoint captures Kafka offsets and keyed last-applied versions; a savepoint is for deliberate migration. Kafka offset is transport position, but LSN is database ordering. Event time/watermarks help late-event analysis, not CDC state ordering. A delete event physically removes current state only if newer; its following tombstone is ignored for state.”

**Production:** “I would validate checkpoint-aligned Iceberg commits, connector jars, recovery and rescaling, monitor backpressure/lag/slot retention, and retain source positions for audit. Those runtime observations are still unvalidated in this lab.”
