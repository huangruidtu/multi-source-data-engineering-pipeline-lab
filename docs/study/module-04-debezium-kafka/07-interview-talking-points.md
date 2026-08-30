# Module 04 — Interview talking points

**30 seconds:** “PostgreSQL WAL flows through a publication and replication slot to Debezium, then to keyed Kafka topics. Kafka provides retained, at-least-once transport; Flink decides current state.” 中文：Kafka 不是数据库顺序的唯一真相。

**Bug story:** “A review caught `include.transaction`; Debezium PostgreSQL 3.0 requires `provide.transaction.metadata`. I corrected the actual connector JSON and test. It is configured, but I do not claim runtime observation.” 中文：区分配置、预期和已验证。

**Production answer:** “I would monitor slot retention, connector health, topic lag and schema compatibility.” 中文：不要说当前已有完整监控。

## 2-minute architecture answer

“For the CDC path, PostgreSQL remains the source of truth. I enable logical WAL, create a publication for exactly customers, products, orders and payments, and give Debezium a replication slot. Debezium performs an initial snapshot and then streams `r`, `c`, `u`, and `d` envelopes into `mdep.commerce.*` topics. The primary key is the Kafka key, so a downstream stateful consumer gets per-key partition ordering. Kafka is transport, not a global database-order oracle: the next Flink layer uses source LSN first and transaction order only when equal LSN needs it. The configuration and contract tests exist; I would not claim connector/topic events were observed because that Docker runtime is blocked.”

中文提示：先讲 source of truth 和边界，再讲顺序。最后主动说明 runtime 未验证，会比模糊地说“跑过 CDC”更可信。

## Debugging answer: transaction metadata correction

“During review I found the connector used `include.transaction`, but Debezium PostgreSQL 3.0 expects `provide.transaction.metadata=true`. I corrected the actual connector JSON and added a test that parses that JSON rather than testing a duplicated constant. The result is configuration assurance, not an assertion that transaction boundary events were already seen.”

中文提示：重点是“真实配置文件 + regression test + 不夸大观测结果”。不要把 transaction metadata 说成 Kafka 全局排序。

## Incident/recovery answer

“If a connector falls behind, I first inspect connector/task status, current WAL position, slot confirmed flush position, retained WAL and broker lag. I do not immediately drop the slot: that can force a resnapshot or create a recovery decision. After identifying the fault, I resume or deliberately rebootstrap, then use replay-safe downstream rules and capture evidence.”

中文提示：体现生产排障顺序：检测、影响、恢复、验证。不要只说“restart connector”。

## Trade-off and production answer

“CDC was chosen because the project needs source mutations and deletes, not just periodic table state. It costs source privileges and operational care. The current one-node KRaft/RF=1 topology is intentional lab scope; production would evaluate multi-broker replication, ISR/acks, capacity, Connect worker redundancy, slot/WAL alerts, schema compatibility and least-privilege credentials.”

中文提示：将 lab 选择与 production recommendation 分开。不要声称已部署 HA、schema registry 或监控。

## What I learned / what I would change

“I learned that connector configuration names and ordering domains matter: source LSN, Debezium transaction order and Kafka offsets answer different questions. Next I would run the supplied CRUD/restart/schema-evolution exercise in Docker, retain run evidence, and only then make runtime acceptance claims.”

中文提示：结尾要落到可执行的下一步和证据，而不是空泛地说‘improve reliability’。

## 30-second spoken summary

“MDEP-10 captures four PostgreSQL commerce tables through logical decoding. I configured a publication, replication slot and Debezium PostgreSQL connector, which publishes keyed `mdep.commerce.*` change events to Kafka. It is deliberately transport-only: Flink owns downstream current state. I statically tested the connector contract, but I do not claim the Docker CDC runtime was observed.”

中文提示：用四句完成。强调四张表、source-to-Kafka 边界、Flink ownership、runtime honesty；不要展开所有配置项。

## 60-second spoken summary

“The source is PostgreSQL OLTP state. Logical WAL and `pgoutput` let Debezium read committed row changes for customers, products, orders and payments. The replication slot lets it resume but also creates WAL-retention risk if capture stalls. Debezium emits snapshot and streaming envelopes to table-specific Kafka topics, keyed by the source primary key. Kafka gives per-partition ordering and replay, not global database ordering. That is why MDEP-11 later uses source LSN and transaction metadata for current-state decisions. The repository configures transaction metadata correctly with `provide.transaction.metadata=true`, but I would describe it as configured—not observed.”

中文提示：这里适合展示你知道 slot 的双面性：恢复能力 + WAL 风险。说完 ordering 后停，等面试官追问。

## How it connects to Flink

“Debezium is not the current-state engine in this architecture. It turns source commits into transport records. Flink consumes those records, preserves raw Bronze evidence, and applies a keyed LSN-first state rule before writing CDC Silver. Keeping capture and state application separate makes the ownership boundary clear and makes replay semantics testable.”

中文提示：不要把 MDEP-10 说成已经写 Iceberg；那是 MDEP-11 的职责。

## What is still unvalidated?

“The connector JSON, source prerequisites and pure contract tests are present. What remains unvalidated is physical Docker startup, connector registration, initial snapshot, actual CRUD envelopes, tombstones, transaction boundary events, Connect restart and topic replay. I have an explicit validator for those exercises and would retain run-specific evidence before changing the status.”

中文提示：这是加分答案。主动承认范围，不要用 ‘should work’ 代替证据。
