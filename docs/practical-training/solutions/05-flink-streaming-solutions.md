# Solutions — 05 Flink streaming
## FS-01
Raw branch archives unparsed evidence; parser routes failure to quarantine; no current-state mutation occurs. This separates evidence from accepted state.
## FS-02
Bare `1001` collides across `customers`, `products`, and `orders`. Use `key_identity(event)` → `entity:primary_key`; Flink then isolates ValueState per business entity/key.
## FS-03
Exactly-once checkpoint configuration is implemented, and pure ordering is offline tested. Kafka/Flink/checkpoint/Iceberg end-to-end delivery proof is runtime deferred; configuration alone is not proof.
