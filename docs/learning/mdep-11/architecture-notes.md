# MDEP-11 architecture notes

The implemented path is `KafkaSource -> raw Bronze stream -> Debezium parser -> quarantine side output or keyed state -> Iceberg changelog`. Bronze is append-oriented evidence; Quarantine retains original payload, topic, unavailable transport metadata, reason, and processing time; Silver contains one current row per source key.

CDC freshness is **LSN first**. A higher LSN is accepted and a lower LSN is stale. With the same LSN, the identical topic/partition/offset is an exact replay; a different transport coordinate is not newer because Kafka partition numbers have no global database-order meaning. Normal Kafka keying normally keeps one source key on one partition, but correctness does not depend on numeric partition ordering.

The job configures 30-second exactly-once checkpoints, a two-minute timeout, five-second minimum pause, three tolerated checkpoint failures, and fixed-delay restart. This configuration is not runtime evidence of end-to-end exactly-once: that depends on observed Kafka source and Iceberg checkpoint commit behavior. Checkpoint state contains source positions Flink can restore and the two keyed states.

Source event time receives a 60-second bounded-out-of-orderness watermark for learning late-data/event-date analysis. State acceptance remains independent of watermarks: a late higher-LSN mutation can be valid while an on-time lower-LSN mutation is stale.
