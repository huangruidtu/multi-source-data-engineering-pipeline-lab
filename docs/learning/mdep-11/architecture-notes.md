# MDEP-11 architecture notes

The implemented path is `KafkaSource -> raw Bronze stream -> Debezium parser -> quarantine side output or keyed state -> Iceberg changelog`. Bronze is append-oriented evidence; Quarantine retains original payload, topic, unavailable transport metadata, reason, and processing time; Silver contains one current row per source key.

CDC freshness is **LSN first**. A higher LSN is accepted and a lower LSN is stale. With the same LSN, two records from the same Debezium `transaction.id` use `transaction.total_order` when both values exist. Only after equal transaction order does known identical topic/partition/offset identify an exact replay. Otherwise the position is ambiguous and does not mutate Silver. Kafka partition numbers have no global database-order meaning.

The job configures 30-second exactly-once checkpoints, a two-minute timeout, five-second minimum pause, three tolerated checkpoint failures, and fixed-delay restart. This configuration is not runtime evidence of end-to-end exactly-once: that depends on observed Kafka source and Iceberg checkpoint commit behavior. Checkpoint state contains source positions Flink can restore and the two keyed states.

Source event time receives a 60-second bounded-out-of-orderness watermark for learning late-data/event-date analysis. State acceptance remains independent of watermarks: a late higher-LSN mutation can be valid while an on-time lower-LSN mutation is stale.

Snapshot `r` records may have no Debezium transaction metadata. The job accepts an initial snapshot record as the first state for its key and stores its source LSN; subsequent CDC uses LSN first. Missing transaction and missing transport metadata never prove a replay, so unresolved equal-LSN positions are safely ignored.
