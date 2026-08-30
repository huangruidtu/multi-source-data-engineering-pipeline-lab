# Final V1 Interview Q&A

Every answer below is bounded by the final V1 truth model: code and offline
evidence exist; full infrastructure integration is deferred. The last sentence
of each answer is a natural spoken-English version.

## Architecture

### 1. What problem does this project solve?
**Direct answer:** It demonstrates one coherent commerce pipeline with batch,
CDC, lakehouse, and warehouse paths without duplicating writers. **MDEP example:**
PostgreSQL, REST, and files land in Bronze; Spark and Flink own separate Silver
domains; dbt owns Gold. **Why/trade-off:** ownership reduces races but makes
boundaries explicit. **Senior follow-up:** how would you govern cross-domain
contracts? **Spoken:** “I designed it around clear data ownership, not around
showing as many tools as possible.”

### 2. Why both batch and CDC?
**Direct answer:** Batch suits reference ingestion and bounded backfills; CDC
captures change history/current state for commerce tables. **Example:** locations
are Spark batch Silver, orders are Flink CDC Silver. **Trade-off:** two paths need
common reconciliation. **Follow-up:** when would you consolidate them? **Spoken:**
“They solve different latency and source-semantics problems, so I made the split
explicit.”

### 3. Who owns Bronze, Silver, and Gold?
**Direct answer:** Airflow/batch owns batch Bronze, Spark owns batch Silver,
Flink owns CDC Bronze/current-state Silver, and dbt owns Gold. **Why:** a single
canonical Silver writer prevents conflicting states. **Limitation:** ownership is
documented rather than enforced by a platform ACL. **Follow-up:** how would you
enforce it? **Spoken:** “For every dataset, I can name one canonical writer.”

### 4. What does V1 prove?
**Direct answer:** It proves implementation, configuration, offline correctness
rules, documentation, and failure reasoning. **Example:** the Silver version
rule has unit tests. **Limitation:** it does not prove a live multi-service run.
**Follow-up:** what is V1.x? **Spoken:** “The code and contracts are reviewable;
the physical integration lab is deliberately deferred.”

### 5. Why use Iceberg?
**Direct answer:** Iceberg supplies table metadata and snapshot-oriented Silver
tables for both batch and streaming designs. **Example:** Spark and Flink use a
Hadoop-style catalog concept. **Trade-off:** operational catalog/object-store
validation remains deferred. **Follow-up:** how would you manage compaction?
**Spoken:** “Iceberg is the shared table boundary, not another processing engine.”

### 6. How do you avoid duplicate platforms?
**Direct answer:** Every approved tool has one role: Airflow orchestrates, Spark
batch-transforms, Debezium captures, Kafka transports, Flink projects CDC, dbt
models Gold. **Trade-off:** fewer alternatives means fewer comparison exercises.
**Follow-up:** why not Databricks? **Spoken:** “I optimized for explainable
concept coverage rather than tool sprawl.”

## Batch and Airflow

### 7. What does Airflow do here?
**Direct answer:** It schedules and retries bounded Bronze ingestion; it does not
own business transformation. **Example:** `bronze_ingestion.py` calls batch code.
**Trade-off:** DAG orchestration cannot replace data-quality logic. **Follow-up:**
how do you backfill? **Spoken:** “Airflow coordinates work; the pipeline code
owns the data semantics.”

### 8. How is batch idempotency designed?
**Direct answer:** Inputs and outputs use deterministic identity and paths so a
repeat can be recognised rather than blindly duplicated. **Example:** file
identity is content based. **Limitation:** concurrent object-store behavior is
not runtime-tested. **Follow-up:** how would you make writes atomic? **Spoken:**
“A rerun should converge on the same logical Bronze result.”

### 9. Why preserve quarantine records?
**Direct answer:** Rejected data must remain evidence, including original context
and a reason. **Example:** invalid CSV/JSON fixtures exercise this contract.
**Trade-off:** storage and handling complexity increase. **Follow-up:** who fixes
the record? **Spoken:** “Quarantine is an observable decision, not a trash can.”

### 10. How does REST ingestion handle rate limits?
**Direct answer:** The extractor has pagination and retry behavior including a
429 case. **Example:** `test_pagination_retries_a_429_then_returns_all_pages`.
**Limitation:** it is a controlled offline API behavior. **Follow-up:** add jitter
or a circuit breaker? **Spoken:** “I test the retry contract without pretending
I have production API telemetry.”

### 11. What is Bronze for?
**Direct answer:** Bronze is immutable, replayable source-aligned landing data.
**Example:** batch Parquet and raw CDC archive are separated from Silver state.
**Trade-off:** replay requires version-safe downstream logic. **Follow-up:** how
long retain it? **Spoken:** “Bronze keeps evidence so transformations can be
reproduced and corrected.”

### 12. What runtime work is deferred for Airflow?
**Direct answer:** Container startup, DAG execution, backfill, and physical
Parquet inspection. **Example:** the DAG and validator exist in the repository.
**Why disclose it:** configuration is not execution evidence. **Follow-up:** what
would you record? **Spoken:** “Airflow is implemented and statically checked, not
claimed as run.”

## Spark and Iceberg

### 13. How do you prevent an old replay overwriting Silver?
**Direct answer:** Compare a complete lexicographic version tuple before update.
**Example:** locations compare `updated_at`, extract time, ingestion time, hash.
**Why:** a changed payload hash cannot make an older business version fresh.
**Follow-up:** what is a no-op? **Spoken:** “An old replay loses even if it has a
different hash.”

### 14. Why is `record_hash` only a tie-breaker?
**Direct answer:** A hash describes payload difference, not temporal freshness.
**Example:** `incoming_is_newer` puts it last. **Trade-off:** equal timestamps
still require deterministic choice. **Follow-up:** could hashes collide? **Spoken:**
“Hash makes ties deterministic; it never overrules a newer business timestamp.”

### 15. What was the stale-update bug?
**Direct answer:** The old merge allowed update when hashes differed, which could
regress current state. **Fix:** testable full-tuple ordering. **Why it matters:**
Bronze replays are normal. **Follow-up:** how would you test physical MERGE?
**Spoken:** “I found that a payload difference is not a version signal.”

### 16. How does Spark ownership stay safe?
**Direct answer:** Its contracts allow only `exchange_rates` and `locations`.
**Example:** CDC entities raise an ownership error. **Trade-off:** a new dataset
needs an explicit ownership decision. **Follow-up:** store this in a registry?
**Spoken:** “The batch job cannot quietly become a second writer for orders.”

### 17. How is schema evolution handled?
**Direct answer:** Contracts and table definitions are designed for controlled,
additive change; tests cover declared expectations. **Limitation:** an actual
Iceberg evolution run is deferred. **Follow-up:** how handle renames? **Spoken:**
“I separate schema-design evidence from a runtime migration claim.”

### 18. What does the incremental boundary mean?
**Direct answer:** It is start-inclusive and end-exclusive to prevent overlap
ambiguity. **Example:** tested in `test_silver_contracts.py`. **Trade-off:** late
data still needs version-aware replay. **Follow-up:** watermark vs batch window?
**Spoken:** “The boundary is deterministic, but freshness still comes from the
version tuple.”

## Debezium and Kafka

### 19. How is PostgreSQL CDC enabled?
**Direct answer:** PostgreSQL publishes the four commerce tables and Debezium
uses pgoutput, a named slot, and an explicit publication. **Evidence:**
`cdc-init.sql` and connector JSON. **Limitation:** no live slot inspection.
**Follow-up:** WAL retention risk? **Spoken:** “The prerequisites are configured
and reviewed; runtime replication is a V1.x exercise.”

### 20. How do you enable transaction metadata?
**Direct answer:** With `provide.transaction.metadata=true`, not
`include.transaction`. **Example:** the actual JSON is read by contract tests.
**Limitation:** transaction boundary events were not observed live. **Follow-up:**
ordering across partitions? **Spoken:** “It is configured for Debezium 3.0, but I
do not claim I observed BEGIN or END events.”

### 21. What do Debezium operations mean?
**Direct answer:** `r`, `c`, `u`, and `d` represent snapshot read, create,
update, and delete. **Example:** deletes require `after=null`. **Trade-off:** a
tombstone is a separate Kafka record. **Follow-up:** compaction behavior?
**Spoken:** “I validate envelope semantics before applying current state.”

### 22. Why Kafka keys matter?
**Direct answer:** Keys preserve per-key partition affinity and identify the
source primary key. **Example:** CDC contract maps each approved table's key.
**Limitation:** no physical topic partition check occurred. **Follow-up:** key
change behavior? **Spoken:** “The key is the identity contract, not just a routing
detail.”

### 23. Is Kafka offset source freshness?
**Direct answer:** No; it is a transport position within one partition.
**Example:** the Flink model never compares partition numbers as freshness.
**Trade-off:** source ordering evidence must be retained. **Follow-up:** what is
globally ordered? **Spoken:** “Kafka offset tells me delivery position, while LSN
tells me source progress.”

### 24. How do tombstones behave?
**Direct answer:** Debezium delete envelope and optional Kafka tombstone are
different messages; the model treats a null value tombstone as non-mutating.
**Why:** deletion state is decided from the delete envelope. **Follow-up:** topic
compaction? **Spoken:** “A tombstone is transport cleanup evidence, not a second
business delete.”

## Flink and CDC state

### 25. What is the Flink ordering rule?
**Direct answer:** Per entity/key: source LSN, then same-transaction total order,
then known transport identity for exact replay. **Example:** `version_decision`.
**Trade-off:** unresolved equal position is rejected. **Follow-up:** cross-table
transaction ordering? **Spoken:** “I use source evidence first and stay
conservative when the evidence is incomplete.”

### 26. Why equal-LSN transaction ordering?
**Direct answer:** Multiple changes in one PostgreSQL transaction can share an
LSN; Debezium total order distinguishes them. **Limitation:** only when the same
transaction ID and order fields are available. **Follow-up:** collection order?
**Spoken:** “LSN alone is insufficient inside a multi-row transaction.”

### 27. What is an equal-position conflict?
**Direct answer:** Equal LSN without a newer transaction order or provable same
transport identity is not safely applied. **Why:** guessing can corrupt state.
**Trade-off:** availability yields to correctness. **Follow-up:** alerting path?
**Spoken:** “When I cannot prove freshness, I quarantine/ignore rather than
overwrite.”

### 28. How are exact replays handled?
**Direct answer:** Same known topic, partition, and offset is an exact replay and
does not mutate state. **Limitation:** current PyFlink input lacks that metadata
at runtime. **Follow-up:** custom deserializer? **Spoken:** “The model supports
idempotency evidence, but the runtime metadata limitation is explicit.”

### 29. How are deletes applied?
**Direct answer:** A newer delete clears keyed current state and emits a changelog
delete if a prior row exists. **Example:** `CdcStateApplier`. **Trade-off:** raw
Bronze retains the event. **Follow-up:** soft delete? **Spoken:** “I preserve the
event but remove the current projection.”

### 30. Why retain raw CDC Bronze?
**Direct answer:** It creates an immutable evidence/replay layer independent of
the current-state projection. **Example:** malformed messages are retained as
unparsed input. **Trade-off:** more storage. **Follow-up:** retention policy?
**Spoken:** “The archive lets me explain what arrived even if Silver rejects it.”

### 31. What is the watermark used for?
**Direct answer:** It supplies event-time context for stream processing but does
not decide CDC state acceptance. **Why:** source LSN/order is stronger evidence.
**Follow-up:** windows? **Spoken:** “Watermarks are for time semantics; LSN is for
current-state correctness.”

### 32. Are checkpoints validated?
**Direct answer:** They are configured as exactly-once with timeout, pause, and
restart strategy. **Limitation:** recovery has not run physically. **Follow-up:**
what validates exactly-once end-to-end? **Spoken:** “Configured is not validated;
I keep that distinction explicit.”

## Snowflake and dbt

### 33. Why is Snowflake not a Silver writer?
**Direct answer:** It externally reads Iceberg Silver and dbt writes Gold. **Why:**
one writer per Silver dataset avoids split ownership. **Follow-up:** metadata
refresh? **Spoken:** “Snowflake is the analytical boundary, not another ingestion
path.”

### 34. What was the `GOLD_GOLD` correction?
**Direct answer:** A historical schema naming error was corrected to `MDEP.GOLD`.
**Why it matters:** static DDL naming mistakes break deployment. **Follow-up:**
how prevent it? **Spoken:** “I added a contract test because tiny DDL errors are
still architecture errors.”

### 35. What is the Gold dimensional model?
**Direct answer:** dbt creates declared-grain dimensions, facts, and marts over
six Silver sources. **Example:** customers/products/locations/date dimensions and
orders/payments facts. **Follow-up:** surrogate keys? **Spoken:** “Gold is where
business-consumable dimensional semantics live.”

### 36. Why does `fct_payments` rebuild fully?
**Direct answer:** It correctly reflects source deletes and upstream order
relinks without a complex incremental merge. **Trade-off:** it costs more at
scale. **Follow-up:** future incremental design? **Spoken:** “For bounded V1, I
chose correctness and explainability over an unproven optimization.”

### 37. What dbt validation exists?
**Direct answer:** Sources, schema declarations, model tests, and a custom
positive-amount test exist. **Limitation:** dbt itself was not executed against
Snowflake. **Follow-up:** freshness SLO? **Spoken:** “The dbt contract is in the
repo; live warehouse proof is deferred.”

## Reliability and validation

### 38. How do you reconcile layers?
**Direct answer:** Use bounded-run counts, anti-joins, duplicate/null checks, and
an exception record across source, Silver, and Gold. **Evidence:** reconciliation
SQL templates. **Limitation:** no live counts are asserted. **Follow-up:** metric
tolerances? **Spoken:** “I provide the reconciliation method without inventing
numbers I did not run.”

### 39. What was the validation-runner false-PASS fix?
**Direct answer:** A stage now passes only with exit code zero and an evidence
log. **Example:** runner self-tests a native failure and a PowerShell throw.
**Follow-up:** durable evidence storage? **Spoken:** “A command failure cannot be
silently reclassified as a pass.”

### 40. What failures are modeled?
**Direct answer:** Duplicate inputs, bad types, missing files, retry behavior,
stale CDC events, deletes, conflicts, and reconciliation exceptions. **Evidence:**
`validation/failure-scenarios.yml`. **Follow-up:** alert priorities? **Spoken:**
“I designed failure handling as part of the data contract.”

### 41. What is your main data-quality boundary?
**Direct answer:** Validate before trusted Silver; route invalid records with
reasons and reconcile results later. **Trade-off:** strict validation can delay
records. **Follow-up:** severity tiers? **Spoken:** “I prefer an explainable
quarantine to silently contaminating trusted state.”

### 42. How would you monitor it in production?
**Direct answer:** Monitor volume, lag, quarantine rate, stale/conflict rate,
checkpoint health, Iceberg commits, dbt tests, and reconciliation deltas.
**Limitation:** monitoring is designed, not deployed. **Follow-up:** SLOs?
**Spoken:** “My V1 gives the signals and failure modes; production wiring is next.”

### 43. What is the biggest known implementation limitation?
**Direct answer:** PyFlink's value-only Kafka deserializer does not preserve
transport metadata in the present topology. **Why disclose:** it limits proof of
exact-replay transport identity. **Follow-up:** custom deserializer design?
**Spoken:** “I documented the boundary instead of pretending the metadata exists.”

### 44. What would you do in V1.x?
**Direct answer:** Run each physical path, retain evidence, inspect outputs, and
exercise recovery rather than add new platform products. **Why:** validation
should prove the existing design. **Follow-up:** order? **Spoken:** “V1.x is a
hands-on verification phase, not a redesign.”

### 45. How do you explain the project to a hiring manager?
**Direct answer:** It is a deliberately bounded data-platform lab emphasizing
source contracts, deterministic current state, canonical ownership, and honest
validation. **Example:** the stale Spark replay and false-PASS fixes show actual
engineering review. **Spoken:** “I can walk from source evidence to Gold and
explain what is tested versus what is deferred.”

## Additional senior follow-ups

### 46. How would you handle WAL disk pressure?
**Direct answer:** Monitor slot lag and retained WAL, alert before disk pressure,
and resolve stalled consumers deliberately. **MDEP boundary:** prerequisites are
configured, but no live slot metrics exist. **Spoken:** “A replication slot is a
durability contract that can become a disk-risk contract.”

### 47. How would you evolve a CDC schema?
**Direct answer:** Require additive-compatible changes, retain envelope/source
metadata, update consumers, and test transition paths. **MDEP example:** Flink
customer DDL includes an additive field. **Spoken:** “Schema evolution is a
consumer-coordination problem, not just a DDL change.”

### 48. How would you scale Kafka partitions?
**Direct answer:** Choose partitions from throughput and key distribution, while
accepting per-key ordering only. **MDEP boundary:** no physical benchmark exists.
**Spoken:** “More partitions improve parallelism but never create global order.”

### 49. How would you make the Flink sink resilient?
**Direct answer:** verify checkpoint-to-commit behavior, test restart recovery,
and reconcile Iceberg snapshots against raw events. **MDEP boundary:** config is
implemented; physical proof deferred. **Spoken:** “Exactly-once is an end-to-end
property I would verify, not a switch I would merely set.”

### 50. What is the project’s strongest engineering lesson?
**Direct answer:** Correctness comes from explicit ordering and ownership rules,
not from assuming tools automatically provide them. **Example:** hash freshness,
Kafka-offset freshness, and false-PASS were all corrected. **Spoken:** “The
project taught me to make evidence and uncertainty first-class parts of design.”
