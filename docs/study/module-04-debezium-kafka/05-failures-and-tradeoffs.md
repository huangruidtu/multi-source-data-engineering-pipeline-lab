# Module 04 — Failures and tradeoffs

| Failure | Detection / impact | Current behavior | Recovery / improvement |
| --- | --- | --- | --- |
| connector/slot lag | connector and DB metrics | WAL retention/disk risk | fix connector, monitor lag | retention/runbook |
| duplicate delivery | consumer versioning | repeat mutation | downstream idempotency | replay test |
| schema addition | envelope change | consumer parser risk | additive `preferred_language` exercise | compatibility policy |
| wrong transaction property | JSON test | metadata not enabled | corrected to `provide.transaction.metadata` | runtime observation |

CDC is selected over polling for mutation fidelity/latency; it costs operational slot and broker management. Kafka is transport, not an ordering oracle or database. A schema registry could help production but is outside V1.

## Failure catalogue grounded in MDEP

| Trigger | Detection | Impact | Current MDEP behaviour | Recovery | Residual risk / improvement | Interview lesson |
| --- | --- | --- | --- | --- | --- | --- |
| `wal_level` not logical | validator `SHOW wal_level` | connector cannot decode rows | Compose configures it | correct source configuration | protect/monitor DB settings | CDC starts at database |
| publication misses table | publication query/topic absence | silent gap | exact four-table list is tested | alter deliberately, assess snapshot | contract approval | narrow ownership matters |
| connector stops | status and lag | WAL retained/delayed data | slot remains | diagnose Connect/broker/source | alert task/slot lag | slot is source-disk responsibility |
| slot recreated | LSN/snapshot change | duplicate/gap ambiguity | no destructive auto-recovery | choose rebootstrap plan | test RPO | do not drop a slot casually |
| old replay | consumer decision | state regression if naïve | M11 version rule | replay from Bronze safely | prove physical replay | offset is not freshness |
| schema addition | changed after image | consumer break | additive field exercise | compatibility rollout | version contracts | additive still needs handling |
| delete/tombstone confusion | envelope inspection | double delete | separate semantic delete/marker | retain evidence | test compaction | marker is not business mutation |
| wrong transaction key | JSON contract test | metadata absent | corrected property | retain regression test | inspect runtime event | config names matter |
| one broker loss | health/lag | total outage | single-node KRaft lab | restore/replay | multi-broker RF/ISR | lab is not HA |

## Trade-offs

**CDC versus polling:** CDC provides mutation history and source position, but needs privileges, retained WAL monitoring and recovery planning. Polling is simpler for reference data, but has weaker delete/intermediate-update semantics.

**PK keying versus random distribution:** primary-key keys preserve per-entity order needed for state application. They can create hot-key skew. MDEP chooses correctness and inspectability; production would measure cardinality/traffic before repartitioning.

**One-node KRaft versus replication:** KRaft removes ZooKeeper from this lab. RF=1 has no ISR or broker-failure resilience. Leader/follower/acks/ISR are important production concepts, but are **GENERAL / NOT IMPLEMENTED IN MDEP**.

**Transaction metadata versus global ordering:** transaction fields improve interpretation where Debezium supplies them. They never impose global ordering across topics/partitions, so MDEP remains LSN-first and conservative about ambiguity.

## Case study: interrupted initial snapshot

### Trigger

Connect or the source database fails while an `initial` snapshot is being produced.

### Detection and data impact

Connector/task status, snapshot markers, source position and topic history must be inspected. A consumer may see part of a baseline and later resumed records; naïvely treating every snapshot record as a later update can produce incorrect state.

### Current MDEP behavior and recovery

MDEP configures initial snapshot and records the runtime exercise, but has no completed physical test. Downstream accepts a first snapshot row as bootstrap state and uses source ordering thereafter. A real recovery must establish whether Connect/slot offsets were retained and whether an intentional re-snapshot is required.

### Residual risk and interview lesson

The code does not prove snapshot restart semantics. In interview, say that snapshot state and streaming state are distinct phases, and that the recovery decision requires evidence rather than “restart it and hope.”

## Case study: offset and slot mismatch

### Trigger

Kafka Connect offsets are lost, reset, or no longer agree with the PostgreSQL slot position.

### Detection

Compare connector configuration/status, internal offset topic availability, slot position, source WAL and produced topic evidence. The exact remediation depends on which system retained truth.

### Data impact and recovery

The path can replay records, require a snapshot, or expose a gap if source WAL is no longer available. MDEP’s architecture relies on downstream idempotent state application for duplicate replay, but it cannot repair a source gap by itself. Production needs a documented rebootstrap procedure, bounded reconciliation and explicit RPO decision.

### Common weak answer

“Kafka will remember it” is weak because the source slot and Connect offsets are separate state stores.

## Case study: retention and compaction confusion

Retention controls how long log segments remain available. Compaction keeps a latest-key representation and tombstone markers subject to its policy; it is not an immediate database delete guarantee. If retention is shorter than a consumer outage, replay may be impossible from Kafka even though PostgreSQL capture continued. MDEP does not configure or test a production retention/compaction policy. A production design would choose them from recovery window, throughput, legal retention and source rebootstrap capability.
