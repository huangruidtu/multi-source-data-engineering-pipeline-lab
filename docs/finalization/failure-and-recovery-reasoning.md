# Failure and Recovery Reasoning

`validation/failure-scenarios.yml` is the scenario inventory; this guide explains
the implemented safety reasoning. Scenarios are designed/offline-tested where
noted, not live runtime evidence.

## Batch
| Failure | Detection / current behavior | Recovery and residual risk |
| --- | --- | --- |
| PostgreSQL unavailable | extraction fails before canonical publication; retries are an orchestration concern | restore source and rerun interval; source availability remains a runtime dependency |
| REST 429/5xx | `fetch_paginated_json` retries bounded retryable status and fails closed | clear fault/rerun; no partial Bronze object is intended |
| malformed file | `land_files` quarantines with locator/reason | correct and publish new version; quarantined record needs remediation |
| duplicate rerun | deterministic ingestion/object key returns already-published | no cleanup; concurrent object-store semantics need V1.x exercise |
| stale Bronze replay | Spark lexicographic version order rejects older state | inspect version evidence; physical Iceberg merge remains deferred |

## Debezium and Kafka
| Failure | Detection / current behavior | Recovery and residual risk |
| --- | --- | --- |
| connector/source restart | documented slot/offset recovery path | verify slot/WAL/offsets in V1.x; WAL retention can become data-loss risk |
| offset loss | replay/snapshot risk is documented | recreate in disposable environment and reconcile; duplicates are high risk without history |
| delete/tombstone | delete envelope clears current state; null tombstone is harmless | reconcile keys; topic compaction behavior is runtime-deferred |
| wrong transaction property | static connector test reads `provide.transaction.metadata=true` | configuration correction prevents a false claim; live metadata still unobserved |

## Flink
| Failure | Detection / current behavior | Recovery and residual risk |
| --- | --- | --- |
| lower/stale LSN | `version_decision` ignores it | inspect source order; newer state remains safe |
| same-LSN ambiguity | transaction total order resolves only known same transaction; otherwise conflict | retain diagnostics and investigate; conservative drop prevents overwrite |
| duplicate delivery | known identical transport coordinate is exact replay no-op | archive can duplicate by design; runtime source currently lacks metadata preservation |
| malformed CDC | parser side-output/quarantine path is defined | inspect raw evidence; physical sinks/checkpoints deferred |
| checkpoint/sink failure | checkpoint/restart configuration exists | V1.x must prove recovery and Iceberg commit behavior |

## Analytics and reconciliation

External Iceberg metadata can become stale; dbt test failures, missing FX, and
orphan payments are intentionally visible. `missing_dkk_rate` preserves an
incomplete conversion. Payment/order relationships are warnings so payments are
not silently discarded. Reconciliation templates use counts, anti-joins, keys,
nulls, and exceptions; no invented live count is reported.

## Historical fixes worth explaining

The Spark stale-hash update was replaced by full version ordering. The Debezium
property was corrected from `include.transaction` to
`provide.transaction.metadata`. A Flink runtime stub was replaced with a topology
definition, but its Kafka metadata limitation is disclosed. `GOLD_GOLD` became
`MDEP.GOLD`; `fct_payments` chose a correct full rebuild; and the MDEP-13 runner
now requires both exit code zero and an evidence log before reporting PASS.

**Interview framing:** “For every failure, I say whether V1 prevents it in pure
logic, records it for recovery, or only defines the V1.x runtime exercise.”
