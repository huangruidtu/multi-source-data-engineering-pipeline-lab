# Module 04 — Failures and tradeoffs

| Failure | Detection / impact | Current behavior | Recovery / improvement |
| --- | --- | --- | --- |
| connector/slot lag | connector and DB metrics | WAL retention/disk risk | fix connector, monitor lag | retention/runbook |
| duplicate delivery | consumer versioning | repeat mutation | downstream idempotency | replay test |
| schema addition | envelope change | consumer parser risk | additive `preferred_language` exercise | compatibility policy |
| wrong transaction property | JSON test | metadata not enabled | corrected to `provide.transaction.metadata` | runtime observation |

CDC is selected over polling for mutation fidelity/latency; it costs operational slot and broker management. Kafka is transport, not an ordering oracle or database. A schema registry could help production but is outside V1.
