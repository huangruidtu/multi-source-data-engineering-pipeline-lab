# Module 06 — Failures and tradeoffs

| Failure | Detection / impact | Current behavior | Recovery / improvement |
| --- | --- | --- | --- |
| external access/metadata missing | setup/dbt runtime | Silver unreadable | documented placeholders | IAM integration validation |
| accidental GOLD_GOLD | model target inspection | confusing physical schema | corrected configuration | deployment schema assertions |
| orphan payment | warning relationship test | incomplete fact relationship | retain/flag evidence | business remediation SLA |
| missing FX rate | flag | fabricated value risk | null/flag remains visible | freshness/SLA/late rate handling |

Snowflake-native Gold is selected for warehouse consumption while Iceberg stays external Silver. Type 1 is selected because current Silver deletes/history cannot support valid intervals; Type 2 would need deliberate history. Table rebuild is selected for payments because correctness outweighs lab-scale compute savings.
