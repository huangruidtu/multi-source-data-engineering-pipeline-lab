# Module 07 — Failures and tradeoffs

| Failure | Detection | Impact | Current behavior / recovery | Residual risk / improvement |
| --- | --- | --- | --- | --- |
| false PASSED exit | runner tests | portfolio lies | explicit exit-code evidence fix | test runner against fault injection |
| missing runtime prerequisite | preflight | cannot accept component | `BLOCKED` with reason | reproducible capable environment |
| row-count-only reconciliation | query design review | false mismatch/pass | key/aggregate/exception templates | automate evidence retention |
| stale evidence | timestamp/run identity | obsolete confidence | matrix versus run distinction | freshness gate |

MDEP selects a file-based validation plane to avoid adding an observability product to V1. It is low-cost and reviewable, but lacks live monitoring. In production, alerting, lineage/catalog, IAM audit, SLOs, incident response, backup/DR and cost controls would be separately designed.
