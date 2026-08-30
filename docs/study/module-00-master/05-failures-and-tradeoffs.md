# Module 00 — Failures and trade-offs

| Failure | Detection / impact | Current behavior / recovery | Residual risk / production improvement |
| --- | --- | --- | --- |
| Duplicate Bronze | manifest/key or Silver order | preserve then deduplicate/replay safely | run physical rerun proof; monitor duplication |
| Old update | version comparator | reject stale Silver mutation | source-clock quality and runtime evidence |
| Bad source payload | validation/quarantine | retain payload plus reason | alert on rate and schema drift |
| runtime unavailable | MDEP-13 `BLOCKED` | record blocker, do not pass | execute in capable environment |

Selected: shared Hadoop Iceberg catalog on S3; alternative managed catalog adds service scope. Selected: two processing engines because batch and unbounded state are distinct learning goals; the cost is operating complexity. A production system might use managed services and stronger IAM/observability, but those are not implemented claims.
