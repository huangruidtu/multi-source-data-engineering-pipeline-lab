# Module 03 — Failures and trade-offs

| Failure | Detection / impact | Current behavior | Recovery / improvement |
| --- | --- | --- | --- |
| stale replay different hash | version test | could regress Silver | full tuple rejects it | prove against Iceberg runtime |
| invalid rate/location | normalizer | bad reference | quarantine | quality metrics/remediation |
| shuffle/skew | explain/task metrics | straggler | repartition/broadcast exercise | tune with workload evidence |
| small files/catalog fault | runtime inspection | cost/commit failure | retry from Bronze | compaction/catalog monitoring |

Iceberg was selected over plain Parquet for atomic current state. HadoopCatalog avoids another V1 service, but has operational/catalog-concurrency limits. Spark rather than Flink owns bounded references; a streaming alternative would add complexity without a source requirement.
