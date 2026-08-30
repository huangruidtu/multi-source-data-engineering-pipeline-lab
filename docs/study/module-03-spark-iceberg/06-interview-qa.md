# Module 03 — Interview Q&A

### Why is a record hash not a freshness signal?
**Direct answer:** it says content differs, not which version is newer. **Deep explanation:** an older correction/replay can have a different payload. **MDEP example:** MDEP-9 now compares business timestamp → source extract → ingestion → hash. **Why chosen:** prevent Silver regression. **Follow-up:** Why use hash at all? deterministic final tie. **Senior extension:** source sequence/version contracts. **Weak answer:** update whenever hash changes.

### Why Iceberg over Parquet for Silver?
**Direct answer:** Parquet stores files; Iceberg manages table snapshots and atomic state changes. **MDEP example:** Spark configures Iceberg MERGE targets. **Follow-up:** Is the runtime proof present? No. **Senior extension:** compaction/snapshot expiry/catalog concurrency. **Weak answer:** saying Parquet cannot be queried.

### Explain a Spark shuffle.
**Direct answer:** data is redistributed by key between stages. **MDEP example:** window dedup/grouping can shuffle; small country lookup can broadcast. **Follow-up:** How spot skew? plan and task metrics. **Senior extension:** adaptive execution/salting after measurement. **Weak answer:** repartition is always faster.
