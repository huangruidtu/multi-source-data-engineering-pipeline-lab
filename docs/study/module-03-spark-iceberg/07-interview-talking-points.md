# Module 03 — Interview talking points

**60 seconds:** “Spark owns only bounded reference Silver. It validates Bronze, quarantines invalid records, deduplicates within a batch, then compares incoming state with existing Iceberg state deterministically.” 中文：强调 Spark 不处理 CDC-owned tables。

**Debugging story:** “A review found hash-change logic could accept an old replay. I made business time primary, then extraction and landing evidence; hash became only a final deterministic tie-breaker.” 中文：这是项目真实修正，适合面试。

**Production improvement:** “I would inspect plans, skew, snapshot growth, files and merge metrics in a real Spark/Iceberg environment; those engine runs remain unvalidated.” 中文：不要说已经检查。
