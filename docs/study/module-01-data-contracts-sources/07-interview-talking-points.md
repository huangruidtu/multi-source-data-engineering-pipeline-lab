# Module 01 — Interview talking points

**30 seconds:** “I defined contract ownership before ingestion: PostgreSQL owns commerce entities, REST/files own references, and every landing receives provenance.” 中文：强调数据源责任，不要只背字段。

**2-minute detail:** “I use primary and foreign keys for OLTP integrity, business keys for downstream grain, and explicit required/nullable rules. Invalid fixtures are intentional. They demonstrate that bad input becomes quarantine evidence with a locator and rejection reason, not a silent drop.” 中文：说明 quarantine 不是修复后的 Silver。

**Trade-off:** “Strict validation protects keys but can increase quarantine. In production I would monitor its rate and agree remediation ownership with the source team.” 中文：这是生产建议，当前未验证运行时。
