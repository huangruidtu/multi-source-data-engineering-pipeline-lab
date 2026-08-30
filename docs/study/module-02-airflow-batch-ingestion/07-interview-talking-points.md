# Module 02 — Interview talking points

**30 seconds:** “Airflow coordinates bounded source extraction; it does not transform into Silver. Each source/entity/interval has a deterministic Bronze identity, so a retry is repeatable.” 中文：logical date 不是实际执行时间。

**Failure story:** “For REST I collect every page before publication. Retryable 429/5xx responses back off; exhaustion fails rather than leaving an unexplained partial object.” 中文：强调部分数据的风险。

**Production answer:** “I would prove DAG retry/backfill and object-store behavior with the supplied validator, then monitor freshness, retry rate and quarantine volume. Those runs are still unvalidated here.” 中文：不要把脚本当作执行证据。
