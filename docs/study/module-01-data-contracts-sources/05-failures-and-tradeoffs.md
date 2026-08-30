# Module 01 — Failures and trade-offs

| Failure | Detection | Impact | Current behavior / recovery | Residual risk / improvement |
| --- | --- | --- | --- | --- |
| required field absent | contract validation | unusable Silver row | quarantine with locator/reason | alert and contract-version handling |
| malformed JSON | parser | extraction cannot type record | preserve raw evidence | schema registry/producer tests |
| duplicate file | content identity | double count risk | deterministic identity, later dedup | prove real object-store behavior |
| FK violation | source DB example | orphan transaction | source rejects/fixture demonstrates | data stewardship workflow |

MDEP selects source-aligned Bronze over source-side “cleanup”: it costs storage and consumer diligence, but preserves auditability. Strict required fields are safer than permissive coercion for keys; a production alternative may allow nullable optional attributes with metrics.
