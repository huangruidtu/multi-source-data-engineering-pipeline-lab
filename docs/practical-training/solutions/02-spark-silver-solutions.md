# Solutions — 02 Spark Silver
## SS-01
No update: `updated_at` is first in locations version tuple, so July loses to August even with different hash. Invariant: hash is final tie-breaker, never freshness.
## SS-02
The snippet lets an old changed payload regress current state. Replace with business timestamp → extract timestamp → ingest timestamp → hash, matching both window winner and MERGE predicate. `test_older_business_version_loses_even_with_different_hash` protects it.
## SS-03
Later `ingested_at` wins only after business/extract tie; identical tuple returns false for exact replay. Tests prevent nondeterministic same-version selection and duplicate replay updates.
