# Solutions — 02 Spark Silver workbook

## SS-01
**Correct answer:** `location_id` is the key. Existing tuple starts `2026-08-20`; incoming starts `2026-07-01`, so incoming is lexicographically lower, `incoming_is_newer` is false, and MERGE performs no update. Final name remains Copenhagen Hub.

**Relevant logic/invariant:** locations use `updated_at`, extraction, ingestion, hash in that order; the MERGE mirrors it. An older business version cannot overwrite current state merely because its payload differs.

**Common wrong answer:** let newer ingestion/extract time or changed hash override older business time. **Production consequence:** replayed Bronze changes regress Silver. **Test:** `test_older_business_version_loses_even_with_different_hash` is the model; assert final primary version wins.

**Interview answer:** “I compare business version first and use landing evidence only to resolve equal business versions. A hash is never enough to make an older replay fresh.”

**Senior follow-up:** physical MERGE execution is **MDEP RUNTIME DEFERRED**; validate it with actual staged/target rows and replay evidence.

## SS-02
**Correct answer:** reject hash-driven update. It proves only nonidentical payload. Correct predicate is locations `updated_at → source_extract_ts → ingested_at → record_hash`; exchange rates substitute `retrieved_at` first. Hash can decide only after all timestamps tie.

**Relevant logic/invariant:** `merge_iceberg` explicitly uses timestamp equality guards before hash comparison. **Common wrong answer:** `hash !=` means changed/new. **Production consequence:** older replay overwrites newer Silver. **Test:** older business version/different hash returns false; exact replay is no-op.

**Interview answer:** “I never use payload difference as freshness. The merge uses the same full version tuple as batch deduplication, so a stale changed record still loses.”

**Senior follow-up:** explain why window dedup alone is insufficient: it has no existing target-state version.

## SS-03
**Correct answer:** A is newer because later `ingested_at` wins after first two ties. B is false because tuples are identical. Senior variation is true under current lexicographic rule because `zzzz > aaaa`, but that is deterministic tie resolution, not a claim that hash measures business recency.

**Invariant:** exact replay does not mutate; each tie-breaker has authority only after earlier fields tie. **Common wrong answer:** hash is primary freshness. **Production consequence:** nondeterministic winner/replay churn or stale regression. **Tests:** use `test_ingested_timestamp_breaks_same_business_and_extract_timestamp_tie`, `test_exact_replay_is_a_no_op`, and final-hash tie-breaker test as patterns.

**Interview answer:** “The tuple is lexicographic: each later field matters only if the earlier evidence ties. That makes replays no-ops while still choosing one reproducible winner for exact timestamp ties.”

**Senior follow-up:** decide whether a business domain should allow hash tie-breaking or instead surface an equal-version conflict for governance.
