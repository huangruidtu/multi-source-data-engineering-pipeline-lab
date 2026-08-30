# Module 03 — Code walkthrough

Reading order: `processing/spark/contracts.py` → `tests/test_silver_contracts.py` → `processing/spark/silver_batch.py` → `scripts/run-mdep-9-silver.ps1` → ADR `0001`.

`contracts.py` restricts batch ownership, normalizes rates/locations, builds natural keys and compares full version tuples. The tests prove newer business version, stale-different-hash rejection, exact replay, timestamp tie breaks and final hash tie break. `silver_batch.py` configures the `mdep` Hadoop Iceberg catalog, creates reference tables, reads Bronze paths, validates/deduplicates and emits Iceberg SQL MERGE. Its inspection/skew flags are learning aids. The PowerShell script documents a `spark-submit` invocation. Do not invent Spark execution from a passing pure-Python contract test.
