# Module 02 — Code walkthrough

Reading order: `orchestration/dags/bronze_ingestion.py` → `ingestion/batch/pipeline.py` → `extractors.py` → `bronze.py` → `scripts/validate-mdep-8-runtime.ps1`.

`bronze_ingestion.py` declares orchestration dependencies. `pipeline.py` composes source-specific extraction with a shared context. `postgres_rows` allows full or bounded `updated_at` extraction for a learning exercise. `fetch_paginated_json` accumulates pages before publication. `BronzePublisher.publish` writes Parquet, a manifest, and returns `published`/`already_published`; `quarantine` writes rejection JSONL. The validator documents the intended twice-run/backfill evidence. Notice the separation: no Spark business transformation is hidden in the DAG.
