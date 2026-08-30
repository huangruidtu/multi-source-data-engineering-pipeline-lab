"""Task-level functions used by the Airflow DAG and local validation."""
from __future__ import annotations

import os
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ingestion.batch.bronze import BatchContext, BronzePublisher, LocalObjectStore, S3ObjectStore, enrich_record, quarantine_record
from ingestion.batch.extractors import fetch_paginated_json, file_identity, postgres_rows, read_csv, read_json


REPO_ROOT = Path(__file__).resolve().parents[2]


def context(logical_date: str, source_name: str, entity: str) -> BatchContext:
    day = date.fromisoformat(logical_date[:10])
    start = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
    return BatchContext(day, start, start + timedelta(days=1), source_name, entity)


def publisher() -> BronzePublisher:
    bucket = os.getenv("BRONZE_S3_BUCKET")
    if bucket:
        return BronzePublisher(S3ObjectStore(bucket, os.getenv("BRONZE_S3_PREFIX", "")))
    return BronzePublisher(LocalObjectStore(os.getenv("BRONZE_LOCAL_ROOT", str(REPO_ROOT / "build" / "local-object-store"))))


def land_rest(logical_date: str, entity: str, endpoint: str) -> dict[str, Any]:
    ctx = context(logical_date, "rest_api", entity)
    source_rows = fetch_paginated_json(endpoint)
    rows = [enrich_record(row, ctx, None, row.get("retrieved_at") or row.get("updated_at"), None, endpoint) for row in source_rows]
    result = publisher().publish(ctx, rows)
    return result.__dict__


def land_postgres(logical_date: str, table: str, incremental: bool = False) -> dict[str, Any]:
    try:
        import psycopg
    except ImportError as error:  # pragma: no cover
        raise RuntimeError("psycopg is required for PostgreSQL extraction.") from error
    ctx = context(logical_date, "postgresql", table)
    connect = lambda: psycopg.connect(os.getenv("POSTGRES_DSN", "postgresql://lab:lab@localhost:5432/commerce"))
    start = ctx.data_interval_start.isoformat() if incremental else None
    end = ctx.data_interval_end.isoformat() if incremental else None
    source_rows = postgres_rows(connect, table, start, end)
    key = f"{table[:-1]}_id"
    rows = [enrich_record(row, ctx, str(row.get(key)) if row.get(key) else None, str(row.get("updated_at")), None, f"commerce.{table}") for row in source_rows]
    return publisher().publish(ctx, rows).__dict__


def land_files(logical_date: str) -> dict[str, Any]:
    valid = REPO_ROOT / "source-data/files/valid"
    invalid = REPO_ROOT / "source-data/files/invalid"
    publications, quarantine_keys, identities = [], [], set()
    for path in sorted(valid.glob("*")):
        source_name = "csv_file" if path.suffix == ".csv" else "json_file"
        file_context = context(logical_date, source_name, path.stem.replace("-", "_"))
        identity = file_identity(path)
        if identity in identities:
            quarantine_keys.append(publisher().quarantine(file_context, [quarantine_record(file_context, "duplicate_file_content", str(path), {"content_hash": identity})]))
            continue
        identities.add(identity)
        rows = read_csv(path) if path.suffix == ".csv" else read_json(path)
        output = []
        for index, row in enumerate(rows, start=1):
            output.append(enrich_record(row, file_context, row.get("category_code") or row.get("device_id"), None, identity, f"{path}#{index}"))
        publications.append(publisher().publish(file_context, output).__dict__)
    for path in sorted(invalid.glob("*")):
        source_name = "csv_file" if path.suffix == ".csv" else "json_file"
        file_context = context(logical_date, source_name, path.stem.replace("-", "_"))
        rejected = []
        try:
            rows = read_csv(path) if path.suffix == ".csv" else read_json(path)
            for index, row in enumerate(rows, start=1):
                rejected.append(quarantine_record(file_context, "invalid_fixture_requires_silver_validation", f"{path}#{index}", row))
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            rejected.append(quarantine_record(file_context, "malformed_json", str(path), {"error": str(error), "raw": path.read_text(encoding="utf-8")}))
        quarantine_keys.append(publisher().quarantine(file_context, rejected))
    missing = REPO_ROOT / "source-data/files/scenarios/expected-location-overrides.csv"
    if not missing.exists():
        missing_context = context(logical_date, "csv_file", "expected_location_overrides")
        quarantine_keys.append(publisher().quarantine(missing_context, [quarantine_record(missing_context, "missing_expected_file", str(missing), None)]))
    return {"publications": publications, "quarantine_keys": quarantine_keys}
