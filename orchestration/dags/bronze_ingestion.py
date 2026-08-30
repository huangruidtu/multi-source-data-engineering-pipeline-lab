"""MDEP-8: one compact, retryable Bronze ingestion DAG."""
from __future__ import annotations

import sys
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from airflow.decorators import dag, task
from ingestion.batch.pipeline import land_files, land_postgres, land_rest


@dag(
    dag_id="mdep_bronze_ingestion",
    start_date=datetime(2025, 2, 1, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["mdep", "bronze", "batch"],
)
def mdep_bronze_ingestion():
    @task
    def postgres_snapshot(logical_date: str):
        return [land_postgres(logical_date, table) for table in ("customers", "products", "orders", "payments")]

    @task
    def rest_reference(logical_date: str):
        base = os.getenv("REST_SOURCE_BASE_URL", "http://localhost:8080/v1")
        return [land_rest(logical_date, "exchange_rates", f"{base}/exchange-rates?page_size=2"), land_rest(logical_date, "locations", f"{base}/locations?page_size=2")]

    @task
    def file_reference(logical_date: str):
        return land_files(logical_date)

    @task
    def complete(*_results):
        return "All source-aligned Bronze tasks completed."

    logical_date = "{{ ds }}"
    complete(postgres_snapshot(logical_date), rest_reference(logical_date), file_reference(logical_date))


mdep_bronze_ingestion()
