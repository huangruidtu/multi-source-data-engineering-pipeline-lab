"""MDEP-68: source-operation-atomic AWS Bronze ingestion with compact XComs."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

AWS_NATIVE_ROOT = Path(__file__).resolve().parents[2]
if str(AWS_NATIVE_ROOT) not in sys.path:
    sys.path.insert(0, str(AWS_NATIVE_ROOT))

from airflow.sdk import Param, dag, get_current_context, task

from mdep_ingestion.extractors import UrllibHttpClient
from mdep_ingestion.orchestration import resolve_run
from mdep_ingestion.runtime import compact_or_raise, pipeline_from_environment


def _resolved_run():
    context = get_current_context()
    params = context["params"]
    dag_started_at = context["dag_run"].start_date
    if dag_started_at is None:
        raise RuntimeError("Airflow dag_run.start_date is required as the stable ingestion timestamp")
    return resolve_run(
        logical_date=context["logical_date"],
        data_interval_start=context["data_interval_start"],
        data_interval_end=context["data_interval_end"],
        ingested_at=dag_started_at,
        backfill_start=params.get("backfill_start"),
        backfill_end=params.get("backfill_end"),
    )


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} is required at task runtime")
    return value


@dag(
    dag_id="mdep_aws_bronze_ingestion",
    start_date=datetime(2025, 2, 1, tzinfo=timezone.utc),
    schedule=os.getenv("MDEP_AIRFLOW_SCHEDULE", "@daily"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    params={
        "backfill_start": Param(None, type=["null", "string"], format="date-time"),
        "backfill_end": Param(None, type=["null", "string"], format="date-time"),
    },
    tags=["mdep", "aws-native", "bronze", "batch"],
)
def aws_bronze_ingestion():
    @task
    def postgres_snapshot() -> list[dict[str, object]]:
        import psycopg

        run = _resolved_run()
        dsn = _required("MDEP_POSTGRES_DSN")
        pipeline = pipeline_from_environment()
        return [
            compact_or_raise(
                pipeline.postgres(
                    run.context("postgresql", entity),
                    table=entity,
                    connect=lambda: psycopg.connect(dsn),
                )
            )
            for entity in ("customers", "products", "orders", "payments")
        ]

    @task
    def rest_reference() -> list[dict[str, object]]:
        run = _resolved_run()
        pipeline = pipeline_from_environment()
        client = UrllibHttpClient()
        endpoints = {
            "exchange_rates": _required("MDEP_REST_EXCHANGE_RATES_URL"),
            "locations": _required("MDEP_REST_LOCATIONS_URL"),
        }
        return [
            compact_or_raise(pipeline.rest(run.context("rest_api", entity), url=url, client=client))
            for entity, url in endpoints.items()
        ]

    @task
    def file_reference() -> list[dict[str, object]]:
        run = _resolved_run()
        pipeline = pipeline_from_environment()
        paths = {
            "product_categories": _required("MDEP_PRODUCT_CATEGORIES_PATH"),
            "devices": _required("MDEP_DEVICES_PATH"),
        }
        return [
            compact_or_raise(pipeline.file_path(run.context("reference_files", entity), path=path))
            for entity, path in paths.items()
        ]

    @task
    def completion_summary(*groups: list[dict[str, object]]) -> dict[str, object]:
        outcomes = [outcome for group in groups for outcome in group]
        return {
            "source_operations": len(outcomes),
            "records": sum(int(outcome["record_count"]) for outcome in outcomes),
            "statuses": sorted({str(outcome["status"]) for outcome in outcomes}),
        }

    completion_summary(postgres_snapshot(), rest_reference(), file_reference())


dag = aws_bronze_ingestion()
