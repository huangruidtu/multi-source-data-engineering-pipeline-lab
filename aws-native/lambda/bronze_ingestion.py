"""Thin, explicitly invoked Lambda adapter for the shared MDEP-68 pipeline."""

from __future__ import annotations

import json
import os
import re
from typing import Any

import boto3

from mdep_ingestion.extractors import UrllibHttpClient
from mdep_ingestion.models import BatchContext, Status
from mdep_ingestion.runtime import build_pipeline, compact_or_raise
from mdep_platform.observability import JsonEventEmitter


POSTGRES_ENTITIES = frozenset({"customers", "products", "orders", "payments"})
SAFE_ERROR_CODE = re.compile(r"^[a-z0-9_]{1,64}$")
REST_ENV = {
    "exchange_rates": "MDEP_REST_EXCHANGE_RATES_URL",
    "locations": "MDEP_REST_LOCATIONS_URL",
}
FILE_ENTITIES = frozenset({"product_categories", "devices"})


def _error_code(detail: str, fallback: str) -> str:
    candidate = detail.partition(":")[0]
    return candidate if SAFE_ERROR_CODE.fullmatch(candidate) else fallback


def _required_environment(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def _context(event: dict[str, Any], source_type: str, entity: str) -> BatchContext:
    try:
        return BatchContext.from_strings(
            source_name=source_type,
            source_entity=entity,
            logical_date=str(event["logical_date"]),
            interval_start=str(event["interval_start"]),
            interval_end=str(event["interval_end"]),
            ingested_at=str(event["ingested_at"]),
        )
    except KeyError as error:
        raise ValueError(f"missing invocation field: {error.args[0]}") from error


def _secret_dsn(secret_arn: str) -> str:
    response = boto3.client("secretsmanager").get_secret_value(SecretId=secret_arn)
    try:
        secret = json.loads(response["SecretString"])
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise RuntimeError("PostgreSQL secret must be a JSON SecretString") from error
    if secret.get("dsn"):
        return str(secret["dsn"])
    required = ("host", "port", "dbname", "username", "password")
    if any(not secret.get(field) for field in required):
        raise RuntimeError(f"PostgreSQL secret requires dsn or {', '.join(required)}")
    from psycopg.conninfo import make_conninfo

    return make_conninfo(
        host=secret["host"],
        port=secret["port"],
        dbname=secret["dbname"],
        user=secret["username"],
        password=secret["password"],
    )


def handler(event: dict[str, Any], _lambda_context: Any) -> dict[str, Any]:
    """Run exactly one source operation; EventBridge/Step Functions invokes it explicitly."""

    if not isinstance(event, dict):
        raise ValueError("invocation event must be an object")
    source_type = str(event.get("source_type", ""))
    entity = str(event.get("entity", ""))
    if source_type not in {"postgresql", "rest_api", "reference_files"} or not entity:
        raise ValueError("source_type and entity must identify an approved source operation")

    bucket = _required_environment("MDEP_DATA_BUCKET")
    kms_key_id = _required_environment("MDEP_KMS_KEY_ID")
    s3_client = boto3.client("s3")
    pipeline = build_pipeline(bucket=bucket, kms_key_id=kms_key_id, s3_client=s3_client)
    context = _context(event, source_type, entity)
    max_records = int(os.getenv("MDEP_MAX_RECORDS", "50000"))

    if source_type == "postgresql":
        if entity not in POSTGRES_ENTITIES:
            raise ValueError(f"unsupported PostgreSQL entity: {entity}")
        import psycopg

        dsn = _secret_dsn(_required_environment("MDEP_POSTGRES_SECRET_ARN"))
        outcome = pipeline.postgres(
            context,
            table=entity,
            connect=lambda: psycopg.connect(dsn, connect_timeout=10),
            max_records=max_records,
        )
    elif source_type == "rest_api":
        environment_name = REST_ENV.get(entity)
        if not environment_name:
            raise ValueError(f"unsupported REST entity: {entity}")
        outcome = pipeline.rest(
            context,
            url=_required_environment(environment_name),
            client=UrllibHttpClient(),
            max_pages=int(os.getenv("MDEP_MAX_PAGES", "100")),
            max_records=max_records,
        )
    else:
        if entity not in FILE_ENTITIES:
            raise ValueError(f"unsupported reference-file entity: {entity}")
        source_key = str(event.get("source_key", ""))
        if not source_key.startswith("source/reference/"):
            raise ValueError("source_key must be under source/reference/")
        response = s3_client.get_object(Bucket=bucket, Key=source_key)
        max_source_bytes = int(os.getenv("MDEP_MAX_SOURCE_BYTES", "10485760"))
        if int(response.get("ContentLength", 0)) > max_source_bytes:
            raise ValueError("source object exceeds MDEP_MAX_SOURCE_BYTES")
        content = response["Body"].read(max_source_bytes + 1)
        if len(content) > max_source_bytes:
            raise ValueError("source object exceeds MDEP_MAX_SOURCE_BYTES")
        outcome = pipeline.file_bytes(context, filename=source_key, content=content, max_records=max_records)

    event_status = {
        Status.PUBLISHED: "recovered" if outcome.detail == "recovered_matching_orphan_data" else "completed",
        Status.ALREADY_PUBLISHED: "noop",
        Status.CONFLICT: "conflicted",
        Status.FAILED: "failed",
    }[outcome.status]
    emitter = JsonEventEmitter()
    emitter.emit(
        component="bronze_ingestion_lambda",
        action="source_run",
        source_id=str(event.get("source_id") or f"lambda-{source_type}-{entity}"),
        source_entity=entity,
        run_identity=context.ingestion_id,
        status=event_status,
        record_count=outcome.record_count,
        rejected_count=1 if outcome.quarantine_key else 0,
        error_code=_error_code(outcome.detail, f"publication_{outcome.status.value}") if outcome.status in {Status.FAILED, Status.CONFLICT} else None,
        manifest_ref=outcome.manifest_key or outcome.quarantine_key,
    )
    if outcome.quarantine_key:
        emitter.emit(
            component="quarantine_publisher",
            action="quarantine",
            source_id=str(event.get("source_id") or f"lambda-{source_type}-{entity}"),
            source_entity=entity,
            run_identity=context.ingestion_id,
            status="quarantined",
            rejected_count=1,
            error_code=_error_code(outcome.detail, "source_rejected"),
            manifest_ref=outcome.quarantine_key,
        )
    compact = compact_or_raise(outcome)
    compact["runtime_evidence"] = "AWS_RUNTIME_DEFERRED" if os.getenv("AWS_EXECUTION_ENV") is None else "AWS_EXECUTED"
    return compact
