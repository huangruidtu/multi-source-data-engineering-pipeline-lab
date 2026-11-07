"""Read-only Spark/Iceberg validation for approved historical reference batches."""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import asdict, dataclass
from datetime import timedelta
from typing import Callable

from mdep_ingestion.models import parse_utc, utc_iso
from mdep_ingestion.object_store import ObjectStore

from .manifest import ManifestValidationError, VerifiedBatch, verify_committed_batch
from .spark_job import newer_than_sql, transform


IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
REQUIRED_ENTITIES = ("exchange_rates", "locations")
KEYS = {
    "exchange_rates": ("rate_date", "base_currency", "quote_currency"),
    "locations": ("location_id",),
}
EXPECTED_SCHEMAS = {
    "exchange_rates": (
        ("rate_date", "date"), ("base_currency", "string"), ("quote_currency", "string"),
        ("rate", "decimal(20,10)"), ("retrieved_at", "timestamp"), ("provider_name", "string"),
        ("source_payload_json", "string"), ("ingestion_id", "string"), ("source_name", "string"),
        ("source_entity", "string"), ("source_record_key", "string"),
        ("source_extract_ts", "timestamp"), ("ingested_at", "timestamp"),
        ("source_version", "string"), ("source_locator", "string"), ("record_hash", "string"),
        ("manifest_key", "string"), ("logical_date", "date"),
        ("interval_start", "timestamp"), ("interval_end", "timestamp"),
    ),
    "locations": (
        ("location_id", "string"), ("location_name", "string"), ("country_code", "string"),
        ("timezone", "string"), ("region", "string"), ("updated_at", "timestamp"),
        ("location_type", "string"), ("source_payload_json", "string"),
        ("ingestion_id", "string"), ("source_name", "string"), ("source_entity", "string"),
        ("source_record_key", "string"), ("source_extract_ts", "timestamp"),
        ("ingested_at", "timestamp"), ("source_version", "string"),
        ("source_locator", "string"), ("record_hash", "string"), ("manifest_key", "string"),
        ("logical_date", "date"), ("interval_start", "timestamp"), ("interval_end", "timestamp"),
    ),
}
PROVENANCE_FIELDS = (
    "ingestion_id", "source_name", "source_entity", "ingested_at", "source_locator",
    "record_hash", "manifest_key", "logical_date", "interval_start", "interval_end",
)


class BackfillValidationError(ValueError):
    """A bounded reason token for a failed validation; never embeds source rows."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class ValidationRequest:
    catalog: str
    namespace: str
    manifest_keys: tuple[str, ...]
    validation_run_identity: str
    expected_interval_start: str
    expected_interval_end: str
    max_payload_bytes: int = 128 * 1024 * 1024


@dataclass(frozen=True)
class EntityValidationSummary:
    source_entity: str
    manifest_count: int
    input_record_count: int
    input_business_key_count: int
    current_state_record_count: int
    current_state_business_key_count: int
    exact_match_count: int
    superseded_by_newer_count: int
    unrelated_current_key_count: int
    snapshot_id: str
    source_content_checksums: tuple[str, ...]


@dataclass(frozen=True)
class BackfillValidationReport:
    report_version: str
    validation_run_identity: str
    catalog: str
    namespace: str
    expected_interval_start: str
    expected_interval_end: str
    manifest_count: int
    status: str
    entities: tuple[EntityValidationSummary, ...]

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))


def _validate_request(request: ValidationRequest) -> tuple[object, object]:
    if not IDENTIFIER.fullmatch(request.catalog) or not IDENTIFIER.fullmatch(request.namespace):
        raise BackfillValidationError("invalid_catalog_or_namespace")
    try:
        run_id = str(uuid.UUID(request.validation_run_identity))
        start = parse_utc(request.expected_interval_start)
        end = parse_utc(request.expected_interval_end)
    except (TypeError, ValueError) as error:
        raise BackfillValidationError("invalid_validation_identity_or_interval") from error
    if run_id != request.validation_run_identity:
        raise BackfillValidationError("noncanonical_validation_run_identity")
    if start >= end or end - start > timedelta(days=31):
        raise BackfillValidationError("expected_interval_outside_31_day_bound")
    if not 1 <= len(request.manifest_keys) <= 20 or len(set(request.manifest_keys)) != len(request.manifest_keys):
        raise BackfillValidationError("manifest_key_set_invalid_or_unbounded")
    if not 1 <= request.max_payload_bytes <= 128 * 1024 * 1024:
        raise BackfillValidationError("payload_verification_bound_invalid")
    return start, end


def _verified_batches(store: ObjectStore, request: ValidationRequest, start, end) -> list[VerifiedBatch]:
    batches: list[VerifiedBatch] = []
    for key in request.manifest_keys:
        try:
            batch = verify_committed_batch(store, key, max_payload_bytes=request.max_payload_bytes)
        except ManifestValidationError as error:
            raise BackfillValidationError(str(error)) from error
        if parse_utc(batch.interval_start) < start or parse_utc(batch.interval_end) > end:
            raise BackfillValidationError("manifest_outside_expected_interval")
        batches.append(batch)
    if {batch.source_entity for batch in batches} != set(REQUIRED_ENTITIES):
        raise BackfillValidationError("required_reference_entity_manifest_missing_or_unexpected")
    return batches


def _table(spark, name: str):
    try:
        return spark.table(name)
    except Exception as error:
        raise BackfillValidationError("required_silver_table_unavailable") from error


def _schema(frame) -> tuple[tuple[str, str], ...]:
    return tuple((field.name, field.dataType.simpleString()) for field in frame.schema.fields)


def _invalid_current_count(frame, entity: str) -> int:
    from functools import reduce
    from pyspark.sql import functions as F

    missing_provenance = reduce(lambda left, right: left | right, (F.col(name).isNull() for name in PROVENANCE_FIELDS))
    if entity == "exchange_rates":
        invalid_business = (
            F.col("rate_date").isNull() | F.col("rate").isNull() | (F.col("rate") <= 0)
            | F.col("retrieved_at").isNull() | (F.length("base_currency") != 3)
            | (F.length("quote_currency") != 3) | (F.col("base_currency") == F.col("quote_currency"))
        )
    else:
        invalid_business = (
            F.col("location_id").isNull() | (F.trim("location_id") == "")
            | F.col("location_name").isNull() | (F.trim("location_name") == "")
            | F.col("timezone").isNull() | (F.trim("timezone") == "")
            | F.col("region").isNull() | F.col("updated_at").isNull()
        )
    return frame.where(missing_provenance | invalid_business).limit(1).count()


def _snapshot_id(spark, table_name: str) -> str:
    try:
        rows = spark.sql(f"SELECT snapshot_id FROM {table_name}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
    except Exception as error:
        raise BackfillValidationError("iceberg_snapshot_metadata_unavailable") from error
    if not rows:
        raise BackfillValidationError("iceberg_table_has_no_snapshot")
    return str(rows[0]["snapshot_id"])


def validate_backfill(
    spark,
    store: ObjectStore,
    request: ValidationRequest,
    *,
    data_path: Callable[[str], str],
) -> BackfillValidationReport:
    """Validate Bronze evidence against current Iceberg state without any write."""
    start, end = _validate_request(request)
    from functools import reduce
    from pyspark.sql import functions as F

    batches = _verified_batches(store, request, start, end)
    summaries: list[EntityValidationSummary] = []
    for entity in REQUIRED_ENTITIES:
        entity_batches = [batch for batch in batches if batch.source_entity == entity]
        transformed = []
        for batch in entity_batches:
            raw = spark.read.parquet(data_path(batch.data_key))
            transformed.append(transform(raw, batch))
        historical = reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), transformed).cache()
        input_count = historical.count()
        if historical.where(F.col("rejection_reason").isNotNull()).limit(1).count():
            historical.unpersist()
            raise BackfillValidationError("invalid_historical_reference_value")
        valid_input = historical.drop("rejection_reason")
        keys = list(KEYS[entity])
        duplicate_input = valid_input.groupBy(*keys).count().where("count > 1").limit(1).count()
        if duplicate_input:
            historical.unpersist()
            raise BackfillValidationError("duplicate_historical_business_key")

        table_name = f"{request.catalog}.{request.namespace}.ref_{entity}"
        current = _table(spark, table_name).cache()
        if _schema(current) != EXPECTED_SCHEMAS[entity]:
            historical.unpersist()
            current.unpersist()
            raise BackfillValidationError("unexpected_silver_schema")
        current_count = current.count()
        current_key_count = current.select(*keys).distinct().count()
        if current_count != current_key_count:
            historical.unpersist()
            current.unpersist()
            raise BackfillValidationError("duplicate_current_business_key")
        if _invalid_current_count(current, entity):
            historical.unpersist()
            current.unpersist()
            raise BackfillValidationError("invalid_current_reference_value_or_provenance")

        joined = valid_input.alias("i").join(current.alias("c"), keys, "left")
        status = joined.select(
            F.when(F.col("c.record_hash").isNull(), "missing")
            .when(F.col("c.record_hash") == F.col("i.record_hash"), "exact")
            .when(F.expr(newer_than_sql(entity, "c", "i")), "superseded")
            .otherwise("conflict")
            .alias("status")
        ).groupBy("status").count().collect()
        counts = {row["status"]: row["count"] for row in status}
        if counts.get("missing", 0):
            historical.unpersist()
            current.unpersist()
            raise BackfillValidationError("historical_business_key_missing_from_current_silver")
        if counts.get("conflict", 0):
            historical.unpersist()
            current.unpersist()
            raise BackfillValidationError("current_silver_not_equal_or_newer_than_historical_input")

        approved = [batch.manifest_key for batch in entity_batches]
        unexpected = current.where(F.col("manifest_key").isin(approved)).join(valid_input.select(*keys), keys, "left_anti").limit(1).count()
        if unexpected:
            historical.unpersist()
            current.unpersist()
            raise BackfillValidationError("approved_manifest_has_unexpected_current_business_key")
        unrelated = current.select(*keys).join(valid_input.select(*keys), keys, "left_anti").count()
        summaries.append(
            EntityValidationSummary(
                source_entity=entity,
                manifest_count=len(entity_batches),
                input_record_count=input_count,
                input_business_key_count=valid_input.select(*keys).distinct().count(),
                current_state_record_count=current_count,
                current_state_business_key_count=current_key_count,
                exact_match_count=counts.get("exact", 0),
                superseded_by_newer_count=counts.get("superseded", 0),
                unrelated_current_key_count=unrelated,
                snapshot_id=_snapshot_id(spark, table_name),
                source_content_checksums=tuple(sorted(batch.source_content_checksum for batch in entity_batches)),
            )
        )
        historical.unpersist()
        current.unpersist()
    return BackfillValidationReport(
        report_version="mdep-backfill-validation-v1",
        validation_run_identity=request.validation_run_identity,
        catalog=request.catalog,
        namespace=request.namespace,
        expected_interval_start=utc_iso(start),
        expected_interval_end=utc_iso(end),
        manifest_count=len(batches),
        status="validated",
        entities=tuple(summaries),
    )
