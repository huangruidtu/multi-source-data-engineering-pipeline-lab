"""Deterministic, source-aligned Bronze and Quarantine publication."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class BatchContext:
    """Stable identity for one source/entity/data-interval landing operation."""

    logical_date: date
    data_interval_start: datetime
    data_interval_end: datetime
    source_name: str
    source_entity: str

    @property
    def ingestion_id(self) -> str:
        return sha256({
            "logical_date": self.logical_date.isoformat(),
            "data_interval_start": self.data_interval_start.isoformat(),
            "data_interval_end": self.data_interval_end.isoformat(),
            "source_name": self.source_name,
            "source_entity": self.source_entity,
        })[:24]


def bronze_key(context: BatchContext) -> str:
    return (
        f"bronze/{context.source_name}/{context.source_entity}/"
        f"ingest_date={context.logical_date.isoformat()}/"
        f"ingestion_id={context.ingestion_id}/data.parquet"
    )


def quarantine_key(context: BatchContext) -> str:
    return (
        f"quarantine/{context.source_name}/{context.source_entity}/"
        f"ingest_date={context.logical_date.isoformat()}/"
        f"ingestion_id={context.ingestion_id}/rejected.jsonl"
    )


def enrich_record(
    record: dict[str, Any], context: BatchContext, source_record_key: str | None,
    source_extract_ts: str | None, source_version: str | None, source_locator: str,
    ingested_at: str | None = None,
) -> dict[str, Any]:
    """Preserve business fields and append the MDEP-6 landing envelope."""
    payload = dict(record)
    payload.update({
        "ingestion_id": context.ingestion_id,
        "source_name": context.source_name,
        "source_entity": context.source_entity,
        "source_record_key": source_record_key,
        "source_extract_ts": source_extract_ts,
        "ingested_at": ingested_at or utc_now(),
        "source_version": source_version,
        "source_locator": source_locator,
        "record_hash": sha256(record),
    })
    return payload


def quarantine_record(context: BatchContext, reason: str, locator: str, original: Any) -> dict[str, Any]:
    return {
        "ingestion_id": context.ingestion_id,
        "source_name": context.source_name,
        "source_entity": context.source_entity,
        "source_locator": locator,
        "ingested_at": utc_now(),
        "rejection_reason": reason,
        "contract_version": "mdep-6-v1",
        "original_payload": original,
    }


class LocalObjectStore:
    """Filesystem implementation whose object keys mirror S3 prefixes for local validation."""

    def __init__(self, root: str | Path):
        self.root = Path(root)

    def path(self, key: str) -> Path:
        return self.root / key

    def exists(self, key: str) -> bool:
        return self.path(key).exists()

    def read_text(self, key: str) -> str:
        return self.path(key).read_text(encoding="utf-8")

    def put_if_absent(self, key: str, source: Path) -> bool:
        destination = self.path(key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            with source.open("rb") as input_file, destination.open("xb") as output_file:
                shutil.copyfileobj(input_file, output_file)
            return True
        except FileExistsError:
            return False

    def append_json_lines(self, key: str, records: Iterable[dict[str, Any]]) -> None:
        self.put_text_if_absent(key, "".join(canonical_json(record) + "\n" for record in records))

    def put_text_if_absent(self, key: str, content: str) -> bool:
        destination = self.path(key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            with destination.open("x", encoding="utf-8") as handle:
                handle.write(content)
            return True
        except FileExistsError:
            return False


class S3ObjectStore:
    """Optional real-S3 implementation; local validation uses LocalObjectStore."""

    def __init__(self, bucket: str, prefix: str = ""):
        try:
            import boto3
            from botocore.exceptions import ClientError
        except ImportError as error:  # pragma: no cover
            raise RuntimeError("boto3 is required for real S3 publication.") from error
        self.bucket, self.prefix, self.client, self.client_error = bucket, prefix.strip("/"), boto3.client("s3"), ClientError

    def _key(self, key: str) -> str:
        return f"{self.prefix}/{key}".strip("/")

    def exists(self, key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=self._key(key))
            return True
        except self.client_error as error:
            if error.response["Error"]["Code"] in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    def put_if_absent(self, key: str, source: Path) -> bool:
        try:
            self.client.put_object(Bucket=self.bucket, Key=self._key(key), Body=source.read_bytes(), IfNoneMatch="*")
            return True
        except self.client_error as error:
            if error.response["Error"]["Code"] in {"412", "PreconditionFailed"}:
                return False
            raise

    def put_text_if_absent(self, key: str, content: str) -> bool:
        try:
            self.client.put_object(Bucket=self.bucket, Key=self._key(key), Body=content.encode("utf-8"), IfNoneMatch="*")
            return True
        except self.client_error as error:
            if error.response["Error"]["Code"] in {"412", "PreconditionFailed"}:
                return False
            raise


@dataclass(frozen=True)
class PublishResult:
    key: str
    status: str  # published | already_published
    record_count: int


class BronzePublisher:
    """Publishes one deterministic Parquet object plus a content manifest.

    A retry sees the same deterministic key. It never appends to a published
    key: the first completed object is canonical for that logical operation.
    """

    def __init__(self, store: LocalObjectStore | S3ObjectStore):
        self.store = store

    def publish(self, context: BatchContext, records: list[dict[str, Any]]) -> PublishResult:
        key = bronze_key(context)
        manifest_key = key + ".manifest.json"
        object_exists = self.store.exists(key)
        if object_exists and self.store.exists(manifest_key):
            return PublishResult(key, "already_published", len(records))
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as error:  # pragma: no cover - depends on local environment
            raise RuntimeError("PyArrow is required to write Bronze Parquet. Install ingestion/batch/requirements.txt.") from error

        with tempfile.TemporaryDirectory(prefix="mdep-bronze-") as directory:
            staged = Path(directory) / "data.parquet"
            pq.write_table(pa.Table.from_pylist(records), staged, compression="snappy")
            if not object_exists:
                object_exists = not self.store.put_if_absent(key, staged)
        manifest = {
            "ingestion_id": context.ingestion_id,
            "record_count": len(records),
            "payload_hash": sha256(records),
            "data_interval_start": context.data_interval_start.isoformat(),
            "data_interval_end": context.data_interval_end.isoformat(),
        }
        self.store.put_text_if_absent(manifest_key, canonical_json(manifest))
        return PublishResult(key, "already_published" if object_exists else "published", len(records))

    def quarantine(self, context: BatchContext, records: list[dict[str, Any]]) -> str:
        key = quarantine_key(context)
        self.store.put_text_if_absent(key, "".join(canonical_json(record) + "\n" for record in records))
        return key
