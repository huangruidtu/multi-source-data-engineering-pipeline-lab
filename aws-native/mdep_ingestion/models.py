"""Stable identity and evidence models shared by every AWS-native source path."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import StrEnum
from typing import Any


CONTRACT_VERSION = "mdep-6-v1"
FORMAT_VERSION = "parquet-v1"
MANIFEST_VERSION = "mdep-bronze-manifest-v2"
RUN_NAMESPACE = uuid.UUID("8f40746f-d3ce-52c7-9694-9f367cf03f40")
ENVELOPE_FIELDS = frozenset(
    {
        "ingestion_id",
        "source_name",
        "source_entity",
        "source_record_key",
        "source_extract_ts",
        "ingested_at",
        "source_version",
        "source_locator",
        "record_hash",
    }
)


def utc_iso(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Timestamps must be timezone-aware.")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_utc(value: str | datetime) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Timestamps must include a UTC offset.")
    return parsed.astimezone(timezone.utc)


def normalize_json(value: Any) -> Any:
    if isinstance(value, datetime):
        return utc_iso(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, dict):
        return {str(key): normalize_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize_json(item) for item in value]
    return value


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(normalize_json(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sha256_hex(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def record_hash(record: dict[str, Any]) -> str:
    return sha256_hex(canonical_json_bytes(record))


class Status(StrEnum):
    PUBLISHED = "published"
    ALREADY_PUBLISHED = "already_published"
    CONFLICT = "conflict"
    FAILED = "failed"


@dataclass(frozen=True)
class BatchContext:
    source_name: str
    source_entity: str
    logical_date: date
    interval_start: datetime
    interval_end: datetime
    ingested_at: datetime

    def __post_init__(self) -> None:
        start = parse_utc(self.interval_start)
        end = parse_utc(self.interval_end)
        if start >= end:
            raise ValueError("Batch interval must be non-empty and use [start, end) order.")
        if not self.source_name or not self.source_entity:
            raise ValueError("source_name and source_entity are required.")
        object.__setattr__(self, "interval_start", start)
        object.__setattr__(self, "interval_end", end)
        object.__setattr__(self, "ingested_at", parse_utc(self.ingested_at))

    @classmethod
    def from_strings(
        cls,
        *,
        source_name: str,
        source_entity: str,
        logical_date: str,
        interval_start: str,
        interval_end: str,
        ingested_at: str,
    ) -> "BatchContext":
        return cls(
            source_name=source_name,
            source_entity=source_entity,
            logical_date=date.fromisoformat(logical_date[:10]),
            interval_start=parse_utc(interval_start),
            interval_end=parse_utc(interval_end),
            ingested_at=parse_utc(ingested_at),
        )

    @property
    def identity_fields(self) -> dict[str, str]:
        return {
            "source_name": self.source_name,
            "source_entity": self.source_entity,
            "logical_date": self.logical_date.isoformat(),
            "interval_start": utc_iso(self.interval_start),
            "interval_end": utc_iso(self.interval_end),
        }

    @property
    def ingestion_id(self) -> str:
        return str(uuid.uuid5(RUN_NAMESPACE, canonical_json_bytes(self.identity_fields).decode("utf-8")))

    @property
    def data_key(self) -> str:
        return (
            f"bronze/{self.source_name}/{self.source_entity}/"
            f"ingest_date={self.logical_date.isoformat()}/"
            f"ingestion_id={self.ingestion_id}/data.parquet"
        )

    @property
    def manifest_key(self) -> str:
        return self.data_key.rsplit("/", 1)[0] + "/_COMMITTED.json"


@dataclass(frozen=True)
class SourceRecord:
    payload: dict[str, Any]
    record_key: str | None
    extract_ts: str | None
    source_version: str | None
    locator: str


@dataclass(frozen=True)
class Extraction:
    records: tuple[SourceRecord, ...]
    source_version: str | None = None


@dataclass(frozen=True)
class PreparedPayload:
    data: bytes
    record_count: int
    checksum: str
    source_content_checksum: str


@dataclass(frozen=True)
class PublicationResult:
    status: Status
    data_key: str
    manifest_key: str
    record_count: int
    checksum: str
    detail: str


@dataclass(frozen=True)
class IngestionOutcome:
    status: Status
    source_name: str
    source_entity: str
    ingestion_id: str
    record_count: int = 0
    data_key: str | None = None
    manifest_key: str | None = None
    quarantine_key: str | None = None
    detail: str = ""

    def compact(self) -> dict[str, Any]:
        value = asdict(self)
        value["status"] = self.status.value
        return value


class SourceError(RuntimeError):
    def __init__(self, reason: str, locator: str, original: Any = None, *, retryable: bool = False):
        super().__init__(f"{reason}: {locator}")
        self.reason = reason
        self.locator = locator
        self.original = original
        self.retryable = retryable


def enrich_record(record: SourceRecord, context: BatchContext) -> dict[str, Any]:
    collisions = ENVELOPE_FIELDS.intersection(record.payload)
    if collisions:
        raise SourceError("reserved_envelope_field", record.locator, {"fields": sorted(collisions)})
    business = normalize_json(record.payload)
    return {
        **business,
        "ingestion_id": context.ingestion_id,
        "source_name": context.source_name,
        "source_entity": context.source_entity,
        "source_record_key": record.record_key,
        "source_extract_ts": record.extract_ts,
        "ingested_at": utc_iso(context.ingested_at),
        "source_version": record.source_version,
        "source_locator": record.locator,
        "record_hash": record_hash(record.payload),
    }
