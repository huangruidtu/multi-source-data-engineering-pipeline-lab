"""Strict AWS-02 commit-manifest verification before any Silver write."""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from datetime import date
from typing import Any

from mdep_ingestion.models import (
    CONTRACT_VERSION,
    FORMAT_VERSION,
    MANIFEST_VERSION,
    RUN_NAMESPACE,
    canonical_json_bytes,
    parse_utc,
    sha256_hex,
    utc_iso,
)
from mdep_ingestion.object_store import ObjectStore, ObjectStoreFailure
from mdep_ingestion.serialization import inspect_parquet, read_parquet


SUPPORTED_SOURCE_ENTITIES = {("rest_api", "exchange_rates"), ("rest_api", "locations")}
MANIFEST_FIELDS = frozenset(
    {
        "manifest_version",
        "contract_version",
        "format_version",
        "logical_run_identity",
        "source_name",
        "source_entity",
        "logical_date",
        "interval_start",
        "interval_end",
        "record_count",
        "payload_checksum",
        "source_content_checksum",
        "data_key",
    }
)
CHECKSUM_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")


class ManifestValidationError(ValueError):
    """A manifest or its claimed immutable Bronze payload is not trustworthy."""


@dataclass(frozen=True)
class VerifiedBatch:
    manifest_key: str
    data_key: str
    source_name: str
    source_entity: str
    logical_date: str
    interval_start: str
    interval_end: str
    logical_run_identity: str
    record_count: int
    payload_checksum: str
    source_content_checksum: str


def _fail(reason: str) -> ManifestValidationError:
    return ManifestValidationError(reason)


def _expected_identity(manifest: dict[str, Any]) -> str:
    identity = {
        "source_name": manifest["source_name"],
        "source_entity": manifest["source_entity"],
        "logical_date": manifest["logical_date"],
        "interval_start": manifest["interval_start"],
        "interval_end": manifest["interval_end"],
    }
    return str(uuid.uuid5(RUN_NAMESPACE, canonical_json_bytes(identity).decode("utf-8")))


def verify_committed_batch(
    store: ObjectStore,
    manifest_key: str,
    *,
    max_payload_bytes: int = 128 * 1024 * 1024,
) -> VerifiedBatch:
    """Verify one explicit `_COMMITTED.json` and the exact object it commits.

    The bounded full-object read is deliberate for this interview slice. It
    validates the checksum of the bytes Spark will consume and fails before a
    Silver or quarantine write. Production-scale multipart verification is a
    documented follow-up, not an unverified shortcut here.
    """

    if not manifest_key.startswith("bronze/") or not manifest_key.endswith("/_COMMITTED.json"):
        raise _fail("manifest_key_outside_committed_bronze_prefix")
    try:
        raw_manifest = store.get_bytes(manifest_key)
        manifest = json.loads(raw_manifest.decode("utf-8"))
    except (ObjectStoreFailure, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise _fail("manifest_unreadable_or_malformed") from error
    if not isinstance(manifest, dict) or frozenset(manifest) != MANIFEST_FIELDS:
        raise _fail("manifest_fields_do_not_match_v2_contract")
    if manifest["manifest_version"] != MANIFEST_VERSION:
        raise _fail("unsupported_manifest_version")
    if manifest["contract_version"] != CONTRACT_VERSION or manifest["format_version"] != FORMAT_VERSION:
        raise _fail("unsupported_bronze_contract_or_format")
    if (manifest["source_name"], manifest["source_entity"]) not in SUPPORTED_SOURCE_ENTITIES:
        raise _fail("source_entity_not_owned_by_aws03")
    try:
        logical_date = date.fromisoformat(manifest["logical_date"]).isoformat()
        interval_start = parse_utc(manifest["interval_start"])
        interval_end = parse_utc(manifest["interval_end"])
        uuid.UUID(manifest["logical_run_identity"])
    except (TypeError, ValueError) as error:
        raise _fail("invalid_logical_identity_fields") from error
    if interval_start >= interval_end:
        raise _fail("invalid_logical_interval")
    if logical_date != manifest["logical_date"] or manifest["logical_run_identity"] != _expected_identity(manifest):
        raise _fail("logical_run_identity_mismatch")
    expected_prefix = (
        f"bronze/{manifest['source_name']}/{manifest['source_entity']}/"
        f"ingest_date={logical_date}/ingestion_id={manifest['logical_run_identity']}"
    )
    if manifest_key != f"{expected_prefix}/_COMMITTED.json" or manifest["data_key"] != f"{expected_prefix}/data.parquet":
        raise _fail("manifest_and_data_paths_do_not_match_identity")
    if not isinstance(manifest["record_count"], int) or isinstance(manifest["record_count"], bool) or manifest["record_count"] < 0:
        raise _fail("invalid_record_count")
    if not all(CHECKSUM_PATTERN.fullmatch(str(manifest[field])) for field in ("payload_checksum", "source_content_checksum")):
        raise _fail("invalid_checksum_encoding")
    info = store.head(manifest["data_key"])
    if info is None:
        raise _fail("committed_data_object_missing")
    if info.size > max_payload_bytes:
        raise _fail("committed_data_object_exceeds_verification_bound")
    try:
        data = store.get_bytes(manifest["data_key"])
        payload = inspect_parquet(data)
        rows = read_parquet(data)
    except (ObjectStoreFailure, ValueError, OSError) as error:
        raise _fail("committed_data_object_unreadable") from error
    if len(data) != info.size or manifest["payload_checksum"] != f"sha256:{sha256_hex(data)}":
        raise _fail("payload_checksum_mismatch")
    if payload.record_count != manifest["record_count"]:
        raise _fail("record_count_mismatch")
    if manifest["source_content_checksum"] != f"sha256:{payload.source_content_checksum}":
        raise _fail("source_content_checksum_mismatch")
    for row in rows:
        if (
            row.get("source_name") != manifest["source_name"]
            or row.get("source_entity") != manifest["source_entity"]
            or row.get("ingestion_id") != manifest["logical_run_identity"]
        ):
            raise _fail("bronze_envelope_does_not_match_manifest")
    return VerifiedBatch(
        manifest_key=manifest_key,
        data_key=manifest["data_key"],
        source_name=manifest["source_name"],
        source_entity=manifest["source_entity"],
        logical_date=logical_date,
        interval_start=utc_iso(interval_start),
        interval_end=utc_iso(interval_end),
        logical_run_identity=manifest["logical_run_identity"],
        record_count=payload.record_count,
        payload_checksum=payload.checksum,
        source_content_checksum=payload.source_content_checksum,
    )
