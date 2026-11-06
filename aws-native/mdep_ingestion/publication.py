"""Manifest-last publication with verified idempotency and bounded recovery."""

from __future__ import annotations

import json
from typing import Any

from .models import (
    CONTRACT_VERSION,
    FORMAT_VERSION,
    MANIFEST_VERSION,
    BatchContext,
    PreparedPayload,
    PublicationResult,
    Status,
    canonical_json_bytes,
    sha256_hex,
    utc_iso,
)
from .object_store import ConditionalWriteConflict, ObjectStore, ObjectStoreFailure
from .serialization import inspect_parquet


MANIFEST_IDENTITY_FIELDS = (
    "manifest_version",
    "contract_version",
    "format_version",
    "logical_run_identity",
    "source_name",
    "source_entity",
    "logical_date",
    "interval_start",
    "interval_end",
    "data_key",
)


def manifest_for(context: BatchContext, payload: PreparedPayload) -> dict[str, Any]:
    return {
        "manifest_version": MANIFEST_VERSION,
        "contract_version": CONTRACT_VERSION,
        "format_version": FORMAT_VERSION,
        "logical_run_identity": context.ingestion_id,
        "source_name": context.source_name,
        "source_entity": context.source_entity,
        "logical_date": context.logical_date.isoformat(),
        "interval_start": utc_iso(context.interval_start),
        "interval_end": utc_iso(context.interval_end),
        "record_count": payload.record_count,
        "payload_checksum": f"sha256:{payload.checksum}",
        "source_content_checksum": f"sha256:{payload.source_content_checksum}",
        "data_key": context.data_key,
    }


class BronzePublisher:
    """Publishes data first and a validated manifest last.

    S3 cannot atomically commit two objects. The manifest is therefore the
    consumer-visible commit boundary. Orphan data is never considered committed
    and can be completed only when its verified source content matches the retry;
    the first object's bytes and ingestion provenance remain authoritative.
    """

    def __init__(self, store: ObjectStore):
        self.store = store

    def _validate_committed(self, context: BatchContext, payload: PreparedPayload) -> PublicationResult:
        if self.store.head(context.data_key) is None:
            return PublicationResult(Status.CONFLICT, context.data_key, context.manifest_key, payload.record_count, payload.checksum, "manifest_exists_but_data_missing")
        try:
            stored_manifest = json.loads(self.store.get_bytes(context.manifest_key).decode("utf-8"))
            stored_payload = inspect_parquet(self.store.get_bytes(context.data_key))
        except (UnicodeDecodeError, json.JSONDecodeError, ObjectStoreFailure, ValueError):
            return PublicationResult(Status.CONFLICT, context.data_key, context.manifest_key, payload.record_count, payload.checksum, "invalid_existing_manifest")
        expected_manifest = manifest_for(context, payload)
        if any(stored_manifest.get(field) != expected_manifest[field] for field in MANIFEST_IDENTITY_FIELDS):
            return PublicationResult(Status.CONFLICT, context.data_key, context.manifest_key, payload.record_count, payload.checksum, "manifest_content_conflict")
        if stored_manifest.get("payload_checksum") != f"sha256:{stored_payload.checksum}":
            return PublicationResult(Status.CONFLICT, context.data_key, context.manifest_key, payload.record_count, payload.checksum, "manifest_data_checksum_mismatch")
        if stored_manifest.get("record_count") != stored_payload.record_count:
            return PublicationResult(Status.CONFLICT, context.data_key, context.manifest_key, payload.record_count, payload.checksum, "manifest_record_count_mismatch")
        if stored_manifest.get("source_content_checksum") != f"sha256:{stored_payload.source_content_checksum}":
            return PublicationResult(Status.CONFLICT, context.data_key, context.manifest_key, payload.record_count, payload.checksum, "manifest_source_content_checksum_mismatch")
        if stored_payload.source_content_checksum != payload.source_content_checksum:
            return PublicationResult(Status.CONFLICT, context.data_key, context.manifest_key, payload.record_count, payload.checksum, "source_content_conflict")
        detail = "verified_identical_retry" if stored_payload.checksum == payload.checksum else "verified_independent_replay_preserved_first_ingestion"
        return PublicationResult(
            Status.ALREADY_PUBLISHED,
            context.data_key,
            context.manifest_key,
            stored_payload.record_count,
            stored_payload.checksum,
            detail,
        )

    def publish(self, context: BatchContext, payload: PreparedPayload) -> PublicationResult:
        try:
            if self.store.head(context.manifest_key) is not None:
                return self._validate_committed(context, payload)

            data_was_written = False
            if self.store.head(context.data_key) is None:
                try:
                    self.store.put_bytes_if_absent(
                        context.data_key,
                        payload.data,
                        content_type="application/vnd.apache.parquet",
                        checksum=payload.checksum,
                    )
                    data_was_written = True
                except ConditionalWriteConflict:
                    pass
            if self.store.head(context.data_key) is None:
                return PublicationResult(Status.FAILED, context.data_key, context.manifest_key, payload.record_count, payload.checksum, "data_write_not_visible")
            try:
                stored_payload = inspect_parquet(self.store.get_bytes(context.data_key))
            except (ObjectStoreFailure, ValueError):
                return PublicationResult(Status.CONFLICT, context.data_key, context.manifest_key, payload.record_count, payload.checksum, "invalid_existing_data")
            if stored_payload.source_content_checksum != payload.source_content_checksum:
                return PublicationResult(Status.CONFLICT, context.data_key, context.manifest_key, payload.record_count, payload.checksum, "data_exists_with_different_source_content")

            manifest = manifest_for(context, stored_payload)
            manifest_bytes = canonical_json_bytes(manifest)
            manifest_checksum = sha256_hex(manifest_bytes)

            try:
                self.store.put_bytes_if_absent(
                    context.manifest_key,
                    manifest_bytes,
                    content_type="application/json",
                    checksum=manifest_checksum,
                )
            except ConditionalWriteConflict:
                return self._validate_committed(context, payload)
            return PublicationResult(
                Status.PUBLISHED,
                context.data_key,
                context.manifest_key,
                stored_payload.record_count,
                stored_payload.checksum,
                "committed_new_data" if data_was_written else "recovered_matching_orphan_data",
            )
        except ObjectStoreFailure as error:
            return PublicationResult(Status.FAILED, context.data_key, context.manifest_key, payload.record_count, payload.checksum, str(error))

    def committed_manifest(self, manifest_key: str) -> dict[str, Any]:
        manifest = json.loads(self.store.get_bytes(manifest_key).decode("utf-8"))
        required = {
            "manifest_version",
            "logical_run_identity",
            "source_name",
            "source_entity",
            "record_count",
            "payload_checksum",
            "source_content_checksum",
            "data_key",
            "interval_start",
            "interval_end",
            "format_version",
        }
        if not required.issubset(manifest):
            raise ValueError(f"manifest is missing fields: {sorted(required.difference(manifest))}")
        data = self.store.get_bytes(manifest["data_key"])
        payload = inspect_parquet(data)
        if manifest["payload_checksum"] != f"sha256:{payload.checksum}":
            raise ValueError("manifest checksum does not match the stored data object")
        if manifest["record_count"] != payload.record_count:
            raise ValueError("manifest record count does not match the stored data object")
        if manifest["source_content_checksum"] != f"sha256:{payload.source_content_checksum}":
            raise ValueError("manifest source content checksum does not match the stored data object")
        return manifest

    def list_committed(self, prefix: str = "bronze/") -> list[dict[str, Any]]:
        return [self.committed_manifest(key) for key in self.store.list_keys(prefix) if key.endswith("/_COMMITTED.json")]


class QuarantinePublisher:
    def __init__(self, store: ObjectStore):
        self.store = store

    def publish(self, context: BatchContext, *, reason: str, locator: str, original: Any) -> str:
        evidence = {
            "contract_version": CONTRACT_VERSION,
            "ingestion_id": context.ingestion_id,
            "source_name": context.source_name,
            "source_entity": context.source_entity,
            "source_locator": locator,
            "ingested_at": utc_iso(context.ingested_at),
            "rejection_reason": reason,
            "original_payload_or_reference": original,
        }
        content = canonical_json_bytes(evidence)
        evidence_hash = sha256_hex(content)
        key = (
            f"quarantine/{context.source_name}/{context.source_entity}/"
            f"ingest_date={context.logical_date.isoformat()}/"
            f"ingestion_id={context.ingestion_id}/rejection={evidence_hash}.json"
        )
        try:
            self.store.put_bytes_if_absent(key, content, content_type="application/json", checksum=evidence_hash)
        except ConditionalWriteConflict:
            if sha256_hex(self.store.get_bytes(key)) != evidence_hash:
                raise ObjectStoreFailure(f"quarantine evidence conflict for {key}") from None
        return key
