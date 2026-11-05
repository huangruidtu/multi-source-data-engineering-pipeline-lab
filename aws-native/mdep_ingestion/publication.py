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
        "data_key": context.data_key,
    }


class BronzePublisher:
    """Publishes data first and a validated manifest last.

    S3 cannot atomically commit two objects. The manifest is therefore the
    consumer-visible commit boundary. Orphan data is never considered committed
    and can be completed only when its bytes match the retried payload.
    """

    def __init__(self, store: ObjectStore):
        self.store = store

    def _actual_checksum(self, key: str) -> str:
        return sha256_hex(self.store.get_bytes(key))

    def _validate_committed(self, context: BatchContext, payload: PreparedPayload, expected_manifest: dict[str, Any]) -> PublicationResult:
        if self.store.head(context.data_key) is None:
            return PublicationResult(Status.CONFLICT, context.data_key, context.manifest_key, payload.record_count, payload.checksum, "manifest_exists_but_data_missing")
        try:
            stored_manifest = json.loads(self.store.get_bytes(context.manifest_key).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError, ObjectStoreFailure):
            return PublicationResult(Status.CONFLICT, context.data_key, context.manifest_key, payload.record_count, payload.checksum, "invalid_existing_manifest")
        if stored_manifest != expected_manifest:
            return PublicationResult(Status.CONFLICT, context.data_key, context.manifest_key, payload.record_count, payload.checksum, "manifest_content_conflict")
        if self._actual_checksum(context.data_key) != payload.checksum:
            return PublicationResult(Status.CONFLICT, context.data_key, context.manifest_key, payload.record_count, payload.checksum, "manifest_data_checksum_mismatch")
        return PublicationResult(Status.ALREADY_PUBLISHED, context.data_key, context.manifest_key, payload.record_count, payload.checksum, "verified_identical_replay")

    def publish(self, context: BatchContext, payload: PreparedPayload) -> PublicationResult:
        manifest = manifest_for(context, payload)
        manifest_bytes = canonical_json_bytes(manifest)
        manifest_checksum = sha256_hex(manifest_bytes)
        try:
            if self.store.head(context.manifest_key) is not None:
                return self._validate_committed(context, payload, manifest)

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
            if self._actual_checksum(context.data_key) != payload.checksum:
                return PublicationResult(Status.CONFLICT, context.data_key, context.manifest_key, payload.record_count, payload.checksum, "data_exists_with_different_content")

            try:
                self.store.put_bytes_if_absent(
                    context.manifest_key,
                    manifest_bytes,
                    content_type="application/json",
                    checksum=manifest_checksum,
                )
            except ConditionalWriteConflict:
                return self._validate_committed(context, payload, manifest)
            return PublicationResult(
                Status.PUBLISHED,
                context.data_key,
                context.manifest_key,
                payload.record_count,
                payload.checksum,
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
            "data_key",
            "interval_start",
            "interval_end",
            "format_version",
        }
        if not required.issubset(manifest):
            raise ValueError(f"manifest is missing fields: {sorted(required.difference(manifest))}")
        data = self.store.get_bytes(manifest["data_key"])
        if manifest["payload_checksum"] != f"sha256:{sha256_hex(data)}":
            raise ValueError("manifest checksum does not match the stored data object")
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
