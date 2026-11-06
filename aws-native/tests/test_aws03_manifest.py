from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

AWS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(AWS_ROOT))

from mdep_ingestion.models import BatchContext, SourceRecord, canonical_json_bytes, enrich_record, sha256_hex
from mdep_ingestion.object_store import LocalObjectStore
from mdep_ingestion.publication import BronzePublisher
from mdep_ingestion.serialization import to_parquet
from mdep_silver.manifest import ManifestValidationError, verify_committed_batch


def context(entity: str = "locations", source: str = "rest_api") -> BatchContext:
    return BatchContext(
        source, entity, date(2026, 11, 1),
        datetime(2026, 11, 1, tzinfo=timezone.utc),
        datetime(2026, 11, 2, tzinfo=timezone.utc),
        datetime(2026, 11, 2, 1, tzinfo=timezone.utc),
    )


def publish(store: LocalObjectStore, ctx: BatchContext) -> None:
    record = SourceRecord(
        payload={"location_id": "loc-1", "location_name": "Copenhagen", "country_code": "DK", "timezone": "Europe/Copenhagen", "updated_at": "2026-11-01T10:00:00Z"},
        record_key="loc-1", extract_ts="2026-11-01T10:00:00Z", source_version="v1", locator="page=1,row=1",
    )
    result = BronzePublisher(store).publish(ctx, to_parquet([enrich_record(record, ctx)]))
    if result.status.value != "published":
        raise AssertionError(result)


class Aws03ManifestTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = LocalObjectStore(self.temp.name)
        self.ctx = context()
        publish(self.store, self.ctx)

    def tearDown(self):
        self.temp.cleanup()

    def test_verifies_exact_aws02_v2_manifest_and_payload(self):
        verified = verify_committed_batch(self.store, self.ctx.manifest_key)
        self.assertEqual(self.ctx.data_key, verified.data_key)
        self.assertEqual(1, verified.record_count)

    def test_rejects_orphan_tamper_and_wrong_path(self):
        Path(self.temp.name, self.ctx.data_key).unlink()
        with self.assertRaisesRegex(ManifestValidationError, "missing"):
            verify_committed_batch(self.store, self.ctx.manifest_key)
        publish_store = LocalObjectStore(self.temp.name)
        Path(self.temp.name, self.ctx.manifest_key).unlink()
        publish(publish_store, self.ctx)
        Path(self.temp.name, self.ctx.data_key).write_bytes(b"tampered")
        with self.assertRaises(ManifestValidationError):
            verify_committed_batch(self.store, self.ctx.manifest_key)
        with self.assertRaisesRegex(ManifestValidationError, "outside"):
            verify_committed_batch(self.store, "quarantine/_COMMITTED.json")

    def test_rejects_manifest_mutation_and_non_owned_commerce_snapshot(self):
        path = Path(self.temp.name, self.ctx.manifest_key)
        manifest = json.loads(path.read_text())
        manifest["logical_date"] = "2026-10-31"
        path.write_bytes(canonical_json_bytes(manifest))
        with self.assertRaisesRegex(ManifestValidationError, "identity"):
            verify_committed_batch(self.store, self.ctx.manifest_key)

        commerce = context("customers", "postgres")
        payload = to_parquet([])
        BronzePublisher(self.store).publish(commerce, payload)
        with self.assertRaisesRegex(ManifestValidationError, "not_owned"):
            verify_committed_batch(self.store, commerce.manifest_key)

    def test_rejects_manifest_with_unknown_field_and_bad_checksum_shape(self):
        path = Path(self.temp.name, self.ctx.manifest_key)
        manifest = json.loads(path.read_text())
        manifest["unexpected"] = True
        path.write_bytes(canonical_json_bytes(manifest))
        with self.assertRaisesRegex(ManifestValidationError, "fields"):
            verify_committed_batch(self.store, self.ctx.manifest_key)

