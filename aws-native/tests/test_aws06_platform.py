from __future__ import annotations

import json
import sys
import tempfile
import unittest
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

AWS_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY = AWS_ROOT.parent
sys.path.insert(0, str(AWS_ROOT))

from mdep_ingestion.models import BatchContext, Status
from mdep_ingestion.object_store import LocalObjectStore, ObjectStoreFailure
from mdep_ingestion.pipeline import IngestionPipeline
from mdep_ingestion.publication import BronzePublisher, QuarantinePublisher
from mdep_platform.observability import JsonEventEmitter, ListEventSink
from mdep_platform.recovery import reprocess_committed_manifest
from mdep_platform.registry import RegistryRunner, SourceRegistry, SourceRegistryError
from scripts.platform_inventory import preview


UTC = timezone.utc
REGISTRY_PATH = AWS_ROOT / "config" / "source-registry.json"


def context(day=1, ingested_at="2026-11-07T10:00:00Z"):
    return BatchContext.from_strings(
        source_name="json_file", source_entity="devices", logical_date=f"2025-02-{day:02d}",
        interval_start=f"2025-02-{day:02d}T00:00:00Z",
        interval_end=f"2025-02-{day + 1:02d}T00:00:00Z", ingested_at=ingested_at,
    )


def make_pipeline(store):
    return IngestionPipeline(BronzePublisher(store), QuarantinePublisher(store))


class RegistryValidationTests(unittest.TestCase):
    def setUp(self):
        self.document = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))

    def load(self, document):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "registry.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            return SourceRegistry.load(path, repository_root=REPOSITORY)

    def test_approved_devices_source_registers_without_pipeline_code_change(self):
        registry = SourceRegistry.load(REGISTRY_PATH, repository_root=REPOSITORY)
        source = registry.get("devices-json-reference")
        self.assertEqual(("json_file", "devices", 100), (source.source_type, source.source_entity, source.max_records))
        self.assertEqual(REPOSITORY / "source-data/files/valid/device-reference.json", source.local_path)

    def test_schema_rejects_unknown_fields_types_entities_missing_and_limits(self):
        mutations = []
        for field, value in (
            ("source_type", "jdbc"), ("source_entity", "invented"),
            ("max_records", 0), ("max_records", 100001),
        ):
            candidate = deepcopy(self.document)
            candidate["sources"][0][field] = value
            mutations.append(candidate)
        missing = deepcopy(self.document)
        del missing["sources"][0]["owner"]
        mutations.append(missing)
        secret = deepcopy(self.document)
        secret["sources"][0]["password"] = "never-store-this"
        mutations.append(secret)
        for candidate in mutations:
            with self.subTest(candidate=candidate), self.assertRaises(SourceRegistryError):
                self.load(candidate)

    def test_duplicate_source_id_and_enabled_logical_identity_are_rejected(self):
        duplicate_id = deepcopy(self.document)
        duplicate_id["sources"].append(deepcopy(duplicate_id["sources"][0]))
        with self.assertRaisesRegex(SourceRegistryError, "duplicate_source_id"):
            self.load(duplicate_id)
        duplicate_identity = deepcopy(self.document)
        second = deepcopy(duplicate_identity["sources"][0])
        second["source_id"] = "devices-json-second"
        second["local_path"] = "aws-native/fixtures/device-reference-conflict.json"
        duplicate_identity["sources"].append(second)
        with self.assertRaisesRegex(SourceRegistryError, "duplicate_enabled_logical_identity"):
            self.load(duplicate_identity)

    def test_unsafe_paths_and_unexpected_rest_endpoint_are_rejected(self):
        for unsafe in ("../secret.json", str((REPOSITORY / "source-data/files/valid/device-reference.json").resolve())):
            candidate = deepcopy(self.document)
            candidate["sources"][0]["local_path"] = unsafe
            with self.assertRaisesRegex(SourceRegistryError, "unsafe_local_path"):
                self.load(candidate)
        rest = {
            "registry_version": "mdep-source-registry-v1",
            "sources": [{
                "source_id": "locations-rest", "source_type": "rest_api", "source_entity": "locations",
                "contract_version": "mdep-6-v1", "endpoint_env": "MDEP_LOCATIONS_URL",
                "allowed_hosts": ["api.example.test"], "enabled": True, "max_records": 100,
                "max_pages": 2, "owner": "MDEP platform", "description": "Bounded locations endpoint"
            }],
        }
        registry = self.load(rest)
        source = registry.get("locations-rest")
        for url in ("http://api.example.test/locations", "https://evil.example/locations", "https://u:p@api.example.test/x"):
            with self.assertRaisesRegex(SourceRegistryError, "unsafe_or_unexpected_endpoint"):
                registry.resolve_endpoint(source, {"MDEP_LOCATIONS_URL": url})


class PlatformIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.registry = SourceRegistry.load(REGISTRY_PATH, repository_root=REPOSITORY)
        self.sink = ListEventSink()
        self.emitter = JsonEventEmitter(self.sink, clock=lambda: datetime(2026, 11, 7, 10, tzinfo=UTC))

    def tearDown(self):
        self.temp.cleanup()

    def runner(self, store, registry=None):
        return RegistryRunner(registry or self.registry, make_pipeline(store), self.emitter, monotonic=lambda: 1.0)

    def registry_for_path(self, local_path):
        document = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
        document["sources"][0]["local_path"] = local_path
        path = self.root / (Path(local_path).stem + "-registry.json")
        path.write_text(json.dumps(document), encoding="utf-8")
        return SourceRegistry.load(path, repository_root=REPOSITORY)

    def test_real_onboarding_manifest_verification_replay_backfill_and_reprocessing(self):
        store = LocalObjectStore(self.root / "objects")
        runner = self.runner(store)
        first = runner.execute("devices-json-reference", context())
        replay = runner.execute("devices-json-reference", context(ingested_at="2026-11-07T11:00:00Z"))
        backfill = runner.execute("devices-json-reference", context(day=2))
        publisher = BronzePublisher(store)
        manifest = publisher.committed_manifest(str(first.manifest_key))
        calls = []
        reprocessed = reprocess_committed_manifest(
            publisher, str(first.manifest_key), calls.append, self.emitter, source_id="devices-json-reference"
        )
        self.assertEqual(Status.PUBLISHED, first.status)
        self.assertEqual(Status.ALREADY_PUBLISHED, replay.status)
        self.assertEqual("verified_independent_replay_preserved_first_ingestion", replay.detail)
        self.assertEqual(Status.PUBLISHED, backfill.status)
        self.assertEqual(2, manifest["record_count"])
        self.assertEqual((first.ingestion_id, "devices", "json_file"), (
            manifest["logical_run_identity"], manifest["source_entity"], manifest["source_name"],
        ))
        self.assertRegex(manifest["payload_checksum"], r"^sha256:[0-9a-f]{64}$")
        self.assertRegex(manifest["source_content_checksum"], r"^sha256:[0-9a-f]{64}$")
        self.assertEqual(2, len(publisher.list_committed()))
        self.assertEqual(first.ingestion_id, reprocessed.logical_run_identity)
        self.assertEqual(1, len(calls))
        statuses = [event["status"] for event in self.sink.events]
        self.assertIn("noop", statuses)
        self.assertTrue(all("payload" not in json.dumps(event).lower() for event in self.sink.events))

    def test_invalid_source_is_quarantined_and_conflicting_replay_fails_closed(self):
        store = LocalObjectStore(self.root / "objects")
        invalid_registry = self.registry_for_path("source-data/files/invalid/device-reference-invalid.json")
        invalid = self.runner(store, invalid_registry).execute("devices-json-reference", context())
        self.assertEqual(Status.FAILED, invalid.status)
        self.assertIsNotNone(invalid.quarantine_key)
        self.assertIsNone(store.head(context().manifest_key))

        valid = self.runner(store).execute("devices-json-reference", context(day=2))
        conflict_registry = self.registry_for_path("aws-native/fixtures/device-reference-conflict.json")
        conflict = self.runner(store, conflict_registry).execute("devices-json-reference", context(day=2))
        self.assertEqual(Status.PUBLISHED, valid.status)
        self.assertEqual(Status.CONFLICT, conflict.status)
        self.assertIn("conflict", conflict.detail)
        self.assertIn("quarantined", [event["status"] for event in self.sink.events])
        self.assertIn("conflicted", [event["status"] for event in self.sink.events])

    def test_interrupted_manifest_retry_recovers_same_identity_without_rewriting_data(self):
        class FailManifestOnce(LocalObjectStore):
            def __init__(self, root):
                super().__init__(root)
                self.failed = False

            def put_bytes_if_absent(self, key, data, *, content_type, checksum):
                if key.endswith("/_COMMITTED.json") and not self.failed:
                    self.failed = True
                    raise ObjectStoreFailure("simulated interruption")
                return super().put_bytes_if_absent(key, data, content_type=content_type, checksum=checksum)

        store = FailManifestOnce(self.root / "objects")
        runner = self.runner(store)
        failed = runner.execute("devices-json-reference", context())
        first_data = store.get_bytes(context().data_key)
        recovered = runner.execute("devices-json-reference", context())
        self.assertEqual(Status.FAILED, failed.status)
        self.assertEqual(Status.PUBLISHED, recovered.status)
        self.assertEqual("recovered_matching_orphan_data", recovered.detail)
        self.assertEqual(first_data, store.get_bytes(context().data_key))
        self.assertEqual(context().ingestion_id, recovered.ingestion_id)
        self.assertIn("recovered", [event["status"] for event in self.sink.events])

    def test_cost_inventory_is_configuration_only_and_never_deletes(self):
        result = preview(AWS_ROOT / "config" / "sandbox-preview.json")
        self.assertEqual("NON_DESTRUCTIVE_CONFIGURATION_PREVIEW", result["mode"])
        self.assertFalse(result["observed_billing"])
        self.assertFalse(result["deletion_performed"])
        self.assertEqual({}, result["cost_assumptions"])
        self.assertIn("enable_dms_cdc", result["disabled_flags"])


if __name__ == "__main__":
    unittest.main()
