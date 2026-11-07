"""Reproducible config -> Bronze -> manifest -> replay/recovery AWS-06 demo."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

AWS_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY = AWS_ROOT.parent
sys.path.insert(0, str(AWS_ROOT))

from mdep_ingestion.models import BatchContext
from mdep_ingestion.object_store import LocalObjectStore, ObjectStoreFailure
from mdep_ingestion.pipeline import IngestionPipeline
from mdep_ingestion.publication import BronzePublisher, QuarantinePublisher
from mdep_platform.observability import JsonEventEmitter, ListEventSink
from mdep_platform.recovery import reprocess_committed_manifest
from mdep_platform.registry import RegistryRunner, SourceRegistry


UTC = timezone.utc


class FailManifestOnce(LocalObjectStore):
    def __init__(self, root):
        super().__init__(root)
        self.failed = False

    def put_bytes_if_absent(self, key, data, *, content_type, checksum):
        if key.endswith("/_COMMITTED.json") and not self.failed:
            self.failed = True
            raise ObjectStoreFailure("simulated_manifest_interruption")
        return super().put_bytes_if_absent(key, data, content_type=content_type, checksum=checksum)


def context(*, day: int = 1, ingested_hour: int = 12) -> BatchContext:
    return BatchContext.from_strings(
        source_name="json_file", source_entity="devices", logical_date=f"2025-02-{day:02d}",
        interval_start=f"2025-02-{day:02d}T00:00:00Z",
        interval_end=f"2025-02-{day + 1:02d}T00:00:00Z",
        ingested_at=f"2026-11-07T{ingested_hour:02d}:00:00Z",
    )


def pipeline(store: LocalObjectStore) -> IngestionPipeline:
    return IngestionPipeline(BronzePublisher(store), QuarantinePublisher(store))


def run_demo(registry_path: Path, object_root: Path) -> dict:
    registry = SourceRegistry.load(registry_path, repository_root=REPOSITORY)
    sink = ListEventSink()
    emitter = JsonEventEmitter(sink, clock=lambda: datetime(2026, 11, 7, 12, tzinfo=UTC))
    store = LocalObjectStore(object_root / "normal")
    bronze = BronzePublisher(store)
    runner = RegistryRunner(registry, pipeline(store), emitter)
    run = context()
    first = runner.execute("devices-json-reference", run)
    replay = runner.execute("devices-json-reference", run)
    manifest = bronze.committed_manifest(str(first.manifest_key))
    data_info = store.head(str(first.data_key))
    processed: list[str] = []
    reprocessed = reprocess_committed_manifest(
        bronze, str(first.manifest_key), lambda value: processed.append(str(value["payload_checksum"])),
        emitter, source_id="devices-json-reference",
    )

    recovery_store = FailManifestOnce(object_root / "recovery")
    recovery_runner = RegistryRunner(registry, pipeline(recovery_store), emitter)
    interrupted = recovery_runner.execute("devices-json-reference", run)
    recovered = recovery_runner.execute("devices-json-reference", run)

    backfill = runner.execute("devices-json-reference", context(day=2, ingested_hour=13))
    base_document = json.loads(registry_path.read_text(encoding="utf-8"))

    def variant(local_path: str, name: str) -> SourceRegistry:
        document = json.loads(json.dumps(base_document))
        document["sources"][0]["local_path"] = local_path
        path = object_root / f"{name}-registry.json"
        path.write_text(json.dumps(document), encoding="utf-8")
        return SourceRegistry.load(path, repository_root=REPOSITORY)

    invalid_runner = RegistryRunner(
        variant("source-data/files/invalid/device-reference-invalid.json", "invalid"), pipeline(store), emitter
    )
    invalid = invalid_runner.execute("devices-json-reference", context(day=3))
    conflict_runner = RegistryRunner(
        variant("aws-native/fixtures/device-reference-conflict.json", "conflict"), pipeline(store), emitter
    )
    conflict = conflict_runner.execute("devices-json-reference", run)
    return {
        "source_id": "devices-json-reference",
        "registered_sources": len(registry.definitions),
        "first_status": first.status.value,
        "replay_status": replay.status.value,
        "interrupted_status": interrupted.status.value,
        "recovered_status": recovered.status.value,
        "backfill_status": backfill.status.value,
        "invalid_status": invalid.status.value,
        "invalid_quarantine_key": invalid.quarantine_key,
        "conflict_status": conflict.status.value,
        "manifest_verified": data_info is not None and manifest["payload_checksum"] == f"sha256:{data_info.checksum}",
        "manifest_key": first.manifest_key,
        "record_count": manifest["record_count"],
        "reprocessed_run_identity": reprocessed.logical_run_identity,
        "committed_manifests": len(bronze.list_committed()),
        "events": sink.events,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", type=Path, default=AWS_ROOT / "config" / "source-registry.json")
    parser.add_argument("--object-root", type=Path)
    args = parser.parse_args()
    if args.object_root:
        result = run_demo(args.registry, args.object_root.resolve())
    else:
        with tempfile.TemporaryDirectory() as directory:
            result = run_demo(args.registry, Path(directory))
    print("AWS06_PLATFORM_EVIDENCE=" + json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
