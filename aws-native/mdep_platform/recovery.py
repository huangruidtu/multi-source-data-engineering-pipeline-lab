"""Explicit recovery helpers that preserve the existing manifest/checkpoint protocols."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Mapping, Any

from mdep_ingestion.publication import BronzePublisher

from .observability import JsonEventEmitter


@dataclass(frozen=True)
class ReprocessingResult:
    manifest_key: str
    logical_run_identity: str
    record_count: int
    processor_result: Any


def reprocess_committed_manifest(
    publisher: BronzePublisher,
    manifest_key: str,
    processor: Callable[[Mapping[str, Any]], Any],
    emitter: JsonEventEmitter,
    *,
    source_id: str,
) -> ReprocessingResult:
    """Verify the immutable commit before invoking a selected downstream processor."""
    try:
        manifest = publisher.committed_manifest(manifest_key)
        result = processor(manifest)
    except Exception as error:
        emitter.emit(
            component="manifest_reprocessor", action="reprocess", source_id=source_id,
            source_entity="unknown", run_identity="unverified", status="failed",
            error_code=type(error).__name__, manifest_ref=manifest_key,
        )
        raise
    emitter.emit(
        component="manifest_reprocessor", action="reprocess", source_id=source_id,
        source_entity=str(manifest["source_entity"]), run_identity=str(manifest["logical_run_identity"]),
        status="completed", record_count=int(manifest["record_count"]), manifest_ref=manifest_key,
    )
    return ReprocessingResult(
        manifest_key=manifest_key,
        logical_run_identity=str(manifest["logical_run_identity"]),
        record_count=int(manifest["record_count"]),
        processor_result=result,
    )
