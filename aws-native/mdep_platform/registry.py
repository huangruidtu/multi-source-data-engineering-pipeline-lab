"""Strict source registry that dispatches into existing AWS-02 components."""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping
from urllib.parse import urlparse

from jsonschema import Draft202012Validator

from mdep_ingestion.contracts import CONTRACTS
from mdep_ingestion.extractors import FILE_TYPES, HttpClient, extract_file_path, extract_rest
from mdep_ingestion.models import BatchContext, IngestionOutcome, SourceError, Status
from mdep_ingestion.pipeline import IngestionPipeline

from .observability import JsonEventEmitter


REGISTRY_VERSION = "mdep-source-registry-v1"
SOURCE_COMBINATIONS = {
    "json_file": frozenset({"devices"}),
    "csv_file": frozenset({"product_categories"}),
    "rest_api": frozenset({"exchange_rates", "locations"}),
}
SAFE_REASON_TOKEN = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
SAFE_OUTCOME_REASONS = frozenset({
    "data_exists_with_different_source_content",
    "data_write_not_visible",
    "invalid_existing_data",
    "invalid_existing_manifest",
    "manifest_content_conflict",
    "manifest_data_checksum_mismatch",
    "manifest_exists_but_data_missing",
    "manifest_record_count_mismatch",
    "manifest_source_content_checksum_mismatch",
    "reserved_envelope_field",
    "source_content_conflict",
})


def _outcome_error_code(outcome: IngestionOutcome) -> str:
    candidate = outcome.detail.partition(":")[0].strip()
    if SAFE_REASON_TOKEN.fullmatch(candidate) and candidate in SAFE_OUTCOME_REASONS:
        return candidate
    if outcome.status == Status.CONFLICT:
        return "publication_conflict"
    if outcome.quarantine_key:
        return "source_rejected"
    return "publication_failed"


class SourceRegistryError(ValueError):
    pass


@dataclass(frozen=True)
class SourceDefinition:
    source_id: str
    source_type: str
    source_entity: str
    contract_version: str
    enabled: bool
    max_records: int
    owner: str
    description: str
    local_path: Path | None = None
    endpoint_env: str | None = None
    allowed_hosts: tuple[str, ...] = ()
    max_pages: int | None = None


class SourceRegistry:
    def __init__(self, definitions: tuple[SourceDefinition, ...], repository_root: Path):
        self.definitions = definitions
        self.repository_root = repository_root.resolve()
        self._by_id = {definition.source_id: definition for definition in definitions}

    @classmethod
    def load(cls, path: str | Path, *, repository_root: str | Path) -> "SourceRegistry":
        config_path = Path(path)
        try:
            document = json.loads(config_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise SourceRegistryError(f"registry_unreadable:{config_path}") from error
        schema_path = Path(__file__).with_name("source-registry.schema.json")
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        errors = sorted(Draft202012Validator(schema).iter_errors(document), key=lambda item: list(item.path))
        if errors:
            first = errors[0]
            location = ".".join(str(part) for part in first.absolute_path) or "$"
            raise SourceRegistryError(f"registry_schema_invalid:{location}:{first.message}")
        if document["registry_version"] != REGISTRY_VERSION:
            raise SourceRegistryError("unsupported_registry_version")

        root = Path(repository_root).resolve()
        definitions: list[SourceDefinition] = []
        source_ids: set[str] = set()
        enabled_identities: set[tuple[str, str]] = set()
        for item in document["sources"]:
            source_id = item["source_id"]
            if source_id in source_ids:
                raise SourceRegistryError(f"duplicate_source_id:{source_id}")
            source_ids.add(source_id)
            source_type = item["source_type"]
            entity = item["source_entity"]
            if entity not in CONTRACTS or entity not in SOURCE_COMBINATIONS[source_type]:
                raise SourceRegistryError(f"unsupported_source_contract:{source_type}:{entity}")
            if item["contract_version"] != "mdep-6-v1":
                raise SourceRegistryError(f"unsupported_contract_version:{item['contract_version']}")
            identity = (source_type, entity)
            if item["enabled"] and identity in enabled_identities:
                raise SourceRegistryError(f"duplicate_enabled_logical_identity:{source_type}:{entity}")
            if item["enabled"]:
                enabled_identities.add(identity)

            local_path = None
            if source_type in {"json_file", "csv_file"}:
                raw_path = Path(item["local_path"])
                if raw_path.is_absolute() or ".." in raw_path.parts:
                    raise SourceRegistryError(f"unsafe_local_path:{item['local_path']}")
                local_path = (root / raw_path).resolve()
                allowed_roots = (
                    (root / "source-data" / "files").resolve(),
                    (root / "aws-native" / "fixtures").resolve(),
                )
                if not any(allowed_root in local_path.parents for allowed_root in allowed_roots) or local_path.suffix.lower() != FILE_TYPES[entity]:
                    raise SourceRegistryError(f"unsafe_or_unexpected_local_path:{item['local_path']}")

            definitions.append(SourceDefinition(
                source_id=source_id,
                source_type=source_type,
                source_entity=entity,
                contract_version=item["contract_version"],
                enabled=item["enabled"],
                max_records=item["max_records"],
                owner=item["owner"],
                description=item["description"],
                local_path=local_path,
                endpoint_env=item.get("endpoint_env"),
                allowed_hosts=tuple(item.get("allowed_hosts", [])),
                max_pages=item.get("max_pages"),
            ))
        return cls(tuple(definitions), root)

    def get(self, source_id: str) -> SourceDefinition:
        try:
            source = self._by_id[source_id]
        except KeyError as error:
            raise SourceRegistryError(f"unknown_source_id:{source_id}") from error
        if not source.enabled:
            raise SourceRegistryError(f"source_disabled:{source_id}")
        return source

    @staticmethod
    def resolve_endpoint(source: SourceDefinition, environ: Mapping[str, str]) -> str:
        if not source.endpoint_env:
            raise SourceRegistryError(f"source_has_no_endpoint:{source.source_id}")
        url = environ.get(source.endpoint_env, "")
        parsed = urlparse(url)
        if (
            parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password
            or parsed.fragment or parsed.hostname not in source.allowed_hosts
        ):
            raise SourceRegistryError(f"unsafe_or_unexpected_endpoint:{source.source_id}")
        return url


class RegistryRunner:
    def __init__(
        self,
        registry: SourceRegistry,
        pipeline: IngestionPipeline,
        emitter: JsonEventEmitter,
        *,
        environ: Mapping[str, str] | None = None,
        monotonic=time.monotonic,
    ):
        self.registry = registry
        self.pipeline = pipeline
        self.emitter = emitter
        self.environ = environ if environ is not None else os.environ
        self.monotonic = monotonic

    def execute(self, source_id: str, context: BatchContext, *, http_client: HttpClient | None = None) -> IngestionOutcome:
        source = self.registry.get(source_id)
        if context.source_name != source.source_type or context.source_entity != source.source_entity:
            raise SourceRegistryError("context_does_not_match_registered_source")
        started = self.monotonic()
        try:
            if source.source_type in {"json_file", "csv_file"}:
                assert source.local_path is not None
                extraction = extract_file_path(
                    entity=source.source_entity, path=source.local_path, max_records=source.max_records
                )
            else:
                if http_client is None:
                    raise SourceRegistryError("rest_source_requires_http_client")
                extraction = extract_rest(
                    entity=source.source_entity,
                    url=self.registry.resolve_endpoint(source, self.environ),
                    client=http_client,
                    max_pages=source.max_pages or 1,
                    max_records=source.max_records,
                )
        except SourceError as error:
            duration = round((self.monotonic() - started) * 1000)
            outcome = self.pipeline.reject(context, error)
            self.emitter.emit(
                component="source_extractor", action="extract", source_id=source_id,
                source_entity=source.source_entity, run_identity=context.ingestion_id,
                status="failed", rejected_count=1, duration_ms=duration, error_code=error.reason,
            )
            self.emitter.emit(
                component="quarantine_publisher", action="quarantine", source_id=source_id,
                source_entity=source.source_entity, run_identity=context.ingestion_id,
                status="quarantined", rejected_count=1, error_code=error.reason,
                manifest_ref=outcome.quarantine_key,
            )
            return outcome

        duration = round((self.monotonic() - started) * 1000)
        self.emitter.emit(
            component="source_extractor", action="extract", source_id=source_id,
            source_entity=source.source_entity, run_identity=context.ingestion_id,
            status="completed", record_count=len(extraction.records), duration_ms=duration,
        )
        publication_started = self.monotonic()
        outcome = self.pipeline.complete(context, extraction)
        publication_duration = round((self.monotonic() - publication_started) * 1000)
        status = {
            Status.PUBLISHED: "recovered" if outcome.detail == "recovered_matching_orphan_data" else "completed",
            Status.ALREADY_PUBLISHED: "noop",
            Status.CONFLICT: "conflicted",
            Status.FAILED: "failed",
        }[outcome.status]
        self.emitter.emit(
            component="bronze_publisher", action="publish", source_id=source_id,
            source_entity=source.source_entity, run_identity=context.ingestion_id,
            status=status, record_count=outcome.record_count, duration_ms=publication_duration,
            error_code=_outcome_error_code(outcome) if outcome.status in {Status.CONFLICT, Status.FAILED} else None,
            manifest_ref=outcome.manifest_key,
        )
        if outcome.quarantine_key:
            self.emitter.emit(
                component="quarantine_publisher", action="quarantine", source_id=source_id,
                source_entity=source.source_entity, run_identity=context.ingestion_id,
                status="quarantined", rejected_count=1,
                error_code=_outcome_error_code(outcome), manifest_ref=outcome.quarantine_key,
            )
        return outcome
