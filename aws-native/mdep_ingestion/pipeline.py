"""Cohesive source -> enrichment -> Parquet -> commit-manifest pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from .extractors import HttpClient, extract_file_bytes, extract_file_path, extract_postgres, extract_rest
from .models import BatchContext, Extraction, IngestionOutcome, SourceError, Status, enrich_record
from .publication import BronzePublisher, QuarantinePublisher
from .serialization import to_parquet


class IngestionPipeline:
    def __init__(self, publisher: BronzePublisher, quarantine: QuarantinePublisher):
        self.publisher = publisher
        self.quarantine = quarantine

    def complete(self, context: BatchContext, extraction: Extraction) -> IngestionOutcome:
        try:
            enriched = [enrich_record(record, context) for record in extraction.records]
            prepared = to_parquet(enriched)
        except SourceError as error:
            return self.reject(context, error)
        result = self.publisher.publish(context, prepared)
        return IngestionOutcome(
            status=result.status,
            source_name=context.source_name,
            source_entity=context.source_entity,
            ingestion_id=context.ingestion_id,
            record_count=result.record_count,
            data_key=result.data_key,
            manifest_key=result.manifest_key,
            detail=result.detail,
        )

    def reject(self, context: BatchContext, error: SourceError) -> IngestionOutcome:
        key = self.quarantine.publish(context, reason=error.reason, locator=error.locator, original=error.original)
        return IngestionOutcome(
            status=Status.FAILED,
            source_name=context.source_name,
            source_entity=context.source_entity,
            ingestion_id=context.ingestion_id,
            quarantine_key=key,
            detail=str(error),
        )

    def postgres(
        self,
        context: BatchContext,
        *,
        table: str,
        connect: Callable[[], Any],
        incremental_by_updated_at: bool = False,
        max_records: int = 100_000,
    ) -> IngestionOutcome:
        try:
            extraction = extract_postgres(
                context=context,
                table=table,
                connect=connect,
                incremental_by_updated_at=incremental_by_updated_at,
                max_records=max_records,
            )
        except SourceError as error:
            return self.reject(context, error)
        return self.complete(context, extraction)

    def rest(
        self,
        context: BatchContext,
        *,
        url: str,
        client: HttpClient,
        timeout_seconds: float = 10,
        max_retries: int = 2,
        max_pages: int = 100,
        max_records: int = 50_000,
    ) -> IngestionOutcome:
        try:
            extraction = extract_rest(
                entity=context.source_entity,
                url=url,
                client=client,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                max_pages=max_pages,
                max_records=max_records,
            )
        except SourceError as error:
            return self.reject(context, error)
        return self.complete(context, extraction)

    def file_path(self, context: BatchContext, *, path: str | Path, max_records: int = 50_000) -> IngestionOutcome:
        try:
            extraction = extract_file_path(entity=context.source_entity, path=path, max_records=max_records)
        except SourceError as error:
            return self.reject(context, error)
        return self.complete(context, extraction)

    def file_bytes(self, context: BatchContext, *, filename: str, content: bytes, max_records: int = 50_000) -> IngestionOutcome:
        try:
            extraction = extract_file_bytes(entity=context.source_entity, filename=filename, content=content, max_records=max_records)
        except SourceError as error:
            return self.reject(context, error)
        return self.complete(context, extraction)
