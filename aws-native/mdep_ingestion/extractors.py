"""Dependency-injected PostgreSQL, REST, CSV and JSON extractors."""

from __future__ import annotations

import csv
import io
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

from .contracts import business_key, validate_record
from .models import BatchContext, Extraction, SourceError, SourceRecord, sha256_hex, utc_iso


POSTGRES_TABLES = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "payments": "payment_id",
}
RETRYABLE_HTTP_STATUS = frozenset({429, 500, 502, 503, 504})
FILE_TYPES = {"product_categories": ".csv", "devices": ".json"}


@dataclass(frozen=True)
class HttpResponse:
    status: int
    body: bytes
    headers: Mapping[str, str]


class HttpClient(Protocol):
    def get(self, url: str, *, timeout: float) -> HttpResponse: ...


class UrllibHttpClient:
    def get(self, url: str, *, timeout: float) -> HttpResponse:
        request = Request(url, method="GET", headers={"Accept": "application/json"})
        try:
            with urlopen(request, timeout=timeout) as response:
                return HttpResponse(response.status, response.read(), dict(response.headers.items()))
        except HTTPError as error:
            return HttpResponse(error.code, error.read(), dict(error.headers.items()))
        except (URLError, TimeoutError) as error:
            raise SourceError("rest_transport_error", url, str(error), retryable=True) from error


def _paged_url(url: str, page: int) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["page"] = str(page)
    return urlunparse(parsed._replace(query=urlencode(query)))


def extract_rest(
    *,
    entity: str,
    url: str,
    client: HttpClient,
    timeout_seconds: float = 10,
    max_retries: int = 2,
    backoff_seconds: float = 0.25,
    max_pages: int = 100,
    max_records: int = 50_000,
    sleep: Callable[[float], None] = time.sleep,
) -> Extraction:
    records: list[SourceRecord] = []
    seen_pages: set[int] = set()
    page: int | None = 1
    versions: list[str] = []
    while page is not None:
        if page in seen_pages:
            raise SourceError("pagination_loop", url, {"page": page})
        if len(seen_pages) >= max_pages:
            raise SourceError("max_pages_exceeded", url, {"max_pages": max_pages})
        seen_pages.add(page)
        request_url = _paged_url(url, page)
        response: HttpResponse | None = None
        for attempt in range(max_retries + 1):
            try:
                response = client.get(request_url, timeout=timeout_seconds)
            except SourceError as error:
                if not error.retryable or attempt == max_retries:
                    raise
                sleep(backoff_seconds * (2**attempt))
                continue
            if 200 <= response.status < 300:
                break
            if response.status in RETRYABLE_HTTP_STATUS and attempt < max_retries:
                retry_after = response.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else backoff_seconds * (2**attempt)
                sleep(min(delay, 30.0))
                continue
            reason = "rest_retry_exhausted" if response.status in RETRYABLE_HTTP_STATUS else "rest_non_retryable_http"
            raise SourceError(reason, request_url, {"status": response.status}, retryable=response.status in RETRYABLE_HTTP_STATUS)
        if response is None or not 200 <= response.status < 300:
            raise SourceError("rest_retry_exhausted", request_url, retryable=True)
        try:
            payload = json.loads(response.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise SourceError("malformed_json", request_url, response.body[:512].decode("utf-8", errors="replace")) from error
        if not isinstance(payload, dict) or not isinstance(payload.get("items"), list) or "next_page" not in payload:
            raise SourceError("invalid_response_schema", request_url, payload)
        version = response.headers.get("ETag")
        if version:
            versions.append(version)
        for index, item in enumerate(payload["items"]):
            locator = f"{request_url}#item={index}"
            if not isinstance(item, dict):
                raise SourceError("invalid_response_schema", locator, item)
            contract = validate_record(entity, item, locator)
            records.append(
                SourceRecord(
                    payload=item,
                    record_key=business_key(contract, item),
                    extract_ts=str(item.get(contract.extract_timestamp)) if contract.extract_timestamp else None,
                    source_version=version,
                    locator=locator,
                )
            )
            if len(records) > max_records:
                raise SourceError("max_records_exceeded", url, {"max_records": max_records})
        next_page = payload["next_page"]
        if next_page is not None and (not isinstance(next_page, int) or isinstance(next_page, bool) or next_page <= 0):
            raise SourceError("invalid_next_page", request_url, next_page)
        page = next_page
    return Extraction(tuple(records), ",".join(versions) or None)


def extract_postgres(
    *,
    context: BatchContext,
    table: str,
    connect: Callable[[], Any],
    incremental_by_updated_at: bool = False,
    fetch_size: int = 1_000,
    max_records: int = 100_000,
) -> Extraction:
    primary_key = POSTGRES_TABLES.get(table)
    if primary_key is None:
        raise SourceError("unsupported_postgres_table", table, sorted(POSTGRES_TABLES))
    query = f"SELECT * FROM commerce.{table}"
    parameters: tuple[Any, ...] = ()
    if incremental_by_updated_at:
        query += " WHERE updated_at >= %s AND updated_at < %s"
        parameters = (context.interval_start, context.interval_end)
    query += f" ORDER BY {primary_key}"
    records: list[SourceRecord] = []
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute(query, parameters)
            columns = [column.name if hasattr(column, "name") else column[0] for column in cursor.description]
            while True:
                batch = cursor.fetchmany(fetch_size)
                if not batch:
                    break
                if len(records) + len(batch) > max_records:
                    raise SourceError("max_records_exceeded", f"commerce.{table}", {"max_records": max_records})
                for row in batch:
                    payload = dict(zip(columns, row, strict=True))
                    locator = f"commerce.{table}/{payload.get(primary_key)}"
                    contract = validate_record(table, payload, locator)
                    records.append(
                        SourceRecord(
                            payload=payload,
                            record_key=business_key(contract, payload),
                            extract_ts=str(payload["updated_at"]),
                            source_version=None,
                            locator=locator,
                        )
                    )
    return Extraction(tuple(records))


def extract_file_bytes(*, entity: str, filename: str, content: bytes, max_records: int = 50_000) -> Extraction:
    suffix = Path(filename).suffix.lower()
    expected = FILE_TYPES.get(entity)
    if expected is None:
        raise SourceError("unsupported_entity", filename, entity)
    if suffix != expected:
        raise SourceError("unsupported_file_type", filename, {"expected": expected, "actual": suffix})
    checksum = sha256_hex(content)
    try:
        decoded = content.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise SourceError("invalid_file_encoding", filename, {"content_sha256": checksum}) from error
    try:
        if suffix == ".csv":
            reader = csv.DictReader(io.StringIO(decoded))
            if reader.fieldnames is None:
                raise SourceError("invalid_source_schema", filename, "missing CSV header")
            raw_records: list[Any] = list(reader)
        else:
            parsed = json.loads(decoded)
            raw_records = parsed if isinstance(parsed, list) else [parsed]
    except json.JSONDecodeError as error:
        raise SourceError("malformed_json", filename, {"content_sha256": checksum, "preview": decoded[:512]}) from error
    if len(raw_records) > max_records:
        raise SourceError("max_records_exceeded", filename, {"max_records": max_records})
    records: list[SourceRecord] = []
    for index, item in enumerate(raw_records, start=1):
        locator = f"content_sha256:{checksum}#row={index}"
        if not isinstance(item, dict):
            raise SourceError("invalid_source_schema", locator, item)
        contract = validate_record(entity, item, locator)
        records.append(
            SourceRecord(
                payload=item,
                record_key=business_key(contract, item),
                extract_ts=str(item.get(contract.extract_timestamp)) if contract.extract_timestamp else None,
                source_version=checksum,
                locator=locator,
            )
        )
    return Extraction(tuple(records), checksum)


def extract_file_path(*, entity: str, path: str | Path, max_records: int = 50_000) -> Extraction:
    file_path = Path(path)
    if not file_path.is_file():
        raise SourceError("missing_expected_file", str(file_path), {"path": str(file_path)})
    return extract_file_bytes(entity=entity, filename=file_path.name, content=file_path.read_bytes(), max_records=max_records)
