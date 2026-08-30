"""Small source-specific extractors. They return source records; no Silver logic lives here."""
from __future__ import annotations

import csv
import hashlib
import json
import time
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse, parse_qsl, urlunparse
from urllib.request import urlopen


RETRYABLE_HTTP = {429, 500, 502, 503, 504}


def fetch_paginated_json(url: str, retries: int = 2, opener: Callable = urlopen) -> list[dict[str, Any]]:
    """Read all pages before publication, so a failed page creates no partial Bronze object."""
    records: list[dict[str, Any]] = []
    page = 1
    while page is not None:
        parsed = urlparse(url)
        query = dict(parse_qsl(parsed.query))
        query["page"] = str(page)
        request_url = urlunparse(parsed._replace(query=urlencode(query)))
        for attempt in range(retries + 1):
            try:
                with opener(request_url, timeout=10) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                records.extend(payload["items"])
                page = payload["next_page"]
                break
            except HTTPError as error:
                if error.code not in RETRYABLE_HTTP or attempt == retries:
                    raise RuntimeError(f"REST extraction failed for {request_url}: HTTP {error.code}") from error
                time.sleep(int(error.headers.get("Retry-After", "1")))
            except URLError as error:
                if attempt == retries:
                    raise RuntimeError(f"REST extraction failed for {request_url}: {error.reason}") from error
                time.sleep(1)
    return records


def file_identity(path: str | Path) -> str:
    content = Path(path).read_bytes()
    return hashlib.sha256(content).hexdigest()


def read_csv(path: str | Path) -> list[dict[str, str]]:
    with Path(path).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def read_json(path: str | Path) -> list[dict[str, Any]]:
    parsed = json.loads(Path(path).read_text(encoding="utf-8"))
    return parsed if isinstance(parsed, list) else [parsed]


def postgres_rows(connect: Callable, table: str, interval_start: str | None = None, interval_end: str | None = None) -> list[dict[str, Any]]:
    """Full snapshot unless a bounded updated_at incremental exercise is requested."""
    if table not in {"customers", "products", "orders", "payments"}:
        raise ValueError(f"Unsupported PostgreSQL source table: {table}")
    query = f"SELECT * FROM commerce.{table}"
    parameters: tuple[str, ...] = ()
    if interval_start and interval_end:
        query += " WHERE updated_at >= %s AND updated_at < %s"
        parameters = (interval_start, interval_end)
    with connect() as connection, connection.cursor() as cursor:
        cursor.execute(query, parameters)
        columns = [column.name for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
