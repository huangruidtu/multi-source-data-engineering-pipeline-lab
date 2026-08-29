"""Deterministic local reference-data API for MDEP-7."""

from __future__ import annotations

import json
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


EXCHANGE_RATES = [
    {"rate_date": "2025-02-01", "base_currency": "EUR", "quote_currency": "DKK", "rate": 7.4560, "retrieved_at": "2025-02-01T06:00:00Z"},
    {"rate_date": "2025-02-01", "base_currency": "EUR", "quote_currency": "USD", "rate": 1.0380, "retrieved_at": "2025-02-01T06:00:00Z"},
    {"rate_date": "2025-02-02", "base_currency": "EUR", "quote_currency": "DKK", "rate": 7.4575, "retrieved_at": "2025-02-02T06:00:00Z"},
    {"rate_date": "2025-02-02", "base_currency": "EUR", "quote_currency": "USD", "rate": 1.0410, "retrieved_at": "2025-02-02T06:00:00Z"},
    {"rate_date": "2025-02-03", "base_currency": "EUR", "quote_currency": "DKK", "rate": 7.4580, "retrieved_at": "2025-02-03T06:00:00Z"},
]

LOCATIONS = [
    {"location_id": "loc-100", "location_name": "Copenhagen Hub", "country_code": "DK", "timezone": "Europe/Copenhagen", "updated_at": "2025-02-01T08:00:00Z"},
    {"location_id": "loc-200", "location_name": "Aarhus Depot", "country_code": "DK", "timezone": "Europe/Copenhagen", "updated_at": "2025-02-01T08:00:00Z"},
    {"location_id": "loc-300", "location_name": "Oslo Service Point", "country_code": "NO", "timezone": "Europe/Oslo", "updated_at": "2025-02-01T08:00:00Z"},
]


def paginate(records: list[dict], query: dict[str, list[str]]) -> dict:
    try:
        page = int(query.get("page", ["1"])[0])
        page_size = int(query.get("page_size", ["2"])[0])
    except ValueError as error:
        raise ValueError("page and page_size must be integers") from error
    if page < 1 or page_size < 1 or page_size > 100:
        raise ValueError("page must be >= 1 and page_size must be between 1 and 100")
    start = (page - 1) * page_size
    items = records[start : start + page_size]
    return {
        "items": items,
        "page": page,
        "page_size": page_size,
        "total_items": len(records),
        "next_page": page + 1 if start + page_size < len(records) else None,
    }


class SourceHandler(BaseHTTPRequestHandler):
    server_version = "MDEPSource/1.0"

    def _send_json(self, status: HTTPStatus, payload: dict, headers: dict[str, str] | None = None) -> None:
        encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        scenario = query.get("scenario", [""])[0]

        if scenario == "retryable":
            self._send_json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "retryable_failure", "message": "Retry this request after one second."}, {"Retry-After": "1"})
            return
        if scenario == "rate_limit":
            self._send_json(HTTPStatus.TOO_MANY_REQUESTS, {"error": "rate_limited", "message": "Retry this request after one second."}, {"Retry-After": "1"})
            return
        if scenario == "timeout":
            time.sleep(3)

        if parsed.path == "/health":
            self._send_json(HTTPStatus.OK, {"status": "ok"})
            return
        if parsed.path == "/v1/exchange-rates":
            records = EXCHANGE_RATES
        elif parsed.path == "/v1/locations":
            records = LOCATIONS
        else:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return

        try:
            self._send_json(HTTPStatus.OK, paginate(records, query))
        except ValueError as error:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "invalid_pagination", "message": str(error)})

    def log_message(self, format: str, *args: object) -> None:
        return


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8080), SourceHandler).serve_forever()
