import json
import sys
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path


AWS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(AWS_ROOT))

from mdep_ingestion.extractors import HttpResponse, extract_file_bytes, extract_file_path, extract_postgres, extract_rest
from mdep_ingestion.models import BatchContext, SourceError, SourceRecord, enrich_record


def context(source_name="rest_api", entity="exchange_rates"):
    return BatchContext(
        source_name=source_name,
        source_entity=entity,
        logical_date=date(2025, 2, 1),
        interval_start=datetime(2025, 2, 1, tzinfo=timezone.utc),
        interval_end=datetime(2025, 2, 2, tzinfo=timezone.utc),
        ingested_at=datetime(2025, 2, 2, 12, 30, tzinfo=timezone.utc),
    )


class QueueHttpClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, *, timeout):
        self.calls.append((url, timeout))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def page(items, next_page, status=200, headers=None):
    body = json.dumps({"items": items, "next_page": next_page}).encode()
    return HttpResponse(status, body, headers or {})


RATE = {
    "rate_date": "2025-02-01",
    "base_currency": "EUR",
    "quote_currency": "DKK",
    "rate": 7.45,
    "retrieved_at": "2025-02-01T06:00:00Z",
}


class BatchContextTests(unittest.TestCase):
    def test_identity_uses_source_entity_logical_date_and_half_open_interval(self):
        first = context()
        retry = context()
        changed = BatchContext(
            source_name=first.source_name,
            source_entity=first.source_entity,
            logical_date=first.logical_date,
            interval_start=first.interval_start,
            interval_end=datetime(2025, 2, 3, tzinfo=timezone.utc),
            ingested_at=first.ingested_at,
        )
        self.assertEqual(first.ingestion_id, retry.ingestion_id)
        self.assertNotEqual(first.ingestion_id, changed.ingestion_id)
        self.assertIn(first.ingestion_id, first.data_key)
        self.assertNotEqual(first.interval_end, first.ingested_at)
        self.assertEqual(datetime(2025, 2, 2, 12, 30, tzinfo=timezone.utc), first.ingested_at)

    def test_context_rejects_naive_or_empty_interval(self):
        with self.assertRaises(ValueError):
            BatchContext(
                "rest_api",
                "locations",
                date(2025, 2, 1),
                datetime(2025, 2, 1),
                datetime(2025, 2, 2),
                datetime(2025, 2, 2, 12, 30, tzinfo=timezone.utc),
            )
        with self.assertRaises(ValueError):
            BatchContext(
                "rest_api",
                "locations",
                date(2025, 2, 1),
                datetime(2025, 2, 1, tzinfo=timezone.utc),
                datetime(2025, 2, 1, tzinfo=timezone.utc),
                datetime(2025, 2, 2, 12, 30, tzinfo=timezone.utc),
            )

    def test_enrichment_preserves_business_fields_and_is_retry_stable(self):
        source = SourceRecord(RATE, "2025-02-01|EUR|DKK", RATE["retrieved_at"], '"v1"', "page=1#item=0")
        first = enrich_record(source, context())
        second = enrich_record(source, context())
        self.assertEqual(first, second)
        self.assertEqual(7.45, first["rate"])
        self.assertEqual(context().ingestion_id, first["ingestion_id"])
        self.assertEqual(64, len(first["record_hash"]))


class RestExtractorTests(unittest.TestCase):
    def test_successful_pagination_and_retry_429(self):
        client = QueueHttpClient(
            [
                HttpResponse(429, b"{}", {"Retry-After": "0"}),
                page([RATE], 2, headers={"ETag": '"p1"'}),
                page([{**RATE, "quote_currency": "USD"}], None, headers={"ETag": '"p2"'}),
            ]
        )
        sleeps = []
        result = extract_rest(entity="exchange_rates", url="https://example.test/rates?page_size=1", client=client, sleep=sleeps.append)
        self.assertEqual(2, len(result.records))
        self.assertEqual('"p1","p2"', result.source_version)
        self.assertEqual([0.0], sleeps)
        self.assertTrue(client.calls[0][0].endswith("page_size=1&page=1"))

    def test_page_three_permanent_failure_returns_no_partial_extraction(self):
        client = QueueHttpClient([page([RATE], 2), page([{**RATE, "quote_currency": "USD"}], 3), HttpResponse(400, b"{}", {})])
        with self.assertRaisesRegex(SourceError, "rest_non_retryable_http"):
            extract_rest(entity="exchange_rates", url="https://example.test/rates", client=client)

    def test_retryable_5xx_is_bounded(self):
        client = QueueHttpClient([HttpResponse(503, b"{}", {})] * 3)
        with self.assertRaisesRegex(SourceError, "rest_retry_exhausted"):
            extract_rest(entity="exchange_rates", url="https://example.test/rates", client=client, max_retries=2, sleep=lambda _delay: None)
        self.assertEqual(3, len(client.calls))

    def test_malformed_json_invalid_marker_loop_and_schema_are_rejected(self):
        cases = [
            ([HttpResponse(200, b"{bad", {})], "malformed_json"),
            ([page([RATE], "two")], "invalid_next_page"),
            ([page([RATE], 1)], "pagination_loop"),
            ([page([{"rate_date": "2025-02-01"}], None)], "invalid_source_schema"),
        ]
        for responses, reason in cases:
            with self.subTest(reason=reason), self.assertRaisesRegex(SourceError, reason):
                extract_rest(entity="exchange_rates", url="https://example.test/rates", client=QueueHttpClient(responses))

    def test_max_pages_and_records_are_bounded(self):
        with self.assertRaisesRegex(SourceError, "max_records_exceeded"):
            extract_rest(entity="exchange_rates", url="https://example.test/rates", client=QueueHttpClient([page([RATE, RATE], None)]), max_records=1)
        with self.assertRaisesRegex(SourceError, "max_pages_exceeded"):
            extract_rest(entity="exchange_rates", url="https://example.test/rates", client=QueueHttpClient([page([RATE], 2)]), max_pages=1)


class FileExtractorTests(unittest.TestCase):
    def test_csv_and_json_success_and_rename_identity(self):
        csv_content = b"category_code,category_name,sort_order\nELEC,Electronics,1\n"
        first = extract_file_bytes(entity="product_categories", filename="categories.csv", content=csv_content)
        renamed = extract_file_bytes(entity="product_categories", filename="renamed.csv", content=csv_content)
        self.assertEqual(first, renamed)
        json_content = json.dumps(
            [{"device_id": "dev-1", "location_id": "loc-1", "device_type": "sensor", "device_status": "active", "updated_at": "2025-02-01T00:00:00Z"}]
        ).encode()
        self.assertEqual(1, len(extract_file_bytes(entity="devices", filename="devices.json", content=json_content).records))

    def test_missing_unsupported_malformed_and_invalid_schema_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory, self.assertRaisesRegex(SourceError, "missing_expected_file"):
            extract_file_path(entity="devices", path=Path(directory) / "missing.json")
        with self.assertRaisesRegex(SourceError, "unsupported_file_type"):
            extract_file_bytes(entity="devices", filename="devices.csv", content=b"x\n1\n")
        with self.assertRaisesRegex(SourceError, "malformed_json"):
            extract_file_bytes(entity="devices", filename="devices.json", content=b"[{bad")
        with self.assertRaisesRegex(SourceError, "invalid_source_schema"):
            extract_file_bytes(entity="product_categories", filename="categories.csv", content=b"category_code\nELEC\n")


class FakeColumn:
    def __init__(self, name):
        self.name = name


class FakeCursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.description = [FakeColumn("customer_id"), FakeColumn("customer_name"), FakeColumn("customer_status"), FakeColumn("created_at"), FakeColumn("updated_at")]
        self.executions = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, parameters=()):
        self.executions.append((query, parameters))

    def fetchmany(self, size):
        result, self.rows = self.rows[:size], self.rows[size:]
        return result


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self):
        return self._cursor


class PostgresExtractorTests(unittest.TestCase):
    def test_read_only_allowlisted_snapshot_and_bound_interval(self):
        row = ("cust-1", "Ada", "active", "2025-01-01T00:00:00Z", "2025-02-01T01:00:00Z")
        cursor = FakeCursor([row])
        result = extract_postgres(context=context("postgresql", "customers"), table="customers", connect=lambda: FakeConnection(cursor), incremental_by_updated_at=True)
        self.assertEqual(1, len(result.records))
        self.assertEqual(("SET TRANSACTION READ ONLY", ()), cursor.executions[0])
        query, parameters = cursor.executions[1]
        self.assertIn("updated_at >= %s AND updated_at < %s", query)
        self.assertIn("ORDER BY customer_id", query)
        self.assertEqual(context().interval_start, parameters[0])
        self.assertEqual(context().interval_end, parameters[1])

    def test_table_allowlist_rejects_identifier_injection(self):
        with self.assertRaisesRegex(SourceError, "unsupported_postgres_table"):
            extract_postgres(context=context("postgresql", "customers"), table="customers; DROP TABLE customers", connect=lambda: None)


if __name__ == "__main__":
    unittest.main()
