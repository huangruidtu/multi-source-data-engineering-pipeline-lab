import json
import sys
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path


AWS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(AWS_ROOT))

from mdep_ingestion.extractors import HttpResponse
from mdep_ingestion.models import BatchContext, Status
from mdep_ingestion.object_store import LocalObjectStore
from mdep_ingestion.pipeline import IngestionPipeline
from mdep_ingestion.publication import BronzePublisher, QuarantinePublisher
from mdep_ingestion.serialization import read_parquet


def ctx(source, entity):
    return BatchContext(
        source,
        entity,
        date(2025, 2, 1),
        datetime(2025, 2, 1, tzinfo=timezone.utc),
        datetime(2025, 2, 2, tzinfo=timezone.utc),
        datetime(2025, 2, 2, 12, 30, tzinfo=timezone.utc),
    )


class OnePageClient:
    def __init__(self, responses):
        self.responses = list(responses)

    def get(self, _url, *, timeout):
        del timeout
        return self.responses.pop(0)


class Column:
    def __init__(self, name):
        self.name = name


class Cursor:
    description = [Column("customer_id"), Column("customer_name"), Column("customer_status"), Column("created_at"), Column("updated_at")]

    def __init__(self):
        self.rows = [("cust-1", "Ada", "active", "2025-01-01T00:00:00Z", "2025-02-01T00:00:00Z")]

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, *_args):
        return None

    def fetchmany(self, _size):
        rows, self.rows = self.rows, []
        return rows


class Connection:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self):
        return Cursor()


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = LocalObjectStore(self.temp.name)
        self.pipeline = IngestionPipeline(BronzePublisher(self.store), QuarantinePublisher(self.store))

    def tearDown(self):
        self.temp.cleanup()

    def test_successful_postgres_rest_csv_and_json_paths_produce_parquet(self):
        outcomes = [self.pipeline.postgres(ctx("postgresql", "customers"), table="customers", connect=Connection)]
        rest_body = json.dumps(
            {
                "items": [{"location_id": "loc-1", "location_name": "Copenhagen", "country_code": "DK", "timezone": "Europe/Copenhagen", "updated_at": "2025-02-01T00:00:00Z"}],
                "next_page": None,
            }
        ).encode()
        outcomes.append(self.pipeline.rest(ctx("rest_api", "locations"), url="https://example.test/locations", client=OnePageClient([HttpResponse(200, rest_body, {})])))
        outcomes.append(
            self.pipeline.file_bytes(
                ctx("csv_file", "product_categories"),
                filename="product_categories.csv",
                content=b"category_code,category_name,sort_order\nELEC,Electronics,1\n",
            )
        )
        outcomes.append(
            self.pipeline.file_bytes(
                ctx("json_file", "devices"),
                filename="devices.json",
                content=json.dumps(
                    [{"device_id": "dev-1", "location_id": "loc-1", "device_type": "sensor", "device_status": "active", "updated_at": "2025-02-01T00:00:00Z"}]
                ).encode(),
            )
        )
        self.assertTrue(all(outcome.status is Status.PUBLISHED for outcome in outcomes))
        for outcome in outcomes:
            self.assertGreater(len(read_parquet(self.store.get_bytes(outcome.data_key))), 0)

    def test_duplicate_renamed_file_is_identical_replay(self):
        content = b"category_code,category_name,sort_order\nELEC,Electronics,1\n"
        first = self.pipeline.file_bytes(ctx("csv_file", "product_categories"), filename="first.csv", content=content)
        renamed = self.pipeline.file_bytes(ctx("csv_file", "product_categories"), filename="renamed.csv", content=content)
        self.assertEqual(Status.PUBLISHED, first.status)
        self.assertEqual(Status.ALREADY_PUBLISHED, renamed.status)

    def test_historical_backfill_separates_interval_source_and_ingestion_times(self):
        historical = BatchContext(
            "rest_api",
            "locations",
            date(2020, 1, 1),
            datetime(2020, 1, 1, tzinfo=timezone.utc),
            datetime(2020, 1, 2, tzinfo=timezone.utc),
            datetime(2026, 11, 6, 10, 15, tzinfo=timezone.utc),
        )
        item = {
            "location_id": "loc-old",
            "location_name": "Historical",
            "country_code": "DK",
            "timezone": "Europe/Copenhagen",
            "updated_at": "2020-01-01T08:00:00Z",
        }
        body = json.dumps({"items": [item], "next_page": None}).encode()
        first = self.pipeline.rest(historical, url="https://example.test/locations", client=OnePageClient([HttpResponse(200, body, {})]))
        retry = self.pipeline.rest(historical, url="https://example.test/locations", client=OnePageClient([HttpResponse(200, body, {})]))
        landed = read_parquet(self.store.get_bytes(first.data_key))[0]
        self.assertEqual(Status.PUBLISHED, first.status)
        self.assertEqual(Status.ALREADY_PUBLISHED, retry.status)
        self.assertEqual("2020-01-01T08:00:00Z", landed["source_extract_ts"])
        self.assertEqual("2026-11-06T10:15:00Z", landed["ingested_at"])
        self.assertEqual("2020-01-01", historical.logical_date.isoformat())

    def test_page_three_failure_has_no_canonical_publication(self):
        rate = {"rate_date": "2025-02-01", "base_currency": "EUR", "quote_currency": "DKK", "rate": 7.45, "retrieved_at": "2025-02-01T00:00:00Z"}
        client = OnePageClient(
            [
                HttpResponse(200, json.dumps({"items": [rate], "next_page": 2}).encode(), {}),
                HttpResponse(200, json.dumps({"items": [{**rate, "quote_currency": "USD"}], "next_page": 3}).encode(), {}),
                HttpResponse(400, b"{}", {}),
            ]
        )
        outcome = self.pipeline.rest(ctx("rest_api", "exchange_rates"), url="https://example.test/rates", client=client)
        self.assertEqual(Status.FAILED, outcome.status)
        self.assertIsNotNone(outcome.quarantine_key)
        self.assertEqual([], self.store.list_keys("bronze/"))

    def test_malformed_file_is_quarantined_with_original_reference(self):
        outcome = self.pipeline.file_bytes(ctx("json_file", "devices"), filename="devices.json", content=b"[{bad")
        self.assertEqual(Status.FAILED, outcome.status)
        evidence = json.loads(self.store.get_bytes(outcome.quarantine_key))
        self.assertEqual("malformed_json", evidence["rejection_reason"])
        self.assertEqual("devices.json", evidence["source_locator"])
        self.assertIn("content_sha256", evidence["original_payload_or_reference"])


if __name__ == "__main__":
    unittest.main()
