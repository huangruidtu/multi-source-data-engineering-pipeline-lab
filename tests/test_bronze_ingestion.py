import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.error import HTTPError

from ingestion.batch.bronze import BatchContext, BronzePublisher, LocalObjectStore, bronze_key, enrich_record, quarantine_record
from ingestion.batch.extractors import fetch_paginated_json, file_identity


class BronzeTests(unittest.TestCase):
    def setUp(self):
        self.context = BatchContext(
            date(2025, 2, 1),
            datetime(2025, 2, 1, tzinfo=timezone.utc),
            datetime(2025, 2, 2, tzinfo=timezone.utc),
            "rest_api", "exchange_rates",
        )

    def test_path_and_metadata_are_deterministic(self):
        self.assertEqual(bronze_key(self.context), bronze_key(self.context))
        record = enrich_record({"rate": 7.456}, self.context, None, "2025-02-01T06:00:00Z", None, "page=1", ingested_at="2025-02-01T01:00:00Z")
        self.assertEqual(record["ingestion_id"], self.context.ingestion_id)
        self.assertEqual(record["record_hash"], enrich_record({"rate": 7.456}, self.context, None, None, None, "other", ingested_at="x")["record_hash"])

    def test_quarantine_preserves_evidence(self):
        rejected = quarantine_record(self.context, "malformed_json", "file.json", "{bad")
        self.assertEqual(rejected["rejection_reason"], "malformed_json")
        self.assertEqual(rejected["original_payload"], "{bad")

    def test_duplicate_file_identity_is_content_based(self):
        with tempfile.TemporaryDirectory() as directory:
            first, second = Path(directory) / "a.csv", Path(directory) / "renamed.csv"
            first.write_text("x\n1\n", encoding="utf-8")
            second.write_text("x\n1\n", encoding="utf-8")
            self.assertEqual(file_identity(first), file_identity(second))

    def test_pagination_retries_a_429_then_returns_all_pages(self):
        responses = [HTTPError("http://source", 429, "slow", {"Retry-After": "0"}, None)]
        class Response:
            def __init__(self, payload): self.payload = payload
            def read(self): return json.dumps(self.payload).encode()
            def __enter__(self): return self
            def __exit__(self, *_): return False
        responses.extend([Response({"items": [{"id": 1}], "next_page": 2}), Response({"items": [{"id": 2}], "next_page": None})])
        def opener(*_args, **_kwargs):
            response = responses.pop(0)
            if isinstance(response, Exception):
                response.close()
                raise response
            return response
        self.assertEqual([row["id"] for row in fetch_paginated_json("http://source/v1/items", opener=opener)], [1, 2])

    @unittest.skipUnless(__import__("importlib").util.find_spec("pyarrow"), "PyArrow not installed")
    def test_publication_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            publication = BronzePublisher(LocalObjectStore(directory))
            records = [enrich_record({"id": "one"}, self.context, "one", None, None, "test", ingested_at="2025-02-01T00:00:00Z")]
            self.assertEqual(publication.publish(self.context, records).status, "published")
            self.assertEqual(publication.publish(self.context, records).status, "already_published")

    @unittest.skipUnless(__import__("importlib").util.find_spec("pyarrow"), "PyArrow not installed")
    def test_file_ingestion_is_source_aligned_and_quarantines_bad_inputs(self):
        from unittest.mock import patch
        from ingestion.batch.pipeline import land_files
        with tempfile.TemporaryDirectory() as directory, patch.dict("os.environ", {"BRONZE_LOCAL_ROOT": directory}):
            result = land_files("2025-02-01")
            self.assertEqual(len(result["publications"]), 2)
            self.assertEqual(len(result["quarantine_keys"]), 5)
            self.assertTrue(any("product_categories" in item["key"] for item in result["publications"]))


if __name__ == "__main__":
    unittest.main()
