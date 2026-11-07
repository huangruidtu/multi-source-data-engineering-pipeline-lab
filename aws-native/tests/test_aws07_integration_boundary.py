import sys
import tempfile
import unittest
from pathlib import Path


AWS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(AWS_ROOT))

from integration.run_demo import _ingest
from mdep_ingestion.models import Status
from mdep_ingestion.object_store import LocalObjectStore
from mdep_silver.manifest import verify_committed_batch


class Aws07IntegrationBoundaryTests(unittest.TestCase):
    def test_reference_input_uses_aws02_rest_pipeline_with_replay_and_conflict(self):
        rate = {
            "rate_date": "2026-11-01",
            "base_currency": "EUR",
            "quote_currency": "DKK",
            "rate": "7.5000000000",
            "retrieved_at": "2026-11-01T06:00:00Z",
            "provider_name": "ECB",
        }
        with tempfile.TemporaryDirectory() as directory:
            store = LocalObjectStore(directory)
            published, requests = _ingest(store, "exchange_rates", 1, 8, [rate])
            replay, _ = _ingest(store, "exchange_rates", 1, 8, [rate])
            conflict, _ = _ingest(store, "exchange_rates", 1, 8, [{**rate, "rate": "7.55"}])

            self.assertEqual(Status.PUBLISHED, published.status)
            self.assertEqual(Status.ALREADY_PUBLISHED, replay.status)
            self.assertEqual(Status.CONFLICT, conflict.status)
            self.assertEqual(["https://aws07-fixture.invalid/exchange_rates?page=1"], requests)
            verified = verify_committed_batch(store, str(published.manifest_key))
            self.assertEqual(1, verified.record_count)
            self.assertEqual(published.data_key, verified.data_key)


if __name__ == "__main__":
    unittest.main()
