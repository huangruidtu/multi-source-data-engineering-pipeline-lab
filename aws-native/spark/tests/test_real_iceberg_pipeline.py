from __future__ import annotations

import tempfile
import sys
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

AWS_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(AWS_ROOT))

from mdep_ingestion.models import BatchContext, SourceRecord, enrich_record
from mdep_ingestion.object_store import LocalObjectStore
from mdep_ingestion.publication import BronzePublisher
from mdep_ingestion.serialization import to_parquet
from mdep_silver.manifest import verify_committed_batch
from mdep_silver.spark_job import build_local_spark, process_verified_batch


UTC = timezone.utc


def batch_context(entity: str, day: int, *, ingested_hour: int = 1) -> BatchContext:
    start = datetime(2026, 11, day, tzinfo=UTC)
    return BatchContext("rest_api", entity, start.date(), start, start + timedelta(days=1), start + timedelta(days=1, hours=ingested_hour))


def source(payload: dict, index: int) -> SourceRecord:
    entity_key = payload.get("location_id") or "|".join(str(payload.get(key)) for key in ("rate_date", "base_currency", "quote_currency"))
    extract = payload.get("updated_at") or payload.get("retrieved_at")
    return SourceRecord(payload, entity_key, extract, extract, f"page=1,row={index}")


def publish(store: LocalObjectStore, context: BatchContext, payloads: list[dict]):
    rows = [enrich_record(source(payload, index), context) for index, payload in enumerate(payloads, 1)]
    result = BronzePublisher(store).publish(context, to_parquet(rows))
    if result.status.value != "published":
        raise AssertionError(result)
    return verify_committed_batch(store, context.manifest_key)


class RealIcebergPipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        root = Path(cls.temp.name)
        cls.store = LocalObjectStore(root / "objects")
        cls.spark = build_local_spark((root / "warehouse").resolve().as_uri())
        cls.spark.sparkContext.setLogLevel("WARN")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.temp.cleanup()

    def data_path(self, verified) -> str:
        return str((self.store.root / verified.data_key).resolve())

    def test_real_bronze_manifest_to_iceberg_merge_replay_and_recovery(self):
        rates = [
            {"rate_date": "2026-11-01", "base_currency": "eur", "quote_currency": "dkk", "rate": "7.4500000001", "retrieved_at": "2026-11-01T05:00:00Z"},
            {"rate_date": "2026-11-01", "base_currency": "EUR", "quote_currency": "DKK", "rate": "7.4600000002", "retrieved_at": "2026-11-01T06:00:00Z", "provider_name": "ECB"},
            {"rate_date": "2026-11-01", "base_currency": "EUR", "quote_currency": "USD", "rate": "0", "retrieved_at": "2026-11-01T06:00:00Z"},
            {"rate_date": "2026-11-01", "base_currency": "EUR", "quote_currency": "EUR", "rate": "1", "retrieved_at": "2026-11-01T06:00:00Z"},
        ]
        first = publish(self.store, batch_context("exchange_rates", 1), rates)
        result = process_verified_batch(self.spark, first, self.data_path(first))
        self.assertEqual((4, 1, 3, 1, 1, 1, 0, 0), (
            result.input_count, result.accepted_count, result.rejected_count, result.duplicate_count,
            result.unique_winner_count, result.inserted_count, result.updated_count, result.unchanged_count,
        ))
        row = self.spark.sql("SELECT * FROM local.mdep_silver.ref_exchange_rates").first().asDict()
        self.assertEqual("ECB", row["provider_name"])
        self.assertIn('"provider_name":"ECB"', row["source_payload_json"])
        self.assertEqual("7.4600000002", str(row["rate"]))

        replay = process_verified_batch(self.spark, first, self.data_path(first))
        self.assertEqual((0, 0, 1), (replay.inserted_count, replay.updated_count, replay.unchanged_count))
        self.assertEqual(3, self.spark.table("local.mdep_silver.silver_quarantine").count())
        self.assertEqual(1, self.spark.table("local.mdep_silver.processing_reconciliation").count())

        newer = publish(self.store, batch_context("exchange_rates", 2), [
            {
                "rate_date": "2026-11-01", "base_currency": "EUR", "quote_currency": "DKK", "rate": "7.4700000003",
                "retrieved_at": "2026-11-02T06:00:00Z",
            },
            {
                "rate_date": "2026-11-01", "base_currency": "EUR", "quote_currency": "USD", "rate": "1.0800000000",
                "retrieved_at": "2026-11-02T06:00:00Z",
            },
        ])
        updated = process_verified_batch(self.spark, newer, self.data_path(newer))
        self.assertEqual((1, 1, 0), (updated.inserted_count, updated.updated_count, updated.unchanged_count))

        equal_version_conflict = publish(self.store, batch_context("exchange_rates", 3), [{
            "rate_date": "2026-11-01", "base_currency": "EUR", "quote_currency": "DKK", "rate": "7.4800000004",
            "retrieved_at": "2026-11-02T06:00:00Z",
        }])
        equal_result = process_verified_batch(self.spark, equal_version_conflict, self.data_path(equal_version_conflict))
        self.assertEqual((0, 1, 0), (equal_result.inserted_count, equal_result.updated_count, equal_result.unchanged_count))

        stale = publish(self.store, batch_context("exchange_rates", 4), [{
            "rate_date": "2026-11-01", "base_currency": "EUR", "quote_currency": "DKK", "rate": "9.9999999999",
            "retrieved_at": "2026-10-31T06:00:00Z",
        }])
        stale_result = process_verified_batch(self.spark, stale, self.data_path(stale))
        self.assertEqual((0, 0, 1), (stale_result.inserted_count, stale_result.updated_count, stale_result.unchanged_count))
        final_rate = self.spark.sql("SELECT CAST(rate AS STRING) rate FROM local.mdep_silver.ref_exchange_rates WHERE quote_currency = 'DKK'").first()["rate"]
        self.assertEqual("7.4800000004", final_rate)
        self.assertEqual(2, self.spark.table("local.mdep_silver.ref_exchange_rates").count())

        snapshots = self.spark.sql("SELECT operation FROM local.mdep_silver.ref_exchange_rates.snapshots").collect()
        self.assertGreaterEqual(len(snapshots), 2)
        self.assertTrue(all(row["operation"] in {"append", "overwrite"} for row in snapshots))

        locations = publish(self.store, batch_context("locations", 5), [
            {"location_id": "loc-1", "location_name": "Copenhagen", "country_code": "DK", "timezone": "Europe/Copenhagen", "updated_at": "2026-11-05T08:00:00Z", "location_type": "hub"},
            {"location_id": "loc-x", "location_name": "Unknown", "country_code": "XX", "timezone": "Etc/UTC", "updated_at": "2026-11-05T08:00:00Z"},
        ])
        calls = []

        def failed_ack(reconciliation):
            calls.append(reconciliation)
            raise RuntimeError("simulated acknowledgement failure after Iceberg commit")

        with self.assertRaisesRegex(RuntimeError, "acknowledgement"):
            process_verified_batch(self.spark, locations, self.data_path(locations), acknowledge=failed_ack)
        recovered = process_verified_batch(self.spark, locations, self.data_path(locations), acknowledge=calls.append)
        self.assertEqual((0, 0, 1), (recovered.inserted_count, recovered.updated_count, recovered.unchanged_count))
        self.assertEqual(1, self.spark.table("local.mdep_silver.ref_locations").count())
        self.assertEqual("hub", self.spark.table("local.mdep_silver.ref_locations").first()["location_type"])
        self.assertEqual(2, len(calls))


if __name__ == "__main__":
    unittest.main()
