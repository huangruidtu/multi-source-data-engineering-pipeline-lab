from __future__ import annotations

import json
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
from mdep_silver.manifest import ManifestValidationError, verify_committed_batch
from mdep_silver.spark_job import build_local_spark, create_tables, deduplicate, process_verified_batch, transform
from mdep_cdc.dms_adapter import DmsAdapter
from mdep_cdc.iceberg_writer import apply_iceberg_event, create_cdc_tables
from mdep_cdc.models import ApplyDecision


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

    def test_real_dms_cdc_to_iceberg_current_state_tombstone_and_replay(self):
        adapter = DmsAdapter()
        namespace = "cdc_correctness"
        create_cdc_tables(self.spark, namespace=namespace)

        def record(operation, data, lsn=None, txid=None, record_id=None, table="customers"):
            metadata = {
                "timestamp": "2026-11-06T10:00:01.000000Z",
                "record-type": "data",
                "operation": operation,
                "partition-key-type": "primary-key",
                "partition-key-value": str(next(iter(data.values()))),
                "schema-name": "commerce",
                "table-name": table,
                "commit-timestamp": "2026-11-06T10:00:00.000000Z",
            }
            if operation != "load":
                metadata.update({
                    "stream-position": lsn,
                    "transaction-id": txid,
                    "transaction-record-id": record_id,
                })
            return adapter.parse({"data": data, "metadata": metadata})

        customer = {
            "customer_id": "cust-cdc-1", "customer_name": "Ada", "email": "ada@example.test",
            "customer_status": "active", "created_at": "2026-11-01T08:00:00Z",
            "updated_at": "2026-11-01T08:00:00Z",
        }
        snapshot = record("load", customer)
        self.assertEqual(ApplyDecision.APPLIED_INSERT, apply_iceberg_event(
            self.spark, snapshot, namespace=namespace, observed_at=datetime(2026, 11, 6, 10, 1, tzinfo=UTC)
        ).decision)
        updated_data = {**customer, "customer_status": "suspended", "updated_at": "2026-11-06T10:02:00Z"}
        update = record("update", updated_data, "16/B6C60", 731, 1)
        self.assertEqual(ApplyDecision.APPLIED_UPDATE, apply_iceberg_event(
            self.spark, update, namespace=namespace, observed_at=datetime(2026, 11, 6, 10, 2, tzinfo=UTC)
        ).decision)
        self.assertEqual(ApplyDecision.DUPLICATE, apply_iceberg_event(
            self.spark, update, namespace=namespace, observed_at=datetime(2026, 11, 6, 11, 0, tzinfo=UTC)
        ).decision)

        stale_update = record("update", customer, "16/B6C50", 730, 1)
        stale_delete = record("delete", {"customer_id": "cust-cdc-1"}, "16/B6C55", 730, 2)
        self.assertEqual(ApplyDecision.STALE, apply_iceberg_event(self.spark, stale_update, namespace=namespace).decision)
        self.assertEqual(ApplyDecision.STALE, apply_iceberg_event(self.spark, stale_delete, namespace=namespace).decision)

        conflict = record("update", {**updated_data, "customer_status": "inactive"}, "16/B6C60", 731, 1)
        self.assertEqual(ApplyDecision.CONFLICT, apply_iceberg_event(self.spark, conflict, namespace=namespace).decision)
        table = f"local.{namespace}.core_customers"
        self.assertEqual("suspended", self.spark.table(table).first()["customer_status"])

        delete = record("delete", {"customer_id": "cust-cdc-1"}, "16/B6C70", 732, 1)
        self.assertEqual(ApplyDecision.APPLIED_DELETE, apply_iceberg_event(
            self.spark, delete, namespace=namespace, observed_at=datetime(2026, 11, 6, 10, 3, tzinfo=UTC)
        ).decision)
        self.assertEqual(ApplyDecision.DUPLICATE, apply_iceberg_event(self.spark, delete, namespace=namespace).decision)
        tombstone = self.spark.table(table).first().asDict()
        self.assertTrue(tombstone["is_deleted"])
        self.assertEqual("16/B6C70", tombstone["source_lsn"])
        self.assertEqual(("commerce", "customers", "cust-cdc-1", "delete"), (
            tombstone["source_schema"], tombstone["source_table"],
            tombstone["primary_key_value"], tombstone["dms_operation"],
        ))
        self.assertEqual(0, self.spark.table(table).where("not is_deleted").count())
        self.assertEqual(ApplyDecision.STALE, apply_iceberg_event(self.spark, update, namespace=namespace).decision)

        # Four entity-scoped canonical tables use the Gold-facing field names.
        fixtures = {
            "products": {
                "product_id": "prod-1", "product_name": "Keyboard", "category_code": "HW",
                "unit_price": "99.50", "currency": "DKK", "updated_at": "2026-11-06T10:00:00Z",
            },
            "orders": {
                "order_id": "order-1", "customer_id": "cust-2", "order_status": "created",
                "order_ts": "2026-11-06T10:00:00Z", "currency": "EUR", "order_total": "100.00",
                "updated_at": "2026-11-06T10:00:00Z",
            },
            "payments": {
                "payment_id": "pay-1", "order_id": "order-1", "payment_status": "authorized",
                "payment_ts": "2026-11-06T10:00:00Z", "amount": "100.00", "currency": "EUR",
                "authorization_code": "auth-1", "updated_at": "2026-11-06T10:00:00Z",
            },
        }
        for index, (entity, payload) in enumerate(fixtures.items(), 1):
            result = apply_iceberg_event(
                self.spark, record("insert", payload, f"16/B6D{index}0", 740 + index, 1, entity),
                namespace=namespace,
            )
            self.assertEqual(ApplyDecision.APPLIED_INSERT, result.decision)
            self.assertEqual(1, self.spark.table(f"local.{namespace}.core_{entity}").where("not is_deleted").count())

        quarantine = self.spark.table(f"local.{namespace}.cdc_quarantine")
        self.assertEqual(4, quarantine.count())
        self.assertEqual(4, quarantine.select("quarantine_id").distinct().count())
        snapshots = self.spark.sql(f"SELECT operation FROM {table}.snapshots").collect()
        self.assertGreaterEqual(len(snapshots), 3)
        self.assertTrue(all(row["operation"] in {"append", "overwrite"} for row in snapshots))
        print("AWS05_CDC_ICEBERG_EVIDENCE=" + json.dumps({
            "tombstone": {
                "customer_id": tombstone["customer_id"],
                "is_deleted": tombstone["is_deleted"],
                "source_lsn": tombstone["source_lsn"],
                "operation": tombstone["dms_operation"],
            },
            "live_rows": {
                entity: self.spark.table(f"local.{namespace}.core_{entity}").where("not is_deleted").count()
                for entity in ("customers", "products", "orders", "payments")
            },
            "quarantine_rows": quarantine.count(),
            "distinct_quarantine_ids": quarantine.select("quarantine_id").distinct().count(),
            "snapshot_operations": [row["operation"] for row in snapshots],
        }, sort_keys=True))

    def test_identical_duplicate_occurrences_keep_independent_evidence_on_replay(self):
        context = batch_context("locations", 8)
        identical = {
            "location_id": "loc-three",
            "location_name": "Three Identical Occurrences",
            "country_code": "DK",
            "timezone": "Europe/Copenhagen",
            "updated_at": "2026-11-08T08:00:00Z",
        }
        verified = publish(self.store, context, [identical, identical, identical])

        from pyspark.sql import functions as F

        checked = transform(self.spark.read.parquet(self.data_path(verified)), verified)
        for seed in (7, 19):
            shuffled = checked.orderBy(F.rand(seed)).repartition(3)
            winners, _rejected, duplicate_count = deduplicate(shuffled, "locations")
            self.assertEqual(2, duplicate_count)
            self.assertEqual("page=1,row=1", winners.select("source_locator").first()["source_locator"])

        first = process_verified_batch(
            self.spark,
            verified,
            self.data_path(verified),
            namespace="duplicate_occurrences",
        )
        quarantine = self.spark.table("local.duplicate_occurrences.silver_quarantine")
        evidence = quarantine.select("quarantine_id", "source_locator", "original_payload").collect()
        self.assertEqual(2, len(evidence))
        self.assertEqual(2, len({row["quarantine_id"] for row in evidence}))
        rejected_locators = {
            row["source_locator"]
            for row in evidence
        }
        self.assertEqual(2, len(rejected_locators))
        self.assertEqual({"page=1,row=2", "page=1,row=3"}, rejected_locators)
        self.assertTrue(all(row["source_locator"] == json.loads(row["original_payload"])["source_locator"] for row in evidence))
        self.assertEqual((3, 0, 2, 1, 1, 0, 0), (
            first.accepted_count,
            first.rejected_count,
            first.duplicate_count,
            first.unique_winner_count,
            first.inserted_count,
            first.updated_count,
            first.unchanged_count,
        ))
        silver = self.spark.table("local.duplicate_occurrences.ref_locations")
        self.assertEqual(1, silver.count())
        self.assertEqual("page=1,row=1", silver.select("source_locator").first()["source_locator"])

        replay = process_verified_batch(
            self.spark,
            verified,
            self.data_path(verified),
            namespace="duplicate_occurrences",
        )
        replay_evidence = self.spark.table("local.duplicate_occurrences.silver_quarantine")
        self.assertEqual(2, replay_evidence.count())
        self.assertEqual(2, replay_evidence.select("quarantine_id").distinct().count())
        self.assertEqual(
            {row["quarantine_id"] for row in evidence},
            {row["quarantine_id"] for row in replay_evidence.select("quarantine_id").collect()},
        )
        self.assertEqual((3, 0, 2, 1, 0, 0, 1), (
            replay.accepted_count,
            replay.rejected_count,
            replay.duplicate_count,
            replay.unique_winner_count,
            replay.inserted_count,
            replay.updated_count,
            replay.unchanged_count,
        ))
        self.assertEqual(1, self.spark.table("local.duplicate_occurrences.ref_locations").count())

    def test_real_bronze_manifest_to_iceberg_merge_replay_and_recovery(self):
        rates = [
            {"rate_date": "2026-11-01", "base_currency": "eur", "quote_currency": "dkk", "rate": "7.4500000001", "retrieved_at": "2026-11-01T05:00:00Z"},
            {"rate_date": "2026-11-01", "base_currency": "EUR", "quote_currency": "DKK", "rate": "7.4600000002", "retrieved_at": "2026-11-01T06:00:00Z", "provider_name": "ECB"},
            {"rate_date": "2026-11-01", "base_currency": "EUR", "quote_currency": "USD", "rate": "0", "retrieved_at": "2026-11-01T06:00:00Z"},
            {"rate_date": "2026-11-01", "base_currency": "EUR", "quote_currency": "EUR", "rate": "1", "retrieved_at": "2026-11-01T06:00:00Z"},
        ]
        first = publish(self.store, batch_context("exchange_rates", 1), rates)
        result = process_verified_batch(self.spark, first, self.data_path(first))
        self.assertEqual((4, 2, 2, 1, 1, 1, 0, 0), (
            result.input_count, result.accepted_count, result.rejected_count, result.duplicate_count,
            result.unique_winner_count, result.inserted_count, result.updated_count, result.unchanged_count,
        ))
        row = self.spark.sql("SELECT * FROM local.mdep_silver.ref_exchange_rates").first().asDict()
        self.assertEqual("ECB", row["provider_name"])
        self.assertIn('"provider_name":"ECB"', row["source_payload_json"])
        self.assertEqual("7.4600000002", str(row["rate"]))
        schema = {field.name: field.dataType.simpleString() for field in self.spark.table("local.mdep_silver.ref_exchange_rates").schema}
        self.assertEqual("decimal(20,10)", schema["rate"])
        snapshots_before_replay = self.spark.sql("SELECT * FROM local.mdep_silver.ref_exchange_rates.snapshots").count()

        replay = process_verified_batch(self.spark, first, self.data_path(first))
        self.assertEqual((0, 0, 1), (replay.inserted_count, replay.updated_count, replay.unchanged_count))
        self.assertEqual(3, self.spark.table("local.mdep_silver.silver_quarantine").count())
        self.assertEqual(1, self.spark.table("local.mdep_silver.processing_reconciliation").count())
        self.assertEqual(snapshots_before_replay, self.spark.sql("SELECT * FROM local.mdep_silver.ref_exchange_rates.snapshots").count())

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

        snapshots = self.spark.sql(
            "SELECT operation FROM local.mdep_silver.ref_exchange_rates.snapshots ORDER BY committed_at"
        ).collect()
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

        samples = [
            row.asDict()
            for row in self.spark.sql(
                "SELECT rate_date, base_currency, quote_currency, CAST(rate AS STRING) AS rate, provider_name "
                "FROM local.mdep_silver.ref_exchange_rates ORDER BY quote_currency"
            ).collect()
        ]
        evidence = {
            "first_reconciliation": json.loads(result.to_json()),
            "replay_reconciliation": json.loads(replay.to_json()),
            "newer_reconciliation": json.loads(updated.to_json()),
            "stale_reconciliation": json.loads(stale_result.to_json()),
            "silver_rows": samples,
            "snapshot_operations": [row["operation"] for row in snapshots],
            "quarantine_rows": self.spark.table("local.mdep_silver.silver_quarantine").count(),
        }
        print("AWS03_ICEBERG_EVIDENCE=" + json.dumps(evidence, default=str, sort_keys=True))

    def test_integrity_failures_stop_before_any_silver_write(self):
        create_tables(self.spark, "local", "mdep_silver")
        rows_before = self.spark.table("local.mdep_silver.ref_locations").count()
        root = Path(self.temp.name) / "integrity-objects"
        store = LocalObjectStore(root)
        orphan_context = batch_context("locations", 6)
        orphan = publish(store, orphan_context, [{
            "location_id": "loc-orphan", "location_name": "Orphan", "country_code": "DK",
            "timezone": "Europe/Copenhagen", "updated_at": "2026-11-06T08:00:00Z",
        }])
        Path(root, orphan.manifest_key).unlink()
        with self.assertRaisesRegex(ManifestValidationError, "manifest_unreadable"):
            verify_committed_batch(store, orphan.manifest_key)

        tampered_context = batch_context("locations", 7)
        tampered = publish(store, tampered_context, [{
            "location_id": "loc-tampered", "location_name": "Tampered", "country_code": "DK",
            "timezone": "Europe/Copenhagen", "updated_at": "2026-11-07T08:00:00Z",
        }])
        Path(root, tampered.data_key).write_bytes(b"not parquet")
        with self.assertRaisesRegex(ManifestValidationError, "unreadable"):
            verify_committed_batch(store, tampered.manifest_key)
        self.assertEqual(rows_before, self.spark.table("local.mdep_silver.ref_locations").count())
        print("AWS03_INTEGRITY_EVIDENCE=orphan_rejected,tampered_payload_rejected,no_silver_write")


if __name__ == "__main__":
    unittest.main()
