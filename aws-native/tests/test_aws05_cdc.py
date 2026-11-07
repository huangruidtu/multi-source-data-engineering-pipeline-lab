from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

AWS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(AWS_ROOT))

from mdep_cdc.archive import append_ndjson
from mdep_cdc.dms_adapter import DmsAdapter, DmsContractError
from mdep_cdc.models import ApplyDecision
from mdep_cdc.state_store import CdcStateStore


UTC = timezone.utc


CUSTOMER = {
    "customer_id": "cust-1",
    "customer_name": "Ada Lovelace",
    "email": "ada@example.test",
    "customer_status": "active",
    "created_at": "2026-11-01T08:00:00Z",
    "updated_at": "2026-11-01T08:00:00Z",
}


def dms_record(
    operation: str,
    data: dict,
    *,
    table: str = "customers",
    lsn: str | None = "00000016/B6C50",
    txid: int | None = 730,
    record_id: int | None = 1,
    commit: str = "2026-11-01T08:00:01.123456Z",
) -> dict:
    metadata = {
        "timestamp": "2026-11-01T08:00:02.000000Z",
        "record-type": "data",
        "operation": operation,
        "partition-key-type": "primary-key",
        "partition-key-value": str(next(iter(data.values()))),
        "schema-name": "commerce",
        "table-name": table,
        "commit-timestamp": commit,
    }
    if operation != "load":
        metadata.update(
            {
                "stream-position": lsn,
                "transaction-id": txid,
                "transaction-record-id": record_id,
                "prev-transaction-id": txid - 1 if txid else None,
                "prev-transaction-record-id": 1,
            }
        )
    return {"data": data, "metadata": metadata}


class DmsAdapterTests(unittest.TestCase):
    def setUp(self):
        self.adapter = DmsAdapter()

    def test_real_dms_shape_is_not_debezium_and_retains_raw_event(self):
        record = dms_record("insert", CUSTOMER)
        event = self.adapter.parse(json.dumps(record))
        self.assertEqual("customers", event.entity)
        self.assertEqual("cust-1", event.primary_key)
        self.assertEqual("insert", event.operation)
        self.assertEqual("00000016/B6C50", event.position.source_lsn)
        self.assertEqual(730, int(event.position.transaction_id))
        self.assertEqual(1, event.position.transaction_record_id)
        self.assertEqual(record, json.loads(event.raw_json))
        self.assertNotIn("before", json.loads(event.raw_json))
        self.assertNotIn("after", json.loads(event.raw_json))

    def test_missing_or_ambiguous_coordinates_fail_closed(self):
        for field in ("stream-position", "transaction-id", "transaction-record-id"):
            with self.subTest(field=field):
                record = dms_record("update", CUSTOMER)
                del record["metadata"][field]
                with self.assertRaises(DmsContractError):
                    self.adapter.parse(record)

    def test_control_unknown_table_and_partial_upsert_are_rejected(self):
        control = dms_record("insert", CUSTOMER)
        control["metadata"]["record-type"] = "control"
        with self.assertRaisesRegex(DmsContractError, "non_data_control_record"):
            self.adapter.parse(control)
        with self.assertRaisesRegex(DmsContractError, "unexpected_source_table"):
            self.adapter.parse(dms_record("insert", CUSTOMER, table="accounts"))
        with self.assertRaisesRegex(DmsContractError, "missing_business_fields"):
            self.adapter.parse(dms_record("update", {"customer_id": "cust-1"}))

    def test_delete_requires_only_the_primary_key(self):
        event = self.adapter.parse(dms_record("delete", {"customer_id": "cust-1"}))
        self.assertEqual("delete", event.operation)

    def test_postgres_lsn_comparison_is_numeric_not_lexicographic(self):
        earlier = self.adapter.parse(dms_record("insert", CUSTOMER, lsn="F/FFFFFFFF", txid=10))
        later = self.adapter.parse(dms_record("insert", CUSTOMER, lsn="10/0", txid=11))
        self.assertLess(earlier.position.lsn, later.position.lsn)


class PersistentStateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "state.db"
        self.adapter = DmsAdapter()
        self.now = datetime(2026, 11, 6, 12, tzinfo=UTC)

    def tearDown(self):
        self.temp.cleanup()

    def store(self):
        return CdcStateStore(self.path, clock=lambda: self.now)

    def event(self, operation: str, data: dict, **kwargs):
        return self.adapter.parse(dms_record(operation, data, **kwargs))

    def test_snapshot_update_delete_tombstone_restart_and_no_resurrection(self):
        snapshot = self.event("load", CUSTOMER)
        newer_data = {**CUSTOMER, "customer_status": "suspended", "updated_at": "2026-11-02T09:00:00Z"}
        update = self.event("update", newer_data, lsn="16/B6C60", txid=731)
        delete = self.event("delete", {"customer_id": "cust-1"}, lsn="16/B6C70", txid=732)
        stale_update = self.event("update", CUSTOMER, lsn="16/B6C65", txid=731)

        with self.store() as state:
            self.assertEqual(ApplyDecision.APPLIED_INSERT, state.apply(snapshot).decision)
            self.now += timedelta(seconds=5)
            self.assertEqual(ApplyDecision.APPLIED_UPDATE, state.apply(update).decision)
            self.now += timedelta(seconds=5)
            self.assertEqual(ApplyDecision.APPLIED_DELETE, state.apply(delete).decision)
            tombstone = state.row("customers", "cust-1")
            self.assertEqual(1, tombstone["is_deleted"])
            self.assertEqual("16/B6C70", tombstone["source_lsn"])
            self.assertEqual([], state.live_rows("customers"))

        with self.store() as restarted:
            self.assertEqual(ApplyDecision.DUPLICATE, restarted.apply(delete).decision)
            self.assertEqual(ApplyDecision.STALE, restarted.apply(stale_update).decision)
            self.assertEqual(1, restarted.row("customers", "cust-1")["is_deleted"])
            self.assertEqual(1, len(restarted.quarantine_rows()))

    def test_duplicates_stale_delete_equal_conflict_and_same_transaction_order(self):
        first = self.event("insert", CUSTOMER, lsn="16/B6C50", txid=730, record_id=1)
        next_in_tx_data = {**CUSTOMER, "customer_status": "inactive"}
        next_in_tx = self.event("update", next_in_tx_data, lsn="16/B6C50", txid=730, record_id=2)
        stale_delete = self.event("delete", {"customer_id": "cust-1"}, lsn="16/B6C40", txid=729)
        conflict_data = {**CUSTOMER, "customer_status": "suspended"}
        equal_conflict = self.event("update", conflict_data, lsn="16/B6C50", txid=730, record_id=2)

        with self.store() as state:
            self.assertEqual(ApplyDecision.APPLIED_INSERT, state.apply(first).decision)
            self.assertEqual(ApplyDecision.DUPLICATE, state.apply(first).decision)
            self.assertEqual(ApplyDecision.APPLIED_UPDATE, state.apply(next_in_tx).decision)
            self.assertEqual(ApplyDecision.STALE, state.apply(stale_delete).decision)
            self.assertEqual(ApplyDecision.CONFLICT, state.apply(equal_conflict).decision)
            payload = json.loads(state.row("customers", "cust-1")["payload_json"])
            self.assertEqual("inactive", payload["customer_status"])
            self.assertEqual(2, len(state.quarantine_rows()))
            self.assertEqual(2, len({row["quarantine_id"] for row in state.quarantine_rows()}))

    def test_equal_lsn_from_different_transactions_is_incomparable(self):
        with self.store() as state:
            self.assertEqual(
                ApplyDecision.APPLIED_INSERT,
                state.apply(self.event("insert", CUSTOMER, lsn="16/B6C50", txid=730)).decision,
            )
            changed = {**CUSTOMER, "customer_status": "inactive"}
            result = state.apply(self.event("update", changed, lsn="16/B6C50", txid=999))
            self.assertEqual(ApplyDecision.INCOMPARABLE, result.decision)

    def test_entity_scoped_keys_do_not_collide(self):
        product = {
            "product_id": "shared-id", "product_name": "Keyboard", "category_code": "HW",
            "unit_price": "99.50", "currency": "DKK", "updated_at": "2026-11-01T08:00:00Z",
        }
        customer = {**CUSTOMER, "customer_id": "shared-id"}
        with self.store() as state:
            state.apply(self.event("insert", customer))
            state.apply(self.adapter.parse(dms_record("insert", product, table="products")))
            self.assertEqual(1, len(state.live_rows("customers")))
            self.assertEqual(1, len(state.live_rows("products")))

    def test_actual_ingestion_time_is_distinct_from_source_commit_time(self):
        historical = self.event(
            "insert", CUSTOMER, commit="2024-01-01T00:00:00.000000Z", lsn="1/10", txid=10
        )
        with self.store() as state:
            state.apply(historical)
            row = state.row("customers", "cust-1")
            self.assertEqual("2024-01-01T00:00:00.000000Z", row["source_event_at"])
            self.assertEqual("2026-11-06T12:00:00Z", row["ingested_at"])
            self.assertEqual(row["ingested_at"], row["applied_at"])

    def test_load_after_cdc_cannot_overwrite_current_state(self):
        with self.store() as state:
            state.apply(self.event("insert", CUSTOMER, lsn="16/B6C50", txid=730))
            changed_snapshot = {**CUSTOMER, "customer_status": "inactive"}
            self.assertEqual(ApplyDecision.STALE, state.apply(self.event("load", changed_snapshot)).decision)

    def test_firehose_contract_is_newline_delimited_and_entity_partitioned(self):
        root = Path(self.temp.name) / "archive"
        record = dms_record("insert", CUSTOMER)
        path = append_ndjson(root, "customers", record)
        append_ndjson(root, "customers", dms_record("delete", {"customer_id": "cust-1"}))
        self.assertEqual(("bronze", "cdc", "customers", "records.ndjson"), path.relative_to(root).parts)
        lines = path.read_text(encoding="utf-8").splitlines()
        self.assertEqual(2, len(lines))
        self.assertTrue(all(json.loads(line)["metadata"]["record-type"] == "data" for line in lines))


if __name__ == "__main__":
    unittest.main()
