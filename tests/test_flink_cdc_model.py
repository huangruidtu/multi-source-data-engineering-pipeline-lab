import json
import unittest

from processing.flink.cdc_model import VersionDecision, apply_current_state, key_identity, parse_debezium, parse_lsn, version_decision


def event(entity="customers", primary="c1", op="u", lsn="0/10", partition=0, offset=1, value=None, transaction=None, snapshot=None):
    key_name = {"customers": "customer_id", "products": "product_id", "orders": "order_id", "payments": "payment_id"}[entity]
    after = value if value is not None else ({key_name: primary, "preferred_language": "en"} if op != "d" else None)
    source = {"lsn": lsn, "txId": 7, "ts_ms": "2025-01-01T00:00:00Z"}
    if snapshot is not None:
        source["snapshot"] = snapshot
    payload = {"op": op, "before": {key_name: primary}, "after": after, "source": source}
    if transaction is not None:
        payload["transaction"] = transaction
    return parse_debezium(json.dumps({key_name: primary}), json.dumps(payload), f"mdep.commerce.{entity}", partition, offset)


class FlinkCdcTests(unittest.TestCase):
    def test_lsn_and_snapshot_envelope(self):
        self.assertGreater(parse_lsn("0/11"), parse_lsn("0/10"))
        snapshot = event(op="r", snapshot="last", transaction=None)
        self.assertTrue(snapshot.snapshot)
        self.assertIsNone(snapshot.transaction_id)
        self.assertEqual("upserted", apply_current_state({}, {}, snapshot))

    def test_higher_lsn_accepts_and_lower_lsn_rejects(self):
        state, versions = {}, {}
        self.assertEqual("upserted", apply_current_state(state, versions, event(lsn="0/10")))
        self.assertEqual("upserted", apply_current_state(state, versions, event(lsn="0/11", offset=2)))
        self.assertEqual("lower_lsn_ignored", apply_current_state(state, versions, event(lsn="0/F", offset=9)))

    def test_same_transaction_higher_total_order_is_newer(self):
        first = event(transaction={"id": "tx-1", "total_order": "10", "data_collection_order": "1"})
        later = event(transaction={"id": "tx-1", "total_order": "11", "data_collection_order": "2"}, partition=7, offset=3)
        self.assertEqual(VersionDecision.NEWER, version_decision(later, first))

    def test_same_transaction_lower_total_order_is_stale(self):
        first = event(transaction={"id": "tx-1", "total_order": 11})
        earlier = event(transaction={"id": "tx-1", "total_order": 10}, partition=7, offset=3)
        self.assertEqual(VersionDecision.LOWER_TRANSACTION_ORDER, version_decision(earlier, first))

    def test_same_transaction_same_order_known_transport_is_exact_replay(self):
        first = event(transaction={"id": "tx-1", "total_order": 10})
        replay = event(transaction={"id": "tx-1", "total_order": 10})
        self.assertEqual(VersionDecision.EXACT_REPLAY, version_decision(replay, first))

    def test_unknown_transport_is_not_an_exact_replay(self):
        first = event(partition=None, offset=None, transaction={"id": "tx-1", "total_order": 10})
        unresolved = event(partition=None, offset=None, transaction={"id": "tx-1", "total_order": 10})
        self.assertEqual(VersionDecision.EQUAL_POSITION_CONFLICT, version_decision(unresolved, first))

    def test_same_lsn_without_transaction_or_transport_is_conservative_conflict(self):
        first = event(partition=None, offset=None)
        other = event(partition=None, offset=None)
        self.assertEqual(VersionDecision.EQUAL_POSITION_CONFLICT, version_decision(other, first))

    def test_partition_number_alone_is_not_freshness(self):
        original = event(partition=0, offset=1)
        moved_partition = event(partition=9, offset=999)
        self.assertEqual(VersionDecision.EQUAL_POSITION_CONFLICT, version_decision(moved_partition, original))

    def test_newer_delete_by_transaction_order_and_stale_delete(self):
        state, versions = {}, {}
        apply_current_state(state, versions, event(transaction={"id": "tx-1", "total_order": 10}))
        self.assertEqual("deleted", apply_current_state(state, versions, event(op="d", transaction={"id": "tx-1", "total_order": 11})))
        state, versions = {}, {}
        apply_current_state(state, versions, event(transaction={"id": "tx-1", "total_order": 11}))
        self.assertEqual("lower_transaction_order_ignored", apply_current_state(state, versions, event(op="d", transaction={"id": "tx-1", "total_order": 10})))
        self.assertIn(key_identity(event()), state)

    def test_tombstone_does_not_mutate_silver(self):
        self.assertEqual("tombstone_ignored", apply_current_state({}, {}, None))

    def test_entity_and_primary_key_form_a_collision_safe_key(self):
        self.assertNotEqual(key_identity(event("customers", "same")), key_identity(event("products", "same")))

    def test_preferred_language_and_transaction_metadata_are_preserved(self):
        current = event(value={"customer_id": "c1", "preferred_language": "da"}, transaction={"id": "tx-1", "total_order": "7", "data_collection_order": "3"})
        state, versions = {}, {}
        apply_current_state(state, versions, current)
        self.assertEqual("da", state[key_identity(current)]["preferred_language"])
        self.assertEqual("tx-1", current.transaction_id)
        self.assertEqual(7, current.transaction_total_order)
        self.assertEqual(3, current.transaction_data_collection_order)

    def test_malformed_and_unknown_entities_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "source.lsn"):
            parse_debezium("{}", json.dumps({"op": "c", "after": {"customer_id": "c1"}, "source": {}}), "mdep.commerce.customers", 0, 1)
        with self.assertRaisesRegex(ValueError, "transaction.total_order"):
            event(transaction={"id": "tx-1", "total_order": "not-a-number"})
        with self.assertRaisesRegex(ValueError, "outside"):
            parse_debezium("{}", json.dumps({"op": "c", "source": {"lsn": "0/1"}}), "mdep.commerce.unknown", 0, 1)

    def test_delete_with_after_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "after=null"):
            event(op="d", value={"customer_id": "c1"})


if __name__ == "__main__":
    unittest.main()
