import json
import unittest

from processing.flink.cdc_model import VersionDecision, apply_current_state, key_identity, parse_debezium, parse_lsn, version_decision


def event(entity="customers", primary="c1", op="u", lsn="0/10", partition=0, offset=1, value=None):
    key_name = {"customers": "customer_id", "products": "product_id", "orders": "order_id", "payments": "payment_id"}[entity]
    after = value if value is not None else ({key_name: primary, "preferred_language": "en"} if op != "d" else None)
    payload = {"op": op, "before": {key_name: primary}, "after": after, "source": {"lsn": lsn, "txId": 7, "ts_ms": "2025-01-01T00:00:00Z"}}
    return parse_debezium(json.dumps({key_name: primary}), json.dumps(payload), f"mdep.commerce.{entity}", partition, offset)


class FlinkCdcTests(unittest.TestCase):
    def test_lsn_and_snapshot_envelope(self):
        self.assertGreater(parse_lsn("0/11"), parse_lsn("0/10"))
        snapshot = event(op="r")
        self.assertEqual("c1", snapshot.primary_key)
        self.assertEqual("upserted", apply_current_state({}, {}, snapshot))

    def test_higher_lsn_accepts_and_lower_lsn_rejects(self):
        state, versions = {}, {}
        self.assertEqual("upserted", apply_current_state(state, versions, event(lsn="0/10")))
        self.assertEqual("upserted", apply_current_state(state, versions, event(lsn="0/11", offset=2)))
        self.assertEqual("lower_lsn_ignored", apply_current_state(state, versions, event(lsn="0/F", offset=9)))

    def test_exact_replay_is_rejected(self):
        original = event()
        self.assertEqual(VersionDecision.EXACT_REPLAY, version_decision(original, original))
        state, versions = {}, {}
        apply_current_state(state, versions, original)
        self.assertEqual("exact_replay_ignored", apply_current_state(state, versions, original))

    def test_equal_lsn_partition_number_is_not_freshness(self):
        original = event(lsn="0/10", partition=0, offset=1)
        moved_partition = event(lsn="0/10", partition=9, offset=999)
        self.assertEqual(VersionDecision.EQUAL_LSN_TRANSPORT_CONFLICT, version_decision(moved_partition, original))
        state, versions = {}, {}
        apply_current_state(state, versions, original)
        self.assertEqual("equal_lsn_transport_conflict_ignored", apply_current_state(state, versions, moved_partition))

    def test_newer_delete_and_stale_delete(self):
        state, versions = {}, {}
        apply_current_state(state, versions, event(lsn="0/10"))
        self.assertEqual("lower_lsn_ignored", apply_current_state(state, versions, event(op="d", lsn="0/F")))
        self.assertEqual("deleted", apply_current_state(state, versions, event(op="d", lsn="0/11")))

    def test_tombstone_does_not_mutate_silver(self):
        self.assertEqual("tombstone_ignored", apply_current_state({}, {}, None))

    def test_entity_and_primary_key_form_a_collision_safe_key(self):
        self.assertNotEqual(key_identity(event("customers", "same")), key_identity(event("products", "same")))

    def test_preferred_language_is_preserved(self):
        current = event(value={"customer_id": "c1", "preferred_language": "da"})
        state, versions = {}, {}
        apply_current_state(state, versions, current)
        self.assertEqual("da", state[key_identity(current)]["preferred_language"])

    def test_malformed_and_unknown_entities_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "source.lsn"):
            parse_debezium("{}", json.dumps({"op": "c", "after": {"customer_id": "c1"}, "source": {}}), "mdep.commerce.customers", 0, 1)
        with self.assertRaisesRegex(ValueError, "outside"):
            parse_debezium("{}", json.dumps({"op": "c", "source": {"lsn": "0/1"}}), "mdep.commerce.unknown", 0, 1)

    def test_delete_with_after_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "after=null"):
            event(op="d", value={"customer_id": "c1"})
