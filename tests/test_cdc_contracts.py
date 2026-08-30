import unittest
import json
from pathlib import Path
from ingestion.cdc.contracts import classify_envelope, primary_key, topic_name


class CdcContractTests(unittest.TestCase):
    def test_topic_and_primary_key_follow_source_contract(self):
        self.assertEqual("mdep.commerce.orders", topic_name("orders"))
        self.assertEqual(("ord-1",), primary_key("orders", {"order_id": "ord-1"}))

    def test_unsupported_table_or_key_is_rejected(self):
        with self.assertRaises(ValueError): topic_name("locations")
        with self.assertRaises(ValueError): primary_key("customers", {})

    def test_debezium_operation_and_delete_semantics(self):
        self.assertEqual("snapshot_read", classify_envelope({"op": "r", "after": {"customer_id": "1"}}))
        self.assertEqual("create", classify_envelope({"op": "c", "after": {"customer_id": "1"}}))
        self.assertEqual("update", classify_envelope({"op": "u", "before": {}, "after": {}}))
        self.assertEqual("delete", classify_envelope({"op": "d", "before": {}, "after": None}))
        with self.assertRaises(ValueError): classify_envelope({"op": "d", "after": {}})

    def test_actual_connector_json_has_the_approved_debezium_settings(self):
        config_path = Path(__file__).resolve().parents[1] / "ingestion" / "cdc" / "debezium-postgres-connector.json"
        config = json.loads(config_path.read_text(encoding="utf-8"))["config"]
        self.assertEqual("io.debezium.connector.postgresql.PostgresConnector", config["connector.class"])
        self.assertEqual("pgoutput", config["plugin.name"])
        self.assertEqual("mdep_publication", config["publication.name"])
        self.assertEqual("mdep_debezium_slot", config["slot.name"])
        self.assertEqual("initial", config["snapshot.mode"])
        self.assertEqual("true", config["tombstones.on.delete"])
        self.assertEqual("true", config["provide.transaction.metadata"])
        self.assertEqual("mdep", config["topic.prefix"])
        self.assertEqual(
            {"commerce.customers", "commerce.products", "commerce.orders", "commerce.payments"},
            set(config["table.include.list"].split(",")),
        )
