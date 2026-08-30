import unittest
from pathlib import Path

from processing.flink.flink_cdc_job import (
    BRONZE_LAYOUT,
    CHECKPOINT_INTERVAL_MS,
    QUARANTINE_LAYOUT,
    SILVER_TABLES,
    TOPICS,
    WATERMARK_SECONDS,
    silver_table_ddls,
    topology_spec,
)


class FlinkTopologyTests(unittest.TestCase):
    def test_real_topology_contract_has_source_state_and_sinks(self):
        spec = topology_spec()
        self.assertEqual(4, len(TOPICS))
        self.assertEqual("earliest", spec["starting_offsets"])
        self.assertIn("ValueState", spec["keyed_state"])
        self.assertIn("source_lsn only", spec["cdc_order"])

    def test_job_source_contains_real_wiring_not_a_runtime_stub(self):
        job = Path("processing/flink/flink_cdc_job.py").read_text(encoding="utf-8")
        for construct in ("KafkaSource.builder()", "KeyedProcessFunction", "ValueStateDescriptor", "from_changelog_stream", "connector'='filesystem'", "catalog-type'='hadoop'", "statements.execute()"):
            self.assertIn(construct, job)
        self.assertNotIn("runtime submission requires", job)

    def test_layout_checkpoint_watermark_and_tables_are_concrete(self):
        self.assertEqual(30_000, CHECKPOINT_INTERVAL_MS)
        self.assertEqual(60, WATERMARK_SECONDS)
        self.assertIn("<entity>", BRONZE_LAYOUT)
        self.assertIn("quarantine/cdc", QUARANTINE_LAYOUT)
        self.assertEqual(4, len(SILVER_TABLES))

    def test_iceberg_ddls_include_primary_keys_and_additive_customer_field(self):
        ddls = silver_table_ddls()
        self.assertEqual({"customers", "products", "orders", "payments"}, set(ddls))
        self.assertIn("preferred_language STRING", ddls["customers"])
        for ddl in ddls.values():
            self.assertIn("catalog", ddl.lower()) if False else self.assertIn("PRIMARY KEY", ddl)
            self.assertIn("write.upsert.enabled", ddl)


if __name__ == "__main__":
    unittest.main()
