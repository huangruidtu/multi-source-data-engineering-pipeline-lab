from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[1]
REPOSITORY = ROOT.parents[1]


class GoldContractTests(unittest.TestCase):
    def test_external_schema_maps_existing_aws03_tables_only(self):
        sql = (ROOT / "redshift" / "bootstrap_external_schema.sql").read_text()
        lowered = sql.lower()
        self.assertIn("from data catalog", lowered)
        self.assertIn("database '{{ glue_database }}'", lowered)
        self.assertIn("iam_role '{{ redshift_iam_role_arn }}'", lowered)
        self.assertNotIn("create external database", lowered)
        self.assertNotRegex(lowered, r"create\s+(external\s+)?table")
        self.assertIn("ref_exchange_rates", lowered)
        self.assertIn("ref_locations", lowered)

    def test_incremental_model_is_merge_without_implicit_deletes(self):
        sql = (ROOT / "models" / "gold" / "fct_orders.sql").read_text()
        self.assertIn("incremental_strategy='merge'", sql)
        self.assertIn("unique_key='order_id'", sql)
        self.assertNotIn("post_hook", sql)
        self.assertIn("source_updated_at", sql)
        self.assertIn("fx_retrieved_at", sql)
        self.assertIn("fx_source_extract_ts", sql)
        self.assertIn("fx_ingested_at", sql)
        self.assertIn("fx_record_hash", sql)

    def test_fixture_boundary_and_aws05_live_row_filter_are_explicit(self):
        sources = (ROOT / "models" / "sources.yml").read_text()
        self.assertIn("AWS-05 Iceberg v2 current-state ledgers", sources)
        self.assertIn("fixture_*", sources)
        self.assertTrue((ROOT / "seeds" / "fixture_orders.csv").exists())
        self.assertTrue((ROOT / "seeds" / "silver_bridge_exchange_rates.csv").exists())
        for entity in ("customers", "products", "orders", "payments"):
            staging = (ROOT / "models" / "staging" / f"stg_{entity}.sql").read_text()
            self.assertIn("target.type != 'duckdb'", staging)
            self.assertIn("where not coalesce(is_deleted, false)", staging)

    def test_redshift_lake_role_no_longer_reads_bronze(self):
        identity = (REPOSITORY / "aws-native" / "terraform" / "identity.tf").read_text()
        block = identity.split('data "aws_iam_policy_document" "redshift"', 1)[1]
        self.assertNotIn('"bronze/*"', block)
        self.assertIn('"iceberg/*"', block)


if __name__ == "__main__":
    unittest.main()
