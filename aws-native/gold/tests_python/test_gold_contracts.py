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

    def test_integrated_duckdb_mode_uses_only_real_silver_sources(self):
        staging = list((ROOT / "models" / "staging").glob("stg_*.sql"))
        self.assertEqual(6, len(staging))
        for path in staging:
            text = path.read_text(encoding="utf-8")
            self.assertIn("MDEP_GOLD_INPUT_MODE", text, path.name)
            self.assertIn("source(", text, path.name)
        for name in ("customers", "products", "orders", "payments"):
            text = (ROOT / "models" / "staging" / f"stg_{name}.sql").read_text(encoding="utf-8")
            self.assertIn("where not coalesce(is_deleted, false)", text)

    def test_verified_tombstone_delete_never_infers_absence(self):
        macro = (ROOT / "macros" / "apply_verified_order_tombstones.sql").read_text(encoding="utf-8")
        self.assertIn("cdc_tombstones_verified=true", macro)
        self.assertIn("coalesce(tombstone.is_deleted, false)", macro)
        self.assertIn("lower(tombstone.dms_operation) = 'delete'", macro)
        self.assertIn("tombstone.source_lsn is not null", macro)
        self.assertNotIn("not exists", macro.lower())

    def test_integrated_harness_has_no_fixture_or_seed_fallback(self):
        harness = (ROOT / "scripts" / "run_integrated.py").read_text(encoding="utf-8")
        self.assertIn('"MDEP_GOLD_INPUT_MODE": "integrated"', harness)
        self.assertIn("bridge_tables_do_not_match_contract", harness)
        self.assertIn("export_checksum_mismatch", harness)
        self.assertNotIn('dbt("seed"', harness)
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
