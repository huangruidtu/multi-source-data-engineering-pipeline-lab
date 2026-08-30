import re
import unittest
from pathlib import Path


ROOT = Path("analytics/dbt")


class Mdep12WarehouseContractTests(unittest.TestCase):
    def test_sources_are_only_the_six_approved_silver_tables(self):
        source_yaml = (ROOT / "models/sources.yml").read_text(encoding="utf-8")
        for table in ("core_customers", "core_products", "core_orders", "core_payments", "ref_exchange_rates", "ref_locations"):
            self.assertIn(table, source_yaml)
        self.assertNotIn("bronze", source_yaml.lower())

    def test_gold_dag_has_dimensions_facts_and_mart_with_declared_grain(self):
        marts = ROOT / "models/marts"
        for model in ("dim_customers", "dim_products", "dim_locations", "dim_date", "fct_orders", "fct_payments", "mart_daily_sales"):
            content = (marts / f"{model}.sql").read_text(encoding="utf-8")
            self.assertIn("Grain:", content)
        self.assertIn("incremental", (marts / "fct_orders.sql").read_text(encoding="utf-8"))

    def test_incremental_and_currency_contracts_are_explicit(self):
        orders = (ROOT / "models/marts/fct_orders.sql").read_text(encoding="utf-8")
        enriched = (ROOT / "models/intermediate/int_orders_enriched.sql").read_text(encoding="utf-8")
        self.assertIn("unique_key='order_id'", orders)
        self.assertIn("delete from {{ this }}", orders)
        self.assertIn("quote_currency = 'DKK'", enriched)
        self.assertIn("missing_dkk_rate", enriched)

    def test_setup_keeps_silver_external_and_has_no_embedded_secret(self):
        setup = Path("warehouse/snowflake/01_setup.sql").read_text(encoding="utf-8")
        self.assertEqual(6, len(re.findall(r"CREATE OR REPLACE ICEBERG TABLE", setup)))
        self.assertIn("CATALOG_SOURCE = OBJECT_STORE", setup)
        self.assertNotRegex(setup, r"(?i)(aws_secret_access_key|password\s*=)")


if __name__ == "__main__":
    unittest.main()
