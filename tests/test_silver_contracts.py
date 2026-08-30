from datetime import datetime, timezone
import unittest

from processing.spark.contracts import (
    canonical_record_hash,
    deterministic_silver_key,
    in_incremental_boundary,
    normalize_exchange_rate,
    normalize_location,
)


def exchange_rate(**changes):
    record = {"rate_date": "2025-02-01", "base_currency": "eur", "quote_currency": "dkk", "rate": "7.456", "retrieved_at": "2025-02-01T06:00:00Z"}
    record.update(changes)
    return record


def location(**changes):
    record = {"location_id": "loc-100", "location_name": "Copenhagen Hub", "country_code": "dk", "timezone": "Europe/Copenhagen", "updated_at": "2025-02-01T08:00:00Z"}
    record.update(changes)
    return record


class SilverContractTests(unittest.TestCase):
    def test_exchange_rate_normalizes_business_types_and_key(self):
        result = normalize_exchange_rate(exchange_rate())
        self.assertIsNone(result.rejection_reason)
        self.assertEqual("EUR", result.normalized["base_currency"])
        self.assertEqual("2025-02-01|EUR|DKK", deterministic_silver_key("exchange_rates", result.normalized))

    def test_exchange_rate_rejects_invalid_values(self):
        for changes, reason in (({"rate": "nope"}, "invalid_type"), ({"rate": "0"}, "non_positive_rate"), ({"quote_currency": "EUR"}, "base_currency_equals")):
            self.assertIn(reason, normalize_exchange_rate(exchange_rate(**changes)).rejection_reason)

    def test_location_enrichment_and_reference_integrity(self):
        result = normalize_location(location())
        self.assertEqual("nordics", result.normalized["region"])
        self.assertEqual("unknown_country_reference", normalize_location(location(country_code="XX")).rejection_reason)

    def test_incremental_boundary_is_start_inclusive_and_end_exclusive(self):
        start, end = datetime(2025, 2, 1, tzinfo=timezone.utc), datetime(2025, 2, 2, tzinfo=timezone.utc)
        self.assertTrue(in_incremental_boundary("2025-02-01T00:00:00Z", start, end))
        self.assertFalse(in_incremental_boundary("2025-02-02T00:00:00Z", start, end))

    def test_hash_is_deterministic_and_ownership_is_enforced(self):
        self.assertEqual(canonical_record_hash({"a": 1, "b": 2}), canonical_record_hash({"b": 2, "a": 1}))
        with self.assertRaisesRegex(ValueError, "batch-owned"):
            deterministic_silver_key("orders", {"order_id": "order-1"})
