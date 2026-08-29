import unittest

from app import EXCHANGE_RATES, LOCATIONS, paginate


class PaginationTests(unittest.TestCase):
    def test_returns_deterministic_page(self):
        payload = paginate(EXCHANGE_RATES, {"page": ["2"], "page_size": ["2"]})
        self.assertEqual(payload["page"], 2)
        self.assertEqual(payload["total_items"], 5)
        self.assertEqual(payload["items"][0]["quote_currency"], "DKK")
        self.assertEqual(payload["next_page"], 3)

    def test_final_page_has_no_next_page(self):
        payload = paginate(LOCATIONS, {"page": ["2"], "page_size": ["2"]})
        self.assertEqual(len(payload["items"]), 1)
        self.assertIsNone(payload["next_page"])

    def test_rejects_invalid_page(self):
        with self.assertRaises(ValueError):
            paginate(LOCATIONS, {"page": ["0"]})


if __name__ == "__main__":
    unittest.main()
