import json
import sys
import unittest
from pathlib import Path


AWS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(AWS_ROOT))

from mdep_silver.backfill_validation import BackfillValidationError, ValidationRequest, validate_backfill


class Aws08ContractTests(unittest.TestCase):
    def test_request_fails_closed_before_any_spark_or_object_store_access(self):
        invalid = ValidationRequest(
            catalog="local;DROP",
            namespace="silver",
            manifest_keys=("bronze/a/_COMMITTED.json",),
            validation_run_identity="not-a-uuid",
            expected_interval_start="2026-01-01T00:00:00Z",
            expected_interval_end="2026-01-02T00:00:00Z",
        )
        with self.assertRaisesRegex(BackfillValidationError, "invalid_catalog_or_namespace"):
            validate_backfill(None, None, invalid, data_path=lambda key: key)

    def test_validator_has_no_canonical_write_statement(self):
        source = (AWS_ROOT / "mdep_silver" / "backfill_validation.py").read_text(encoding="utf-8")
        for mutation in ("MERGE INTO", "INSERT INTO", "UPDATE SET", "DELETE FROM", ".write."):
            self.assertNotIn(mutation, source)

    def test_launch_spec_is_bounded_manual_batch_and_uses_pinned_iceberg(self):
        launch = json.loads((AWS_ROOT / "emr" / "start-job-run.example.json").read_text(encoding="utf-8"))
        self.assertEqual("BATCH", launch["mode"])
        self.assertEqual(30, launch["executionTimeoutMinutes"])
        self.assertEqual(1, launch["retryPolicy"]["maxAttempts"])
        parameters = launch["jobDriver"]["sparkSubmit"]["sparkSubmitParameters"]
        self.assertIn("/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar", parameters)
        self.assertIn("org.apache.iceberg.aws.glue.GlueCatalog", parameters)
        self.assertIn("validation/logs/", launch["configurationOverrides"]["monitoringConfiguration"]["s3MonitoringConfiguration"]["logUri"])


if __name__ == "__main__":
    unittest.main()
