import importlib.util
import io
import os
import sys
import unittest
from contextlib import contextmanager, redirect_stdout
import json
from pathlib import Path
from unittest.mock import patch


AWS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(AWS_ROOT))

from mdep_ingestion.models import IngestionOutcome, Status


def load_lambda():
    path = AWS_ROOT / "lambda" / "bronze_ingestion.py"
    spec = importlib.util.spec_from_file_location("mdep_aws02_lambda", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(module)
    return module


@contextmanager
def lambda_environment(**extra):
    values = {
        "MDEP_DATA_BUCKET": "mdep-test",
        "MDEP_KMS_KEY_ID": "arn:aws:kms:eu-west-1:123456789012:key/example",
        **extra,
    }
    with patch.dict(os.environ, values, clear=True):
        yield


class FakeS3:
    def get_object(self, **kwargs):
        self.get_request = kwargs
        content = b'{"devices": []}'
        return {"ContentLength": len(content), "Body": io.BytesIO(content)}


class FakePipeline:
    def __init__(self, status=Status.PUBLISHED, *, quarantine_key=None, detail=""):
        self.status = status
        self.quarantine_key = quarantine_key
        self.detail = detail
        self.calls = []

    def rest(self, context, **kwargs):
        self.calls.append(("rest", context, kwargs))
        return IngestionOutcome(
            self.status, context.source_name, context.source_entity, context.ingestion_id,
            record_count=2, quarantine_key=self.quarantine_key, detail=self.detail,
        )

    def file_bytes(self, context, **kwargs):
        self.calls.append(("file", context, kwargs))
        return IngestionOutcome(
            self.status, context.source_name, context.source_entity, context.ingestion_id,
            record_count=1, quarantine_key=self.quarantine_key, detail=self.detail,
        )


class LambdaAdapterTests(unittest.TestCase):
    def setUp(self):
        self.module = load_lambda()
        self.event = {
            "source_type": "rest_api",
            "entity": "locations",
            "logical_date": "2025-02-01",
            "interval_start": "2025-02-01T00:00:00Z",
            "interval_end": "2025-02-02T00:00:00Z",
            "ingested_at": "2025-02-02T12:30:00Z",
        }

    def test_rest_invocation_uses_configured_endpoint_and_shared_pipeline(self):
        pipeline = FakePipeline()
        fake_s3 = FakeS3()
        with lambda_environment(MDEP_REST_LOCATIONS_URL="https://example.test/locations"), patch.object(
            self.module.boto3, "client", return_value=fake_s3
        ), patch.object(self.module, "build_pipeline", return_value=pipeline):
            output = io.StringIO()
            with redirect_stdout(output):
                result = self.module.handler({**self.event, "source_id": "locations-primary"}, None)
        self.assertEqual("published", result["status"])
        self.assertEqual("https://example.test/locations", pipeline.calls[0][2]["url"])
        self.assertEqual("AWS_RUNTIME_DEFERRED", result["runtime_evidence"])
        event = json.loads(output.getvalue())
        self.assertEqual(("mdep-execution-event-v1", "locations-primary", "completed"), (
            event["event_version"], event["source_id"], event["status"],
        ))
        self.assertNotIn("https://example.test", output.getvalue())

    def test_reference_file_requires_scoped_key_and_reads_same_bucket(self):
        pipeline = FakePipeline()
        fake_s3 = FakeS3()
        event = {**self.event, "source_type": "reference_files", "entity": "devices", "source_key": "source/reference/devices.json"}
        with lambda_environment(), patch.object(self.module.boto3, "client", return_value=fake_s3), patch.object(
            self.module, "build_pipeline", return_value=pipeline
        ):
            self.module.handler(event, None)
        self.assertEqual({"Bucket": "mdep-test", "Key": "source/reference/devices.json"}, fake_s3.get_request)
        self.assertEqual("file", pipeline.calls[0][0])
        with lambda_environment(), patch.object(self.module.boto3, "client", return_value=fake_s3), patch.object(
            self.module, "build_pipeline", return_value=pipeline
        ), self.assertRaises(ValueError):
            self.module.handler({**event, "source_key": "bronze/devices.json"}, None)

    def test_conflict_fails_invocation_for_orchestrator_visibility(self):
        pipeline = FakePipeline(Status.CONFLICT)
        with lambda_environment(MDEP_REST_LOCATIONS_URL="https://example.test/locations"), patch.object(
            self.module.boto3, "client", return_value=FakeS3()
        ), patch.object(self.module, "build_pipeline", return_value=pipeline), self.assertRaises(RuntimeError):
            self.module.handler(self.event, None)

    def test_quarantine_emits_a_metric_filter_compatible_event_before_failure(self):
        pipeline = FakePipeline(
            Status.FAILED,
            quarantine_key="quarantine/rest_api/locations/rejection.json",
            detail="invalid_source_schema: https://example.test/locations#item=4",
        )
        with lambda_environment(MDEP_REST_LOCATIONS_URL="https://example.test/locations"), patch.object(
            self.module.boto3, "client", return_value=FakeS3()
        ), patch.object(self.module, "build_pipeline", return_value=pipeline), self.assertRaises(RuntimeError):
            output = io.StringIO()
            with redirect_stdout(output):
                self.module.handler(self.event, None)
        events = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(["failed", "quarantined"], [event["status"] for event in events])
        self.assertEqual("mdep-execution-event-v1", events[1]["event_version"])
        self.assertEqual("quarantine_publisher", events[1]["component"])
        self.assertEqual(1, events[1]["rejected_count"])
        self.assertEqual("invalid_source_schema", events[1]["error_code"])
        self.assertNotIn("https://", output.getvalue())


if __name__ == "__main__":
    unittest.main()
