import io
import json
import sys
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path


AWS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(AWS_ROOT))

from mdep_ingestion.models import BatchContext, Status, canonical_json_bytes, sha256_hex
from mdep_ingestion.object_store import ConditionalWriteConflict, LocalObjectStore, ObjectStoreFailure, S3ObjectStore
from mdep_ingestion.publication import BronzePublisher, manifest_for
from mdep_ingestion.serialization import read_parquet, to_parquet


def context():
    return BatchContext(
        "rest_api",
        "locations",
        date(2025, 2, 1),
        datetime(2025, 2, 1, tzinfo=timezone.utc),
        datetime(2025, 2, 2, tzinfo=timezone.utc),
    )


def records(name="Copenhagen"):
    return [
        {
            "location_id": "loc-1",
            "location_name": name,
            "country_code": "DK",
            "timezone": "Europe/Copenhagen",
            "updated_at": "2025-02-01T00:00:00Z",
            "ingestion_id": context().ingestion_id,
            "source_name": "rest_api",
            "source_entity": "locations",
            "source_record_key": "loc-1",
            "source_extract_ts": "2025-02-01T00:00:00Z",
            "ingested_at": "2025-02-02T00:00:00Z",
            "source_version": None,
            "source_locator": "page=1",
            "record_hash": "a" * 64,
        }
    ]


class PublicationTests(unittest.TestCase):
    def test_real_parquet_and_identical_replay(self):
        prepared = to_parquet(records())
        self.assertTrue(prepared.data.startswith(b"PAR1"))
        self.assertEqual("loc-1", read_parquet(prepared.data)[0]["location_id"])
        with tempfile.TemporaryDirectory() as directory:
            publisher = BronzePublisher(LocalObjectStore(directory))
            first = publisher.publish(context(), prepared)
            second = publisher.publish(context(), prepared)
            self.assertEqual(Status.PUBLISHED, first.status)
            self.assertEqual(Status.ALREADY_PUBLISHED, second.status)
            self.assertEqual(1, len(publisher.list_committed()))

    def test_changed_content_under_same_identity_is_conflict(self):
        with tempfile.TemporaryDirectory() as directory:
            publisher = BronzePublisher(LocalObjectStore(directory))
            self.assertEqual(Status.PUBLISHED, publisher.publish(context(), to_parquet(records())).status)
            conflict = publisher.publish(context(), to_parquet(records("Aarhus")))
            self.assertEqual(Status.CONFLICT, conflict.status)
            self.assertIn("conflict", conflict.detail)

    def test_matching_orphan_data_is_recovered_by_manifest_last(self):
        prepared = to_parquet(records())
        with tempfile.TemporaryDirectory() as directory:
            store = LocalObjectStore(directory)
            store.put_bytes_if_absent(context().data_key, prepared.data, content_type="application/vnd.apache.parquet", checksum=prepared.checksum)
            result = BronzePublisher(store).publish(context(), prepared)
            self.assertEqual(Status.PUBLISHED, result.status)
            self.assertEqual("recovered_matching_orphan_data", result.detail)
            self.assertIsNotNone(store.head(context().manifest_key))

    def test_orphan_with_changed_content_and_manifest_without_data_conflict(self):
        with tempfile.TemporaryDirectory() as directory:
            store = LocalObjectStore(directory)
            other = to_parquet(records("Aarhus"))
            store.put_bytes_if_absent(context().data_key, other.data, content_type="application/vnd.apache.parquet", checksum=other.checksum)
            self.assertEqual(Status.CONFLICT, BronzePublisher(store).publish(context(), to_parquet(records())).status)
        with tempfile.TemporaryDirectory() as directory:
            store = LocalObjectStore(directory)
            prepared = to_parquet(records())
            manifest = canonical_json_bytes(manifest_for(context(), prepared))
            store.put_bytes_if_absent(context().manifest_key, manifest, content_type="application/json", checksum=sha256_hex(manifest))
            result = BronzePublisher(store).publish(context(), prepared)
            self.assertEqual(Status.CONFLICT, result.status)
            self.assertEqual("manifest_exists_but_data_missing", result.detail)

    def test_interrupted_manifest_write_returns_failed_then_recovers(self):
        class FailManifestOnce(LocalObjectStore):
            def __init__(self, root):
                super().__init__(root)
                self.failed = False

            def put_bytes_if_absent(self, key, data, *, content_type, checksum):
                if key.endswith("_COMMITTED.json") and not self.failed:
                    self.failed = True
                    raise ObjectStoreFailure("simulated permission failure")
                return super().put_bytes_if_absent(key, data, content_type=content_type, checksum=checksum)

        with tempfile.TemporaryDirectory() as directory:
            store = FailManifestOnce(directory)
            publisher = BronzePublisher(store)
            prepared = to_parquet(records())
            failed = publisher.publish(context(), prepared)
            self.assertEqual(Status.FAILED, failed.status)
            self.assertIsNotNone(store.head(context().data_key))
            self.assertIsNone(store.head(context().manifest_key))
            recovered = publisher.publish(context(), prepared)
            self.assertEqual(Status.PUBLISHED, recovered.status)


class FakeClientError(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class FakeS3Client:
    def __init__(self):
        self.objects = {}
        self.put_calls = []
        self.fail_put_code = None

    def head_object(self, **kwargs):
        key = kwargs["Key"]
        if key not in self.objects:
            raise FakeClientError("404")
        value = self.objects[key]
        return {"ContentLength": len(value), "Metadata": {"payload-sha256": sha256_hex(value)}}

    def get_object(self, **kwargs):
        key = kwargs["Key"]
        if key not in self.objects:
            raise FakeClientError("NoSuchKey")
        return {"Body": io.BytesIO(self.objects[key])}

    def put_object(self, **kwargs):
        self.put_calls.append(kwargs)
        if self.fail_put_code:
            code, self.fail_put_code = self.fail_put_code, None
            raise FakeClientError(code)
        if kwargs["Key"] in self.objects:
            raise FakeClientError("PreconditionFailed")
        self.objects[kwargs["Key"]] = kwargs["Body"]


class S3AdapterTests(unittest.TestCase):
    def test_boto3_calls_are_conditional_kms_encrypted_and_scoped(self):
        client = FakeS3Client()
        store = S3ObjectStore("bucket", client=client, prefix="lab", kms_key_id="key-arn")
        store.put_bytes_if_absent("bronze/a", b"payload", content_type="application/octet-stream", checksum=sha256_hex(b"payload"))
        call = client.put_calls[0]
        self.assertEqual("*", call["IfNoneMatch"])
        self.assertEqual("aws:kms", call["ServerSideEncryption"])
        self.assertEqual("key-arn", call["SSEKMSKeyId"])
        self.assertEqual("lab/bronze/a", call["Key"])

    def test_s3_412_and_409_are_conditional_conflicts_and_access_denied_fails(self):
        for code in ("412", "ConditionalRequestConflict"):
            client = FakeS3Client()
            client.fail_put_code = code
            store = S3ObjectStore("bucket", client=client)
            with self.assertRaises(ConditionalWriteConflict):
                store.put_bytes_if_absent("bronze/a", b"x", content_type="x", checksum=sha256_hex(b"x"))
        client = FakeS3Client()
        client.fail_put_code = "AccessDenied"
        with self.assertRaises(ObjectStoreFailure):
            S3ObjectStore("bucket", client=client).put_bytes_if_absent("bronze/a", b"x", content_type="x", checksum=sha256_hex(b"x"))


if __name__ == "__main__":
    unittest.main()
