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


def context(ingested_at=None):
    return BatchContext(
        "rest_api",
        "locations",
        date(2025, 2, 1),
        datetime(2025, 2, 1, tzinfo=timezone.utc),
        datetime(2025, 2, 2, tzinfo=timezone.utc),
        ingested_at or datetime(2025, 2, 2, 12, 30, tzinfo=timezone.utc),
    )


def records(name="Copenhagen", ingested_at="2025-02-02T12:30:00Z"):
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
            "ingested_at": ingested_at,
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

    def test_parquet_preserves_optional_field_first_seen_in_second_record(self):
        two_records = [records()[0], {**records("Aarhus")[0], "location_id": "loc-2", "optional_region_code": "DK-82"}]
        prepared = to_parquet(two_records)
        with tempfile.TemporaryDirectory() as directory:
            store = LocalObjectStore(directory)
            result = BronzePublisher(store).publish(context(), prepared)
            round_trip = read_parquet(store.get_bytes(context().data_key))
        self.assertEqual(Status.PUBLISHED, result.status)
        self.assertIn("optional_region_code", round_trip[0])
        self.assertIsNone(round_trip[0]["optional_region_code"])
        self.assertEqual("DK-82", round_trip[1]["optional_region_code"])

    def test_independent_identical_replay_preserves_first_ingestion_time(self):
        first_time = datetime(2025, 2, 2, 12, 30, tzinfo=timezone.utc)
        replay_time = datetime(2025, 3, 1, 9, 45, tzinfo=timezone.utc)
        first_records = records(ingested_at="2025-02-02T12:30:00Z")
        replay_records = records(ingested_at="2025-03-01T09:45:00Z")
        with tempfile.TemporaryDirectory() as directory:
            store = LocalObjectStore(directory)
            publisher = BronzePublisher(store)
            first = publisher.publish(context(first_time), to_parquet(first_records))
            replay = publisher.publish(context(replay_time), to_parquet(replay_records))
            stored = read_parquet(store.get_bytes(first.data_key))
        self.assertEqual(Status.PUBLISHED, first.status)
        self.assertEqual(Status.ALREADY_PUBLISHED, replay.status)
        self.assertEqual("verified_independent_replay_preserved_first_ingestion", replay.detail)
        self.assertEqual("2025-02-02T12:30:00Z", stored[0]["ingested_at"])

    def test_changed_content_under_same_identity_is_conflict(self):
        with tempfile.TemporaryDirectory() as directory:
            publisher = BronzePublisher(LocalObjectStore(directory))
            self.assertEqual(Status.PUBLISHED, publisher.publish(context(), to_parquet(records())).status)
            conflict = publisher.publish(
                context(datetime(2025, 3, 1, 9, 45, tzinfo=timezone.utc)),
                to_parquet(records("Aarhus", "2025-03-01T09:45:00Z")),
            )
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

    def test_independent_replay_recovers_matching_orphan_without_rewriting_data(self):
        original = to_parquet(records(ingested_at="2025-02-02T12:30:00Z"))
        replay = to_parquet(records(ingested_at="2025-03-01T09:45:00Z"))
        with tempfile.TemporaryDirectory() as directory:
            store = LocalObjectStore(directory)
            store.put_bytes_if_absent(context().data_key, original.data, content_type="application/vnd.apache.parquet", checksum=original.checksum)
            result = BronzePublisher(store).publish(
                context(datetime(2025, 3, 1, 9, 45, tzinfo=timezone.utc)),
                replay,
            )
            manifest = json.loads(store.get_bytes(context().manifest_key))
            stored = read_parquet(store.get_bytes(context().data_key))
        self.assertEqual(Status.PUBLISHED, result.status)
        self.assertEqual("recovered_matching_orphan_data", result.detail)
        self.assertEqual(f"sha256:{original.checksum}", manifest["payload_checksum"])
        self.assertEqual("2025-02-02T12:30:00Z", stored[0]["ingested_at"])

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
    def __init__(self, allowed_list_prefixes=("bronze/", "quarantine/")):
        self.objects = {}
        self.put_calls = []
        self.list_calls = []
        self.head_calls = []
        self.fail_put_code = None
        self.allowed_list_prefixes = allowed_list_prefixes

    def list_objects_v2(self, **kwargs):
        self.list_calls.append(kwargs)
        prefix = kwargs["Prefix"]
        if not any(prefix.startswith(allowed) for allowed in self.allowed_list_prefixes):
            raise FakeClientError("AccessDenied")
        contents = [{"Key": key} for key in sorted(self.objects) if key.startswith(prefix)]
        return {"Contents": contents[: kwargs.get("MaxKeys", 1000)]}

    def head_object(self, **kwargs):
        self.head_calls.append(kwargs)
        key = kwargs["Key"]
        if key not in self.objects:
            raise FakeClientError("AccessDenied")
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
    def test_first_publication_uses_prefix_scoped_exact_key_listing(self):
        client = FakeS3Client()
        publisher = BronzePublisher(S3ObjectStore("bucket", client=client, kms_key_id="key-arn"))
        result = publisher.publish(context(), to_parquet(records()))
        self.assertEqual(Status.PUBLISHED, result.status)
        self.assertTrue(client.list_calls)
        self.assertTrue(all(call["Prefix"].startswith("bronze/") for call in client.list_calls))
        self.assertTrue(all(call["MaxKeys"] == 1 for call in client.list_calls))
        self.assertEqual({context().data_key, context().manifest_key}, set(client.objects))

    def test_missing_object_uses_exact_list_and_does_not_hide_access_denied(self):
        client = FakeS3Client()
        store = S3ObjectStore("bucket", client=client)
        self.assertIsNone(store.head("bronze/rest_api/locations/missing.parquet"))
        self.assertEqual([], client.head_calls)
        self.assertEqual("bronze/rest_api/locations/missing.parquet", client.list_calls[0]["Prefix"])
        with self.assertRaises(ObjectStoreFailure):
            store.head("source/reference/not-listable.json")

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
