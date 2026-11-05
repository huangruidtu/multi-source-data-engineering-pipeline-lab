"""Matching immutable-object semantics for local filesystems and boto3 S3."""

from __future__ import annotations

import base64
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol


@dataclass(frozen=True)
class ObjectInfo:
    key: str
    size: int
    checksum: str | None


class ConditionalWriteConflict(RuntimeError):
    def __init__(self, key: str, code: str):
        super().__init__(f"conditional write failed for {key}: {code}")
        self.key = key
        self.code = code


class ObjectStoreFailure(RuntimeError):
    pass


class ObjectStore(Protocol):
    def head(self, key: str) -> ObjectInfo | None: ...

    def get_bytes(self, key: str) -> bytes: ...

    def put_bytes_if_absent(self, key: str, data: bytes, *, content_type: str, checksum: str) -> None: ...

    def list_keys(self, prefix: str) -> list[str]: ...


class LocalObjectStore:
    def __init__(self, root: str | Path):
        self.root = Path(root)

    def _path(self, key: str) -> Path:
        candidate = (self.root / key).resolve()
        root = self.root.resolve()
        if root not in candidate.parents and candidate != root:
            raise ObjectStoreFailure(f"object key escapes local root: {key}")
        return candidate

    def head(self, key: str) -> ObjectInfo | None:
        path = self._path(key)
        if not path.is_file():
            return None
        data = path.read_bytes()
        return ObjectInfo(key, len(data), hashlib.sha256(data).hexdigest())

    def get_bytes(self, key: str) -> bytes:
        path = self._path(key)
        if not path.is_file():
            raise ObjectStoreFailure(f"object does not exist: {key}")
        return path.read_bytes()

    def put_bytes_if_absent(self, key: str, data: bytes, *, content_type: str, checksum: str) -> None:
        del content_type, checksum
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with path.open("xb") as handle:
                handle.write(data)
        except FileExistsError as error:
            raise ConditionalWriteConflict(key, "412") from error

    def list_keys(self, prefix: str) -> list[str]:
        prefix_path = self._path(prefix)
        if not prefix_path.exists():
            return []
        normalized_prefix = prefix.strip("/")
        return sorted(
            f"{normalized_prefix}/{path.relative_to(prefix_path).as_posix()}".strip("/")
            for path in prefix_path.rglob("*")
            if path.is_file()
        )


class S3ObjectStore:
    """boto3-backed adapter using conditional writes and SSE-KMS."""

    def __init__(self, bucket: str, *, client: Any | None = None, prefix: str = "", kms_key_id: str | None = None):
        if client is None:
            try:
                import boto3
            except ImportError as error:  # pragma: no cover
                raise RuntimeError("boto3 is required for S3 publication.") from error
            client = boto3.client("s3")
        self.bucket = bucket
        self.client = client
        self.prefix = prefix.strip("/")
        self.kms_key_id = kms_key_id

    def _key(self, key: str) -> str:
        return f"{self.prefix}/{key}".strip("/")

    @staticmethod
    def _code(error: Exception) -> str:
        response = getattr(error, "response", {})
        return str(response.get("Error", {}).get("Code", "Unknown"))

    def head(self, key: str) -> ObjectInfo | None:
        full_key = self._key(key)
        try:
            listed = self.client.list_objects_v2(Bucket=self.bucket, Prefix=full_key, MaxKeys=1)
        except Exception as error:
            code = self._code(error)
            raise ObjectStoreFailure(f"S3 existence check failed for {key}: {code}") from error
        if not any(item.get("Key") == full_key for item in listed.get("Contents", [])):
            return None
        try:
            response = self.client.head_object(Bucket=self.bucket, Key=full_key, ChecksumMode="ENABLED")
        except Exception as error:
            code = self._code(error)
            raise ObjectStoreFailure(f"S3 head failed for {key}: {code}") from error
        checksum = response.get("Metadata", {}).get("payload-sha256")
        return ObjectInfo(key, int(response.get("ContentLength", 0)), checksum)

    def get_bytes(self, key: str) -> bytes:
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self._key(key), ChecksumMode="ENABLED")
            return response["Body"].read()
        except Exception as error:
            raise ObjectStoreFailure(f"S3 get failed for {key}: {self._code(error)}") from error

    def put_bytes_if_absent(self, key: str, data: bytes, *, content_type: str, checksum: str) -> None:
        kwargs: dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": self._key(key),
            "Body": data,
            "ContentType": content_type,
            "IfNoneMatch": "*",
            "ChecksumSHA256": base64.b64encode(bytes.fromhex(checksum)).decode("ascii"),
            "Metadata": {"payload-sha256": checksum},
            "ServerSideEncryption": "aws:kms",
        }
        if self.kms_key_id:
            kwargs["SSEKMSKeyId"] = self.kms_key_id
        try:
            self.client.put_object(**kwargs)
        except Exception as error:
            code = self._code(error)
            if code in {"409", "412", "ConditionalRequestConflict", "PreconditionFailed"}:
                raise ConditionalWriteConflict(key, code) from error
            raise ObjectStoreFailure(f"S3 put failed for {key}: {code}") from error

    def list_keys(self, prefix: str) -> list[str]:
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            keys: list[str] = []
            full_prefix = self._key(prefix)
            for page in paginator.paginate(Bucket=self.bucket, Prefix=full_prefix):
                for item in page.get("Contents", []):
                    key = item["Key"]
                    keys.append(key[len(self.prefix) + 1 :] if self.prefix else key)
            return sorted(keys)
        except Exception as error:
            raise ObjectStoreFailure(f"S3 list failed for {prefix}: {self._code(error)}") from error
