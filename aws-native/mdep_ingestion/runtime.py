"""Small runtime helpers shared by Airflow and the Lambda entrypoint."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Mapping

from .models import IngestionOutcome, Status
from .object_store import LocalObjectStore, S3ObjectStore
from .pipeline import IngestionPipeline
from .publication import BronzePublisher, QuarantinePublisher


def build_pipeline(
    *,
    bucket: str | None = None,
    kms_key_id: str | None = None,
    s3_client: Any | None = None,
    local_root: str | Path | None = None,
) -> IngestionPipeline:
    if bool(bucket) == bool(local_root):
        raise ValueError("configure exactly one of bucket or local_root")
    store = (
        S3ObjectStore(str(bucket), client=s3_client, kms_key_id=kms_key_id)
        if bucket
        else LocalObjectStore(Path(str(local_root)))
    )
    return IngestionPipeline(BronzePublisher(store), QuarantinePublisher(store))


def pipeline_from_environment(environ: Mapping[str, str] | None = None) -> IngestionPipeline:
    env = environ or os.environ
    local_root = env.get("MDEP_LOCAL_OBJECT_ROOT")
    bucket = env.get("MDEP_DATA_BUCKET")
    if local_root:
        return build_pipeline(local_root=local_root)
    if not bucket:
        raise RuntimeError("MDEP_DATA_BUCKET or MDEP_LOCAL_OBJECT_ROOT is required")
    kms_key_id = env.get("MDEP_KMS_KEY_ID")
    if not kms_key_id:
        raise RuntimeError("MDEP_KMS_KEY_ID is required for S3 publication")
    return build_pipeline(bucket=bucket, kms_key_id=kms_key_id)


def compact_or_raise(outcome: IngestionOutcome) -> dict[str, Any]:
    compact = outcome.compact()
    if outcome.status in {Status.FAILED, Status.CONFLICT}:
        raise RuntimeError(json.dumps(compact, sort_keys=True))
    return compact
