"""Bounded AWS-01 Lambda skeleton for validating S3 routing metadata.

The function deliberately performs no S3 read or write. Later ingestion stories
can add a queue or workflow target and the matching least-privilege permission.
"""

from __future__ import annotations

import json
import logging
from typing import Any
from urllib.parse import unquote_plus


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
ALLOWED_PREFIXES = ("bronze/", "quarantine/")


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    records = event.get("Records")
    if not isinstance(records, list) or not records:
        raise ValueError("Expected a non-empty S3 Records array")

    routes: list[dict[str, str]] = []
    for record in records:
        bucket = record.get("s3", {}).get("bucket", {}).get("name")
        encoded_key = record.get("s3", {}).get("object", {}).get("key")
        if not bucket or not encoded_key:
            raise ValueError("S3 record is missing bucket name or object key")
        key = unquote_plus(str(encoded_key))
        prefix = next((candidate for candidate in ALLOWED_PREFIXES if key.startswith(candidate)), None)
        if prefix is None:
            raise ValueError(f"Object key is outside approved prefixes: {key}")
        routes.append({"bucket": str(bucket), "key": key, "prefix": prefix.rstrip("/")})

    LOGGER.info(json.dumps({"event": "mdep_object_routed", "routes": routes}, sort_keys=True))
    return {"accepted": len(routes), "routes": routes}
