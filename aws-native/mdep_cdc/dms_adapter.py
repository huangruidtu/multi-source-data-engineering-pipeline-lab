"""Strict adapter for AWS DMS JSON_UNFORMATTED Kinesis records.

This intentionally does not understand Debezium's r/c/u/d envelope. DMS emits
``data`` and ``metadata`` objects and names data operations
load/insert/update/delete.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Mapping

from .models import DmsEvent, ENTITY_CONTRACTS, SourcePosition


LSN_RE = re.compile(r"^(?P<high>[0-9A-Fa-f]{1,8})/(?P<low>[0-9A-Fa-f]{1,8})$")
CDC_OPERATIONS = frozenset({"insert", "update", "delete"})
DATA_OPERATIONS = CDC_OPERATIONS | {"load"}


class DmsContractError(ValueError):
    """An event cannot be safely interpreted under the AWS-05 contract."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def parse_lsn(value: object) -> tuple[str, int]:
    if not isinstance(value, str) or not (match := LSN_RE.fullmatch(value)):
        raise DmsContractError("missing_or_invalid_stream_position")
    normalized = f"{match.group('high').upper()}/{match.group('low').upper()}"
    return normalized, (int(match.group("high"), 16) << 32) + int(match.group("low"), 16)


class DmsAdapter:
    """Convert one real DMS Kinesis JSON record into a typed mutation."""

    def __init__(self, *, source_schema: str = "commerce"):
        self.source_schema = source_schema

    def parse(self, raw: str | bytes | Mapping[str, Any]) -> DmsEvent:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        if isinstance(raw, str):
            original_raw = raw.rstrip("\r\n")
            try:
                envelope = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise DmsContractError("invalid_json") from exc
        else:
            envelope = dict(raw)
            original_raw = canonical_json(envelope)
        if not isinstance(envelope, dict) or not isinstance(envelope.get("metadata"), dict):
            raise DmsContractError("missing_dms_metadata")
        metadata = envelope["metadata"]
        if metadata.get("record-type") != "data":
            raise DmsContractError("non_data_control_record")
        operation = str(metadata.get("operation", "")).lower()
        if operation not in DATA_OPERATIONS:
            raise DmsContractError("unsupported_dms_operation")
        if metadata.get("schema-name") != self.source_schema:
            raise DmsContractError("unexpected_source_schema")
        table = metadata.get("table-name")
        if table not in ENTITY_CONTRACTS:
            raise DmsContractError("unexpected_source_table")
        contract = ENTITY_CONTRACTS[table]
        data = envelope.get("data")
        if not isinstance(data, dict):
            raise DmsContractError("missing_dms_data")
        key = data.get(contract.primary_key)
        if key is None or str(key).strip() == "":
            raise DmsContractError("missing_primary_key")
        if operation != "delete":
            missing = sorted(contract.required_upsert_fields - data.keys())
            if missing:
                raise DmsContractError("missing_business_fields:" + ",".join(missing))

        position = self._position(operation, metadata)
        record_hash = sha256_text(canonical_json(data))
        identity = {
            "entity": contract.entity,
            "primary_key": str(key),
            "operation": operation,
            "position": {
                "phase": position.phase,
                "stream_position": position.stream_position,
                "transaction_id": position.transaction_id,
                "transaction_record_id": position.transaction_record_id,
            },
            "record_hash": record_hash,
        }
        return DmsEvent(
            entity=contract.entity,
            primary_key_name=contract.primary_key,
            primary_key=str(key),
            operation=operation,
            data=data,
            metadata=metadata,
            position=position,
            source_event_at=metadata.get("commit-timestamp") or metadata.get("timestamp"),
            raw_json=original_raw,
            record_hash=record_hash,
            event_hash=sha256_text(canonical_json(identity)),
        )

    @staticmethod
    def _position(operation: str, metadata: Mapping[str, Any]) -> SourcePosition:
        if operation == "load":
            return SourcePosition(0, None, None, None, None)
        stream_position, lsn = parse_lsn(metadata.get("stream-position"))
        txid = metadata.get("transaction-id")
        record_id = metadata.get("transaction-record-id")
        if txid is None or record_id is None:
            raise DmsContractError("missing_transaction_order")
        try:
            record_id = int(record_id)
        except (TypeError, ValueError) as exc:
            raise DmsContractError("invalid_transaction_record_id") from exc
        if record_id < 0:
            raise DmsContractError("invalid_transaction_record_id")
        return SourcePosition(1, stream_position, lsn, str(txid), record_id)
