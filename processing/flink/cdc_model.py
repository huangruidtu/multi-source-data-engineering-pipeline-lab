"""Pure CDC rules shared by the runnable MDEP-11 PyFlink job and unit tests.

These functions deliberately have no PyFlink dependency. The streaming job owns
durable ``ValueState``; this module makes the same ordering rule reviewable
without a JVM.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any

CDC_ENTITIES = frozenset({"customers", "products", "orders", "payments"})
PRIMARY_KEY = {"customers": "customer_id", "products": "product_id", "orders": "order_id", "payments": "payment_id"}


@dataclass(frozen=True)
class CdcEvent:
    entity: str
    primary_key: str
    operation: str
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    source_lsn: int
    source_tx_id: str | None
    transaction_id: str | None
    transaction_total_order: int | None
    transaction_data_collection_order: int | None
    source_event_ts: str | None
    kafka_topic: str
    kafka_partition: int | None
    kafka_offset: int | None
    snapshot: bool
    envelope: str


class VersionDecision(str, Enum):
    NEWER = "newer"
    LOWER_LSN = "lower_lsn"
    LOWER_TRANSACTION_ORDER = "lower_transaction_order"
    EXACT_REPLAY = "exact_replay"
    EQUAL_POSITION_CONFLICT = "equal_position_conflict"


def parse_lsn(value: str | int) -> int:
    """Convert PostgreSQL's ``X/Y`` LSN form to an orderable integer."""
    if isinstance(value, int):
        return value
    high, low = str(value).split("/", 1)
    return (int(high, 16) << 32) + int(low, 16)


def key_identity(event: CdcEvent) -> str:
    """Collision-safe Flink key: the same textual id may occur in two entities."""
    return f"{event.entity}:{event.primary_key}"


def _primary_from_payload(entity: str, key: dict[str, Any] | None, before: dict[str, Any] | None, after: dict[str, Any] | None) -> str:
    primary_field = PRIMARY_KEY[entity]
    for candidate in (key, after, before):
        if candidate and candidate.get(primary_field) is not None:
            return str(candidate[primary_field])
    raise ValueError(f"missing {primary_field} in Kafka key, before, and after")


def _optional_order(value: Any, field: str) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"invalid Debezium transaction.{field}") from error


def parse_debezium(key_json: str | None, value_json: str | None, topic: str, partition: int | None, offset: int | None) -> CdcEvent | None:
    """Normalize a Debezium JSON value; ``None`` is a Kafka tombstone.

    PyFlink 1.20's ``KafkaSource`` exposes value-only deserialization. The runtime
    therefore passes ``None`` for key/partition/offset and obtains the key from
    Debezium's before/after payload. A metadata-preserving source can use this
    same contract by supplying those optional fields.
    """
    if value_json is None:
        return None
    key = json.loads(key_json) if key_json else None
    payload = json.loads(value_json)
    entity = topic.rsplit(".", 1)[-1]
    if entity not in CDC_ENTITIES:
        raise ValueError("CDC entity is outside Flink ownership")
    op, source, transaction = payload.get("op"), payload.get("source") or {}, payload.get("transaction") or {}
    before, after = payload.get("before"), payload.get("after")
    if op not in {"r", "c", "u", "d"}:
        raise ValueError("unsupported Debezium operation")
    if op == "d" and after is not None:
        raise ValueError("delete must have after=null")
    if source.get("lsn") is None:
        raise ValueError("Debezium source.lsn is required")
    return CdcEvent(
        entity, _primary_from_payload(entity, key, before, after), op, before, after,
        parse_lsn(source["lsn"]), str(source.get("txId")) if source.get("txId") is not None else None,
        str(transaction.get("id")) if transaction.get("id") is not None else None,
        _optional_order(transaction.get("total_order"), "total_order"),
        _optional_order(transaction.get("data_collection_order"), "data_collection_order"),
        source.get("ts_ms"), topic, partition, offset, source.get("snapshot") in {True, "true", "last"}, value_json,
    )


def _same_known_transport(event: CdcEvent, last: CdcEvent) -> bool:
    """Identity is available only when both partition and offset are present."""
    return (
        event.kafka_partition is not None and event.kafka_offset is not None
        and last.kafka_partition is not None and last.kafka_offset is not None
        and (event.kafka_topic, event.kafka_partition, event.kafka_offset)
        == (last.kafka_topic, last.kafka_partition, last.kafka_offset)
    )


def version_decision(event: CdcEvent, last: CdcEvent | None) -> VersionDecision:
    """Order one source key by LSN, then Debezium transaction order, then identity.

    Kafka partition numbers are never compared numerically. A missing transport
    coordinate cannot prove that two messages are the same replay.
    """
    if last is None or event.source_lsn > last.source_lsn:
        return VersionDecision.NEWER
    if event.source_lsn < last.source_lsn:
        return VersionDecision.LOWER_LSN
    same_transaction = event.transaction_id is not None and event.transaction_id == last.transaction_id
    if same_transaction and event.transaction_total_order is not None and last.transaction_total_order is not None:
        if event.transaction_total_order > last.transaction_total_order:
            return VersionDecision.NEWER
        if event.transaction_total_order < last.transaction_total_order:
            return VersionDecision.LOWER_TRANSACTION_ORDER
    if _same_known_transport(event, last):
        return VersionDecision.EXACT_REPLAY
    return VersionDecision.EQUAL_POSITION_CONFLICT


def is_newer(event: CdcEvent, last: CdcEvent | None) -> bool:
    return version_decision(event, last) is VersionDecision.NEWER


def event_to_json(event: CdcEvent) -> str:
    return json.dumps(asdict(event), default=str, sort_keys=True)


def event_from_json(value: str) -> CdcEvent:
    return CdcEvent(**json.loads(value))


def apply_current_state(state: dict[str, dict[str, Any]], versions: dict[str, CdcEvent], event: CdcEvent | None) -> str:
    """Small in-memory oracle for tests; production uses ``KeyedProcessFunction``."""
    if event is None:
        return "tombstone_ignored"
    key = key_identity(event)
    decision = version_decision(event, versions.get(key))
    if decision is not VersionDecision.NEWER:
        return f"{decision.value}_ignored"
    versions[key] = event
    if event.operation == "d":
        state.pop(key, None)
        return "deleted"
    state[key] = dict(event.after or {})
    return "upserted"
