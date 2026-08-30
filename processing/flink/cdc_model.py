"""Pure CDC semantics used by the MDEP-11 Flink topology and tests."""
from __future__ import annotations
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

CDC_ENTITIES = frozenset({"customers", "products", "orders", "payments"})
PRIMARY_KEY = {"customers": "customer_id", "products": "product_id", "orders": "order_id", "payments": "payment_id"}

@dataclass(frozen=True)
class CdcEvent:
    entity: str; primary_key: str; operation: str; before: dict[str, Any] | None; after: dict[str, Any] | None
    source_lsn: int; source_tx_id: str | None; source_event_ts: str | None
    kafka_topic: str; kafka_partition: int; kafka_offset: int; snapshot: bool; envelope: str

def parse_lsn(value: str | int) -> int:
    if isinstance(value, int): return value
    high, low = str(value).split("/", 1)
    return (int(high, 16) << 32) + int(low, 16)

def parse_debezium(key_json: str, value_json: str | None, topic: str, partition: int, offset: int) -> CdcEvent | None:
    if value_json is None: return None  # Kafka tombstone: transport compaction marker, not a second delete.
    key, payload = json.loads(key_json), json.loads(value_json)
    entity = topic.rsplit(".", 1)[-1]
    if entity not in CDC_ENTITIES: raise ValueError("CDC entity is outside Flink ownership")
    op, source = payload.get("op"), payload.get("source") or {}
    if op not in {"r", "c", "u", "d"}: raise ValueError("unsupported Debezium operation")
    if op == "d" and payload.get("after") is not None: raise ValueError("delete must have after=null")
    primary = str(key[PRIMARY_KEY[entity]])
    return CdcEvent(entity, primary, op, payload.get("before"), payload.get("after"), parse_lsn(source["lsn"]), str(source.get("txId")) if source.get("txId") is not None else None, source.get("ts_ms"), topic, partition, offset, source.get("snapshot") in {True, "true", "last"}, value_json)

def is_newer(event: CdcEvent, last: CdcEvent | None) -> bool:
    """Source LSN is primary; Kafka partition/offset resolves equal-LSN transport replays."""
    if last is None: return True
    return (event.source_lsn, event.kafka_partition, event.kafka_offset) > (last.source_lsn, last.kafka_partition, last.kafka_offset)

def apply_current_state(state: dict[str, dict[str, Any]], versions: dict[str, CdcEvent], event: CdcEvent | None) -> str:
    if event is None: return "tombstone_ignored"
    last = versions.get(event.primary_key)
    if not is_newer(event, last): return "stale_or_duplicate_ignored"
    versions[event.primary_key] = event
    if event.operation == "d": state.pop(event.primary_key, None); return "deleted"
    state[event.primary_key] = dict(event.after or {}); return "upserted"
