"""Data model shared by the DMS adapter and persistent state writers."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping


@dataclass(frozen=True)
class EntityContract:
    table: str
    entity: str
    primary_key: str
    required_upsert_fields: frozenset[str]


ENTITY_CONTRACTS = {
    "customers": EntityContract(
        "customers", "customers", "customer_id",
        frozenset({"customer_id", "customer_name", "customer_status", "created_at", "updated_at"}),
    ),
    "products": EntityContract(
        "products", "products", "product_id",
        frozenset({"product_id", "product_name", "category_code", "unit_price", "currency", "updated_at"}),
    ),
    "orders": EntityContract(
        "orders", "orders", "order_id",
        frozenset({"order_id", "customer_id", "order_status", "order_ts", "currency", "order_total", "updated_at"}),
    ),
    "payments": EntityContract(
        "payments", "payments", "payment_id",
        frozenset({"payment_id", "order_id", "payment_status", "payment_ts", "amount", "currency", "updated_at"}),
    ),
}


@dataclass(frozen=True)
class SourcePosition:
    """A comparable PostgreSQL coordinate; snapshot precedes every CDC LSN."""

    phase: int
    stream_position: str | None
    lsn: int | None
    transaction_id: str | None
    transaction_record_id: int | None

    @property
    def source_lsn(self) -> str:
        return self.stream_position or "SNAPSHOT"


@dataclass(frozen=True)
class DmsEvent:
    entity: str
    primary_key_name: str
    primary_key: str
    operation: str
    data: Mapping[str, Any]
    metadata: Mapping[str, Any]
    position: SourcePosition
    source_event_at: str | None
    raw_json: str
    record_hash: str
    event_hash: str


class ApplyDecision(str, Enum):
    APPLIED_INSERT = "applied_insert"
    APPLIED_UPDATE = "applied_update"
    APPLIED_DELETE = "applied_delete"
    DUPLICATE = "duplicate"
    STALE = "stale"
    CONFLICT = "conflict"
    INCOMPARABLE = "incomparable"


@dataclass(frozen=True)
class ApplyResult:
    decision: ApplyDecision
    entity: str
    primary_key: str
    reason: str
    quarantine_id: str | None = None
