"""Small pure helpers for MDEP-10 CDC transport semantics."""
from __future__ import annotations

from typing import Any


CDC_TABLES = ("customers", "products", "orders", "payments")
PRIMARY_KEYS = {"customers": ("customer_id",), "products": ("product_id",), "orders": ("order_id",), "payments": ("payment_id",)}
OPERATIONS = {"r": "snapshot_read", "c": "create", "u": "update", "d": "delete"}


def topic_name(table: str, prefix: str = "mdep", schema: str = "commerce") -> str:
    if table not in CDC_TABLES:
        raise ValueError(f"{table!r} is not an MDEP-10 CDC-owned table")
    return f"{prefix}.{schema}.{table}"


def primary_key(table: str, key: dict[str, Any]) -> tuple[Any, ...]:
    fields = PRIMARY_KEYS.get(table)
    if not fields or any(field not in key for field in fields):
        raise ValueError(f"Kafka key does not contain the primary key for {table}")
    return tuple(key[field] for field in fields)


def classify_envelope(value: dict[str, Any]) -> str:
    operation = value.get("op")
    if operation not in OPERATIONS:
        raise ValueError("Unsupported or missing Debezium operation code")
    if operation == "d" and value.get("after") is not None:
        raise ValueError("A Debezium delete event must have after=null")
    return OPERATIONS[operation]
