"""Physical validation needed before source-aligned records enter Bronze."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from .models import SourceError


@dataclass(frozen=True)
class EntityContract:
    required: tuple[str, ...]
    record_key: tuple[str, ...]
    extract_timestamp: str | None


CONTRACTS = {
    "customers": EntityContract(("customer_id", "customer_name", "customer_status", "created_at", "updated_at"), ("customer_id",), "updated_at"),
    "products": EntityContract(("product_id", "product_name", "category_code", "unit_price", "currency", "updated_at"), ("product_id",), "updated_at"),
    "orders": EntityContract(("order_id", "customer_id", "order_status", "order_ts", "currency", "order_total", "updated_at"), ("order_id",), "updated_at"),
    "payments": EntityContract(("payment_id", "order_id", "payment_status", "payment_ts", "amount", "currency", "updated_at"), ("payment_id",), "updated_at"),
    "exchange_rates": EntityContract(("rate_date", "base_currency", "quote_currency", "rate", "retrieved_at"), ("rate_date", "base_currency", "quote_currency"), "retrieved_at"),
    "locations": EntityContract(("location_id", "location_name", "country_code", "timezone", "updated_at"), ("location_id",), "updated_at"),
    "product_categories": EntityContract(("category_code", "category_name", "sort_order"), ("category_code",), None),
    "devices": EntityContract(("device_id", "location_id", "device_type", "device_status", "updated_at"), ("device_id",), "updated_at"),
}


def validate_record(entity: str, record: dict[str, Any], locator: str) -> EntityContract:
    contract = CONTRACTS.get(entity)
    if contract is None:
        raise SourceError("unsupported_entity", locator, entity)
    missing = [field for field in contract.required if record.get(field) in (None, "")]
    if missing:
        raise SourceError("invalid_source_schema", locator, {"missing": missing, "record": record})
    if entity in {"products", "orders", "payments", "exchange_rates"}:
        numeric_field = {"products": "unit_price", "orders": "order_total", "payments": "amount", "exchange_rates": "rate"}[entity]
        try:
            if Decimal(str(record[numeric_field])) < 0:
                raise InvalidOperation
        except (InvalidOperation, ValueError):
            raise SourceError("invalid_source_schema", locator, {"field": numeric_field, "record": record}) from None
    if entity == "product_categories":
        try:
            int(str(record["sort_order"]))
        except ValueError:
            raise SourceError("invalid_source_schema", locator, {"field": "sort_order", "record": record}) from None
    for field in ("created_at", "updated_at", "order_ts", "payment_ts", "retrieved_at"):
        if field in record and record[field] not in (None, ""):
            try:
                datetime.fromisoformat(str(record[field]).replace("Z", "+00:00"))
            except ValueError:
                raise SourceError("invalid_source_schema", locator, {"field": field, "record": record}) from None
    return contract


def business_key(contract: EntityContract, record: dict[str, Any]) -> str:
    return "|".join(str(record[field]) for field in contract.record_key)
