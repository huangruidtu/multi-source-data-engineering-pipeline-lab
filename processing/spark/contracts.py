"""Pure, testable rules shared by the MDEP-9 Spark job.

These functions deliberately describe only the two batch-owned REST reference
datasets.  They are not a generic contract framework and must not be used for
the CDC-owned PostgreSQL current-state entities.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from json import dumps
from typing import Any


BATCH_SILVER_ENTITIES = frozenset({"exchange_rates", "locations"})
CDC_OWNED_ENTITIES = frozenset({"customers", "products", "orders", "payments"})
COUNTRY_REGIONS = {"DK": "nordics", "NO": "nordics"}


@dataclass(frozen=True)
class RuleResult:
    normalized: dict[str, Any] | None
    rejection_reason: str | None


def deterministic_silver_key(entity: str, record: dict[str, Any]) -> str:
    """Return the documented natural key; reject accidental CDC ownership."""
    if entity not in BATCH_SILVER_ENTITIES:
        raise ValueError(f"{entity!r} is not a batch-owned MDEP-9 Silver entity")
    if entity == "exchange_rates":
        return "|".join(str(record.get(field, "")).strip().upper() for field in ("rate_date", "base_currency", "quote_currency"))
    return str(record.get("location_id", "")).strip()


def canonical_record_hash(record: dict[str, Any]) -> str:
    return sha256(dumps(record, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")).hexdigest()


def in_incremental_boundary(value: str | datetime | None, start: datetime, end: datetime) -> bool:
    if value is None:
        return False
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return start <= parsed < end


def normalize_exchange_rate(record: dict[str, Any]) -> RuleResult:
    required = ("rate_date", "base_currency", "quote_currency", "rate", "retrieved_at")
    missing = [field for field in required if record.get(field) in (None, "")]
    if missing:
        return RuleResult(None, "missing_required:" + ",".join(missing))
    try:
        rate_date = date.fromisoformat(str(record["rate_date"]))
        rate = Decimal(str(record["rate"]))
        datetime.fromisoformat(str(record["retrieved_at"]).replace("Z", "+00:00"))
    except (ValueError, InvalidOperation):
        return RuleResult(None, "invalid_type:rate_date_or_rate_or_retrieved_at")
    base, quote = str(record["base_currency"]).strip().upper(), str(record["quote_currency"]).strip().upper()
    if len(base) != 3 or len(quote) != 3:
        return RuleResult(None, "invalid_currency_code")
    if base == quote:
        return RuleResult(None, "base_currency_equals_quote_currency")
    if rate <= 0:
        return RuleResult(None, "non_positive_rate")
    return RuleResult({**record, "rate_date": rate_date.isoformat(), "base_currency": base, "quote_currency": quote, "rate": str(rate)}, None)


def normalize_location(record: dict[str, Any]) -> RuleResult:
    required = ("location_id", "location_name", "country_code", "timezone", "updated_at")
    missing = [field for field in required if record.get(field) in (None, "")]
    if missing:
        return RuleResult(None, "missing_required:" + ",".join(missing))
    try:
        datetime.fromisoformat(str(record["updated_at"]).replace("Z", "+00:00"))
    except ValueError:
        return RuleResult(None, "invalid_type:updated_at")
    country = str(record["country_code"]).strip().upper()
    if country not in COUNTRY_REGIONS:
        return RuleResult(None, "unknown_country_reference")
    return RuleResult({**record, "location_id": str(record["location_id"]).strip(), "country_code": country, "region": COUNTRY_REGIONS[country]}, None)
