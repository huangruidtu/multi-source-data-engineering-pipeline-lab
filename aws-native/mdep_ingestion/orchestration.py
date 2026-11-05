"""Airflow-independent run-interval resolution for scheduled and manual runs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from .models import BatchContext, parse_utc


@dataclass(frozen=True)
class ResolvedRun:
    logical_date: date
    interval_start: datetime
    interval_end: datetime

    def context(self, source_name: str, source_entity: str) -> BatchContext:
        return BatchContext(
            source_name=source_name,
            source_entity=source_entity,
            logical_date=self.logical_date,
            interval_start=self.interval_start,
            interval_end=self.interval_end,
        )


def _logical_date(value: str | date | datetime) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(value[:10])


def resolve_run(
    *,
    logical_date: str | date | datetime,
    data_interval_start: str | datetime,
    data_interval_end: str | datetime,
    backfill_start: str | datetime | None = None,
    backfill_end: str | datetime | None = None,
) -> ResolvedRun:
    """Resolve one explicit ``[start, end)`` interval without conflating it with logical date."""

    if bool(backfill_start) != bool(backfill_end):
        raise ValueError("backfill_start and backfill_end must be supplied together")
    start = parse_utc(backfill_start or data_interval_start)
    end = parse_utc(backfill_end or data_interval_end)
    if start >= end:
        raise ValueError("the target data interval must be non-empty and use [start, end) order")
    return ResolvedRun(_logical_date(logical_date), start, end)
