"""Bounded, PII-safe structured execution events for actual platform boundaries."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Callable, Protocol


EVENT_VERSION = "mdep-execution-event-v1"
STATUSES = frozenset({"started", "completed", "failed", "conflicted", "noop", "recovered", "quarantined"})


class EventSink(Protocol):
    def __call__(self, serialized_event: str) -> None: ...


@dataclass(frozen=True)
class ExecutionEvent:
    component: str
    action: str
    source_id: str
    source_entity: str
    run_identity: str
    status: str
    event_timestamp: str
    record_count: int = 0
    rejected_count: int = 0
    duration_ms: int = 0
    error_code: str | None = None
    manifest_ref: str | None = None
    event_version: str = EVENT_VERSION

    def __post_init__(self) -> None:
        if self.status not in STATUSES:
            raise ValueError(f"unsupported execution status: {self.status}")
        if not all((self.component, self.action, self.source_id, self.source_entity, self.run_identity)):
            raise ValueError("execution event identity fields cannot be empty")
        if min(self.record_count, self.rejected_count, self.duration_ms) < 0:
            raise ValueError("execution event counts and duration cannot be negative")
        parsed = datetime.fromisoformat(self.event_timestamp.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("event_timestamp must include a UTC offset")

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))


class JsonEventEmitter:
    def __init__(self, sink: EventSink = print, *, clock: Callable[[], datetime] | None = None):
        self.sink = sink
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def emit(
        self,
        *,
        component: str,
        action: str,
        source_id: str,
        source_entity: str,
        run_identity: str,
        status: str,
        record_count: int = 0,
        rejected_count: int = 0,
        duration_ms: int = 0,
        error_code: str | None = None,
        manifest_ref: str | None = None,
    ) -> ExecutionEvent:
        timestamp = self.clock().astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        event = ExecutionEvent(
            component=component,
            action=action,
            source_id=source_id,
            source_entity=source_entity,
            run_identity=run_identity,
            status=status,
            event_timestamp=timestamp,
            record_count=record_count,
            rejected_count=rejected_count,
            duration_ms=duration_ms,
            error_code=error_code,
            manifest_ref=manifest_ref,
        )
        self.sink(event.to_json())
        return event


class ListEventSink:
    def __init__(self):
        self.lines: list[str] = []

    def __call__(self, serialized_event: str) -> None:
        self.lines.append(serialized_event)

    @property
    def events(self) -> list[dict]:
        return [json.loads(line) for line in self.lines]
