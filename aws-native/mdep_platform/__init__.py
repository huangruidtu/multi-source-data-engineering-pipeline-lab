"""Small reusable platform layer around the existing MDEP execution engines."""

from .observability import ExecutionEvent, JsonEventEmitter, ListEventSink
__all__ = [
    "ExecutionEvent",
    "JsonEventEmitter",
    "ListEventSink",
]
