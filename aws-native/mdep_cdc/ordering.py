"""Single source-position ordering oracle used by every AWS-05 writer."""

from __future__ import annotations

from .models import ApplyDecision, DmsEvent, SourcePosition


def compare_positions(incoming: SourcePosition, current: SourcePosition) -> int | None:
    """Return -1/0/1, or None when equal LSNs lack a defensible total order."""
    if incoming.phase != current.phase:
        return 1 if incoming.phase > current.phase else -1
    if incoming.phase == 0:
        return 0
    assert incoming.lsn is not None and current.lsn is not None
    if incoming.lsn != current.lsn:
        return 1 if incoming.lsn > current.lsn else -1
    if incoming.transaction_id != current.transaction_id:
        return None
    if incoming.transaction_record_id is None or current.transaction_record_id is None:
        return None
    if incoming.transaction_record_id == current.transaction_record_id:
        return 0
    return 1 if incoming.transaction_record_id > current.transaction_record_id else -1


def decide(incoming: DmsEvent, current_position: SourcePosition, current_event_hash: str) -> ApplyDecision:
    order = compare_positions(incoming.position, current_position)
    if order is None:
        return ApplyDecision.INCOMPARABLE
    if order < 0:
        return ApplyDecision.STALE
    if order == 0:
        return ApplyDecision.DUPLICATE if incoming.event_hash == current_event_hash else ApplyDecision.CONFLICT
    return ApplyDecision.APPLIED_DELETE if incoming.operation == "delete" else ApplyDecision.APPLIED_UPDATE
