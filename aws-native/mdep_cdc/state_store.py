"""SQLite reference writer with atomic current state, tombstone and checkpoint."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from .models import ApplyDecision, ApplyResult, DmsEvent, SourcePosition
from .ordering import decide


UTC = timezone.utc


class CdcStateStore:
    """Persistent offline correctness oracle; a row survives deletes as a tombstone."""

    def __init__(self, path: str | Path, *, clock: Callable[[], datetime] | None = None):
        self.path = str(path)
        self.clock = clock or (lambda: datetime.now(UTC))
        self.connection = sqlite3.connect(self.path)
        self.connection.row_factory = sqlite3.Row
        self._create()

    def close(self) -> None:
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()

    def _create(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS cdc_state (
              entity TEXT NOT NULL, primary_key TEXT NOT NULL, payload_json TEXT,
              is_deleted INTEGER NOT NULL, source_phase INTEGER NOT NULL,
              source_lsn TEXT, source_lsn_int TEXT, transaction_id TEXT,
              transaction_record_id INTEGER, dms_operation TEXT NOT NULL,
              source_event_at TEXT, applied_at TEXT NOT NULL, ingested_at TEXT NOT NULL,
              record_hash TEXT NOT NULL, event_hash TEXT NOT NULL, raw_event_json TEXT NOT NULL,
              PRIMARY KEY (entity, primary_key)
            );
            CREATE TABLE IF NOT EXISTS cdc_quarantine (
              quarantine_id TEXT PRIMARY KEY, entity TEXT, primary_key TEXT,
              reason TEXT NOT NULL, raw_event_json TEXT NOT NULL, observed_at TEXT NOT NULL
            );
            """
        )
        self.connection.commit()

    def apply(self, event: DmsEvent) -> ApplyResult:
        observed = self.clock().astimezone(UTC).isoformat().replace("+00:00", "Z")
        with self.connection:
            row = self.connection.execute(
                "SELECT * FROM cdc_state WHERE entity=? AND primary_key=?",
                (event.entity, event.primary_key),
            ).fetchone()
            if row is None:
                decision = ApplyDecision.APPLIED_DELETE if event.operation == "delete" else ApplyDecision.APPLIED_INSERT
            else:
                current = SourcePosition(
                    row["source_phase"], row["source_lsn"],
                    int(row["source_lsn_int"]) if row["source_lsn_int"] is not None else None,
                    row["transaction_id"], row["transaction_record_id"],
                )
                decision = decide(event, current, row["event_hash"])

            if decision in {ApplyDecision.STALE, ApplyDecision.CONFLICT, ApplyDecision.INCOMPARABLE}:
                reason = {
                    ApplyDecision.STALE: "stale_source_position",
                    ApplyDecision.CONFLICT: "same_position_different_content",
                    ApplyDecision.INCOMPARABLE: "incomparable_source_position",
                }[decision]
                quarantine_id = hashlib.sha256(
                    f"{event.entity}|{event.primary_key}|{event.event_hash}|{reason}".encode()
                ).hexdigest()
                self.connection.execute(
                    "INSERT OR IGNORE INTO cdc_quarantine VALUES (?, ?, ?, ?, ?, ?)",
                    (quarantine_id, event.entity, event.primary_key, reason, event.raw_json, observed),
                )
                return ApplyResult(decision, event.entity, event.primary_key, reason, quarantine_id)
            if decision == ApplyDecision.DUPLICATE:
                return ApplyResult(decision, event.entity, event.primary_key, "exact_replay")

            is_deleted = event.operation == "delete"
            payload_json = None if is_deleted else json.dumps(event.data, sort_keys=True, separators=(",", ":"), default=str)
            self.connection.execute(
                """INSERT INTO cdc_state VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(entity, primary_key) DO UPDATE SET
                     payload_json=excluded.payload_json, is_deleted=excluded.is_deleted,
                     source_phase=excluded.source_phase, source_lsn=excluded.source_lsn,
                     source_lsn_int=excluded.source_lsn_int, transaction_id=excluded.transaction_id,
                     transaction_record_id=excluded.transaction_record_id, dms_operation=excluded.dms_operation,
                     source_event_at=excluded.source_event_at, applied_at=excluded.applied_at,
                     ingested_at=excluded.ingested_at, record_hash=excluded.record_hash,
                     event_hash=excluded.event_hash, raw_event_json=excluded.raw_event_json""",
                (
                    event.entity, event.primary_key, payload_json, int(is_deleted), event.position.phase,
                    event.position.stream_position, str(event.position.lsn) if event.position.lsn is not None else None,
                    event.position.transaction_id, event.position.transaction_record_id, event.operation,
                    event.source_event_at, observed, observed, event.record_hash, event.event_hash, event.raw_json,
                ),
            )
            return ApplyResult(decision, event.entity, event.primary_key, "committed")

    def row(self, entity: str, primary_key: str):
        return self.connection.execute(
            "SELECT * FROM cdc_state WHERE entity=? AND primary_key=?", (entity, primary_key)
        ).fetchone()

    def live_rows(self, entity: str) -> list[sqlite3.Row]:
        return self.connection.execute(
            "SELECT * FROM cdc_state WHERE entity=? AND is_deleted=0 ORDER BY primary_key", (entity,)
        ).fetchall()

    def quarantine_rows(self) -> list[sqlite3.Row]:
        return self.connection.execute("SELECT * FROM cdc_quarantine ORDER BY quarantine_id").fetchall()
