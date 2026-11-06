"""Run the deterministic AWS-05 offline CDC walkthrough without AWS."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

AWS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(AWS_ROOT))

from mdep_cdc.dms_adapter import DmsAdapter
from mdep_cdc.state_store import CdcStateStore


def record(operation: str, data: dict, lsn: str | None = None, txid: int | None = None) -> dict:
    metadata = {
        "timestamp": "2026-11-06T12:00:01.000000Z",
        "record-type": "data", "operation": operation,
        "partition-key-type": "primary-key", "partition-key-value": data["customer_id"],
        "schema-name": "commerce", "table-name": "customers",
        "commit-timestamp": "2026-11-06T12:00:00.000000Z",
    }
    if operation != "load":
        metadata.update({"stream-position": lsn, "transaction-id": txid, "transaction-record-id": 1})
    return {"data": data, "metadata": metadata}


def main() -> None:
    customer = {
        "customer_id": "cust-demo", "customer_name": "Ada", "email": "ada@example.test",
        "customer_status": "active", "created_at": "2026-11-01T00:00:00Z",
        "updated_at": "2026-11-01T00:00:00Z",
    }
    changed = {**customer, "customer_status": "suspended", "updated_at": "2026-11-06T12:00:00Z"}
    adapter = DmsAdapter()
    events = [
        adapter.parse(record("load", customer)),
        adapter.parse(record("update", changed, "16/B6C60", 731)),
        adapter.parse(record("update", changed, "16/B6C60", 731)),
        adapter.parse(record("delete", {"customer_id": "cust-demo"}, "16/B6C70", 732)),
        adapter.parse(record("update", changed, "16/B6C65", 731)),
    ]
    with tempfile.TemporaryDirectory() as directory, CdcStateStore(Path(directory) / "cdc.db") as state:
        for event in events:
            result = state.apply(event)
            print(json.dumps({"operation": event.operation, "source_lsn": event.position.source_lsn,
                              "decision": result.decision.value}, sort_keys=True))
        tombstone = state.row("customers", "cust-demo")
        print(json.dumps({"final_is_deleted": bool(tombstone["is_deleted"]),
                          "final_source_lsn": tombstone["source_lsn"],
                          "quarantine_count": len(state.quarantine_rows())}, sort_keys=True))


if __name__ == "__main__":
    main()
