"""Local evidence writer mirroring Firehose's newline-delimited S3 archive contract."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


def append_ndjson(root: str | Path, entity: str, record: Mapping[str, Any], *, error: bool = False) -> Path:
    prefix = "cdc-errors" if error else "cdc"
    path = Path(root) / "bronze" / prefix / entity / "records.ndjson"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
        stream.write("\n")
    return path
