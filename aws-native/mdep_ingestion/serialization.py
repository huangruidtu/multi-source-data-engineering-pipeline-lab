"""Real, deterministic Parquet serialization for complete source extractions."""

from __future__ import annotations

import io
from typing import Any

from .models import PreparedPayload, sha256_hex


def to_parquet(records: list[dict[str, Any]]) -> PreparedPayload:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as error:  # pragma: no cover - dependency installation is mandatory in CI
        raise RuntimeError("PyArrow is required for the AWS-02 core Parquet path.") from error
    if records:
        field_names = list(dict.fromkeys(field for record in records for field in record))
        table = pa.Table.from_pydict({field: [record.get(field) for record in records] for field in field_names})
    else:
        table = pa.table(
            {
                "ingestion_id": pa.array([], type=pa.string()),
                "source_name": pa.array([], type=pa.string()),
                "source_entity": pa.array([], type=pa.string()),
                "source_record_key": pa.array([], type=pa.string()),
                "source_extract_ts": pa.array([], type=pa.string()),
                "ingested_at": pa.array([], type=pa.string()),
                "source_version": pa.array([], type=pa.string()),
                "source_locator": pa.array([], type=pa.string()),
                "record_hash": pa.array([], type=pa.string()),
            }
        )
    sink = io.BytesIO()
    pq.write_table(
        table,
        sink,
        compression="snappy",
        use_dictionary=False,
        write_statistics=True,
        data_page_version="1.0",
    )
    data = sink.getvalue()
    return PreparedPayload(data=data, record_count=len(records), checksum=sha256_hex(data))


def read_parquet(data: bytes) -> list[dict[str, Any]]:
    import pyarrow.parquet as pq

    return pq.read_table(io.BytesIO(data)).to_pylist()
