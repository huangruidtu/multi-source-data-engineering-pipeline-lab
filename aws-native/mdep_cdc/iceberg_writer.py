"""Real Spark/Iceberg v2 canonical writer using the shared AWS-05 oracle."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from uuid import uuid4

from .models import ApplyDecision, ApplyResult, DmsEvent, ENTITY_CONTRACTS, SourcePosition
from .ordering import decide


UTC = timezone.utc

BUSINESS_COLUMNS = {
    "customers": [
        ("customer_id", "STRING"), ("customer_name", "STRING"), ("email", "STRING"),
        ("customer_status", "STRING"), ("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP"),
    ],
    "products": [
        ("product_id", "STRING"), ("product_name", "STRING"), ("category_code", "STRING"),
        ("unit_price", "DECIMAL(18,2)"), ("currency", "STRING"), ("updated_at", "TIMESTAMP"),
    ],
    "orders": [
        ("order_id", "STRING"), ("customer_id", "STRING"), ("order_status", "STRING"),
        ("order_ts", "TIMESTAMP"), ("currency", "STRING"), ("order_total", "DECIMAL(18,2)"),
        ("updated_at", "TIMESTAMP"),
    ],
    "payments": [
        ("payment_id", "STRING"), ("order_id", "STRING"), ("payment_status", "STRING"),
        ("payment_ts", "TIMESTAMP"), ("amount", "DECIMAL(18,2)"), ("currency", "STRING"),
        ("authorization_code", "STRING"), ("updated_at", "TIMESTAMP"),
    ],
}

COMMON_COLUMNS = [
    ("payload_json", "STRING"), ("is_deleted", "BOOLEAN"),
    ("source_schema", "STRING"), ("source_table", "STRING"), ("primary_key_value", "STRING"),
    ("source_lsn", "STRING"),
    ("source_lsn_int", "DECIMAL(20,0)"), ("transaction_id", "STRING"),
    ("transaction_record_id", "BIGINT"), ("dms_operation", "STRING"),
    ("source_event_at", "TIMESTAMP"), ("applied_at", "TIMESTAMP"),
    ("ingested_at", "TIMESTAMP"), ("record_hash", "STRING"), ("event_hash", "STRING"),
    ("raw_event_json", "STRING"),
]


def create_cdc_tables(spark, *, catalog: str = "local", namespace: str = "mdep_cdc") -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")
    for entity, columns in BUSINESS_COLUMNS.items():
        definitions = ",\n".join(f"{name} {data_type}" for name, data_type in columns + COMMON_COLUMNS)
        spark.sql(
            f"""CREATE TABLE IF NOT EXISTS {catalog}.{namespace}.core_{entity} (
              {definitions}
            ) USING iceberg TBLPROPERTIES ('format-version'='2')"""
        )
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {catalog}.{namespace}.cdc_quarantine (
          quarantine_id STRING NOT NULL, entity STRING NOT NULL, primary_key STRING NOT NULL,
          rejection_reason STRING NOT NULL, source_lsn STRING, transaction_id STRING,
          transaction_record_id BIGINT, raw_event_json STRING NOT NULL, observed_at TIMESTAMP NOT NULL
        ) USING iceberg TBLPROPERTIES ('format-version'='2')"""
    )


def _current_position(row) -> SourcePosition:
    lsn_int = row["source_lsn_int"]
    return SourcePosition(
        0 if row["source_lsn"] == "SNAPSHOT" else 1,
        None if row["source_lsn"] == "SNAPSHOT" else row["source_lsn"],
        int(lsn_int) if lsn_int is not None else None,
        row["transaction_id"],
        row["transaction_record_id"],
    )


def _quarantine(spark, event: DmsEvent, reason: str, observed_at: str, table: str) -> str:
    qid = hashlib.sha256(f"{event.entity}|{event.primary_key}|{event.event_hash}|{reason}".encode()).hexdigest()
    view = "cdc_quarantine_" + uuid4().hex
    spark.createDataFrame(
        [(qid, event.entity, event.primary_key, reason, event.position.source_lsn,
          event.position.transaction_id, event.position.transaction_record_id, event.raw_json, observed_at)],
        "quarantine_id string, entity string, primary_key string, rejection_reason string, source_lsn string, "
        "transaction_id string, transaction_record_id long, raw_event_json string, observed_at string",
    ).createOrReplaceTempView(view)
    spark.sql(
        f"""MERGE INTO {table} target
        USING (SELECT quarantine_id, entity, primary_key, rejection_reason, source_lsn,
          transaction_id, transaction_record_id, raw_event_json, CAST(observed_at AS TIMESTAMP) observed_at
          FROM {view}) incoming
        ON target.quarantine_id = incoming.quarantine_id
        WHEN NOT MATCHED THEN INSERT *"""
    )
    spark.catalog.dropTempView(view)
    return qid


def apply_iceberg_event(
    spark,
    event: DmsEvent,
    *,
    catalog: str = "local",
    namespace: str = "mdep_cdc",
    observed_at: datetime | None = None,
) -> ApplyResult:
    """Apply one event with one atomic Iceberg MERGE to its entity ledger."""
    from pyspark.sql import functions as F

    if event.entity not in BUSINESS_COLUMNS:
        raise ValueError(f"unsupported CDC entity: {event.entity}")
    observed = (observed_at or datetime.now(UTC)).astimezone(UTC).isoformat().replace("+00:00", "Z")
    state_table = f"{catalog}.{namespace}.core_{event.entity}"
    quarantine_table = f"{catalog}.{namespace}.cdc_quarantine"
    create_cdc_tables(spark, catalog=catalog, namespace=namespace)
    current = spark.table(state_table).where(F.col(event.primary_key_name) == event.primary_key).first()
    if current is None:
        decision = ApplyDecision.APPLIED_DELETE if event.operation == "delete" else ApplyDecision.APPLIED_INSERT
    else:
        decision = decide(event, _current_position(current), current["event_hash"])
    if decision == ApplyDecision.DUPLICATE:
        return ApplyResult(decision, event.entity, event.primary_key, "exact_replay")
    if decision in {ApplyDecision.STALE, ApplyDecision.CONFLICT, ApplyDecision.INCOMPARABLE}:
        reason = {
            ApplyDecision.STALE: "stale_source_position",
            ApplyDecision.CONFLICT: "same_position_different_content",
            ApplyDecision.INCOMPARABLE: "incomparable_source_position",
        }[decision]
        qid = _quarantine(spark, event, reason, observed, quarantine_table)
        return ApplyResult(decision, event.entity, event.primary_key, reason, qid)

    import json

    row = {name: None for name, _ in BUSINESS_COLUMNS[event.entity]}
    row.update({name: str(value) if value is not None else None for name, value in event.data.items() if name in row})
    row[event.primary_key_name] = event.primary_key
    row.update(
        {
            "payload_json": None if event.operation == "delete" else json.dumps(event.data, sort_keys=True, separators=(",", ":"), default=str),
            "is_deleted": event.operation == "delete",
            "source_schema": str(event.metadata["schema-name"]),
            "source_table": str(event.metadata["table-name"]),
            "primary_key_value": event.primary_key,
            "source_lsn": event.position.source_lsn,
            "source_lsn_int": str(event.position.lsn) if event.position.lsn is not None else None,
            "transaction_id": event.position.transaction_id,
            "transaction_record_id": event.position.transaction_record_id,
            "dms_operation": event.operation,
            "source_event_at": event.source_event_at,
            "applied_at": observed,
            "ingested_at": observed,
            "record_hash": event.record_hash,
            "event_hash": event.event_hash,
            "raw_event_json": event.raw_json,
        }
    )
    all_columns = BUSINESS_COLUMNS[event.entity] + COMMON_COLUMNS
    source_schema = ", ".join(
        f"{name} {'boolean' if data_type == 'BOOLEAN' else 'long' if data_type == 'BIGINT' else 'string'}"
        for name, data_type in all_columns
    )
    view = "cdc_incoming_" + uuid4().hex
    spark.createDataFrame([tuple(row[name] for name, _ in all_columns)], source_schema).createOrReplaceTempView(view)
    typed = ", ".join(f"CAST({name} AS {data_type}) AS {name}" for name, data_type in all_columns)
    spark.sql(
        f"""MERGE INTO {state_table} target
        USING (SELECT {typed} FROM {view}) incoming
        ON target.{event.primary_key_name} = incoming.{event.primary_key_name}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *"""
    )
    spark.catalog.dropTempView(view)
    return ApplyResult(decision, event.entity, event.primary_key, "committed")
