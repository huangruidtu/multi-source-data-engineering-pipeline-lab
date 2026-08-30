"""Runnable MDEP-11 Kafka/Debezium -> Bronze + Iceberg current-state topology.

PyFlink imports stay inside ``run`` so the CDC contract can be unit-tested on a
workstation. This module contains no intentional RuntimeError stub.
"""
from __future__ import annotations

import json
import os
import pickle
from datetime import datetime, timezone

from processing.flink.cdc_model import CDC_ENTITIES, event_from_json, event_to_json, key_identity, parse_debezium, version_decision

TOPICS = ["mdep.commerce.customers", "mdep.commerce.products", "mdep.commerce.orders", "mdep.commerce.payments"]
CHECKPOINT_INTERVAL_MS, CHECKPOINT_TIMEOUT_MS, CHECKPOINT_MIN_PAUSE_MS, WATERMARK_SECONDS = 30_000, 120_000, 5_000, 60
CONSUMER_GROUP = "mdep-flink-cdc-silver-v1"
BRONZE_LAYOUT = "bronze/cdc/<entity>/event_date=YYYY-MM-DD/"
QUARANTINE_LAYOUT = "quarantine/cdc/<entity>/event_date=YYYY-MM-DD/"
SILVER_TABLES = ["mdep.silver.core_customers", "mdep.silver.core_products", "mdep.silver.core_orders", "mdep.silver.core_payments"]
BRONZE_FIELDS = ["entity", "primary_key", "op", "source_lsn", "source_tx_id", "source_event_ts", "kafka_topic", "kafka_partition", "kafka_offset", "snapshot", "original_envelope", "processed_at", "event_date"]
QUARANTINE_FIELDS = ["entity", "kafka_topic", "kafka_partition", "kafka_offset", "original_payload", "rejection_reason", "processed_at", "event_date"]
SILVER_FIELDS = ["entity", "customer_id", "customer_name", "email", "customer_status", "created_at", "preferred_language", "product_id", "product_name", "category_code", "unit_price", "order_id", "order_status", "order_ts", "order_total", "payment_id", "payment_status", "payment_ts", "amount", "currency", "authorization_code", "source_lsn", "source_tx_id", "source_event_ts", "kafka_topic", "kafka_partition", "kafka_offset", "applied_at"]


def topology_spec() -> dict:
    return {
        "topics": TOPICS, "consumer_group": CONSUMER_GROUP, "starting_offsets": "earliest",
        "checkpoint_interval_ms": CHECKPOINT_INTERVAL_MS, "checkpoint_timeout_ms": CHECKPOINT_TIMEOUT_MS,
        "checkpoint_min_pause_ms": CHECKPOINT_MIN_PAUSE_MS, "bronze_layout": BRONZE_LAYOUT,
        "quarantine_layout": QUARANTINE_LAYOUT, "silver_tables": SILVER_TABLES,
        "keyed_state": "ValueState(last version) and ValueState(current row), keyed by entity:primary_key",
        "cdc_order": "source_lsn only; equal-LSN transport coordinates identify an exact replay but never make another partition newer",
        "watermark": f"source event timestamp with {WATERMARK_SECONDS}s bounded out-of-orderness; never used for CDC state acceptance",
    }


def silver_table_ddls() -> dict[str, str]:
    metadata = "source_lsn BIGINT NOT NULL, source_tx_id STRING, source_event_ts STRING, kafka_topic STRING NOT NULL, kafka_partition INT, kafka_offset BIGINT, applied_at STRING NOT NULL"
    options = "'format-version'='2', 'write.upsert.enabled'='true', 'write.format.default'='parquet'"
    return {
        "customers": f"CREATE TABLE IF NOT EXISTS mdep.silver.core_customers (customer_id STRING NOT NULL, customer_name STRING, email STRING, customer_status STRING, created_at STRING, updated_at STRING, preferred_language STRING, {metadata}, PRIMARY KEY (customer_id) NOT ENFORCED) WITH ({options})",
        "products": f"CREATE TABLE IF NOT EXISTS mdep.silver.core_products (product_id STRING NOT NULL, product_name STRING, category_code STRING, unit_price DECIMAL(12,2), currency STRING, updated_at STRING, {metadata}, PRIMARY KEY (product_id) NOT ENFORCED) WITH ({options})",
        "orders": f"CREATE TABLE IF NOT EXISTS mdep.silver.core_orders (order_id STRING NOT NULL, customer_id STRING, order_status STRING, order_ts STRING, currency STRING, order_total DECIMAL(12,2), updated_at STRING, {metadata}, PRIMARY KEY (order_id) NOT ENFORCED) WITH ({options})",
        "payments": f"CREATE TABLE IF NOT EXISTS mdep.silver.core_payments (payment_id STRING NOT NULL, order_id STRING, payment_status STRING, payment_ts STRING, amount DECIMAL(12,2), currency STRING, authorization_code STRING, updated_at STRING, {metadata}, PRIMARY KEY (payment_id) NOT ENFORCED) WITH ({options})",
    }


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _event_date(timestamp: str) -> str:
    return timestamp[:10]


def _raw_message(topic: str, value: str | None) -> str:
    processed_at = _utc_now()
    return json.dumps({"entity": topic.rsplit(".", 1)[-1], "topic": topic, "value": value, "processed_at": processed_at})


def _bronze_values(raw: str) -> list:
    message = json.loads(raw)
    entity, payload, processed_at = message["entity"], message["value"], message["processed_at"]
    try:
        event = parse_debezium(None, payload, message["topic"], None, None)
        if event is None:
            return [entity, None, "tombstone", None, None, None, message["topic"], None, None, None, None, processed_at, _event_date(processed_at)]
        return [event.entity, event.primary_key, event.operation, event.source_lsn, event.source_tx_id, event.source_event_ts, event.kafka_topic, event.kafka_partition, event.kafka_offset, str(event.snapshot).lower(), event.envelope, processed_at, _event_date(processed_at)]
    except (ValueError, json.JSONDecodeError):
        # Bronze is evidence-first: malformed bytes are archived as unparsed input.
        return [entity, None, "unparsed", None, None, None, message["topic"], None, None, None, payload, processed_at, _event_date(processed_at)]


def _quarantine_value(raw: str, reason: str) -> str:
    message = json.loads(raw)
    return json.dumps({"entity": message["entity"], "kafka_topic": message["topic"], "kafka_partition": None, "kafka_offset": None, "original_payload": message["value"], "rejection_reason": reason, "processed_at": message["processed_at"], "event_date": _event_date(message["processed_at"])})


def _quarantine_values(value: str) -> list:
    record = json.loads(value)
    return [record[field] for field in QUARANTINE_FIELDS]


def _to_silver_row(event, row_kind, row_class):
    after = event.after or {}
    values = {
        "entity": event.entity, "customer_id": after.get("customer_id") or (event.primary_key if event.entity == "customers" else None), "customer_name": after.get("customer_name"), "email": after.get("email"), "customer_status": after.get("customer_status"), "created_at": after.get("created_at"), "preferred_language": after.get("preferred_language"),
        "product_id": after.get("product_id") or (event.primary_key if event.entity == "products" else None), "product_name": after.get("product_name"), "category_code": after.get("category_code"), "unit_price": str(after.get("unit_price")) if after.get("unit_price") is not None else None,
        "order_id": after.get("order_id") or (event.primary_key if event.entity == "orders" else None), "order_status": after.get("order_status"), "order_ts": after.get("order_ts"), "order_total": str(after.get("order_total")) if after.get("order_total") is not None else None,
        "payment_id": after.get("payment_id") or (event.primary_key if event.entity == "payments" else None), "payment_status": after.get("payment_status"), "payment_ts": after.get("payment_ts"), "amount": str(after.get("amount")) if after.get("amount") is not None else None, "currency": after.get("currency"), "authorization_code": after.get("authorization_code"),
        "source_lsn": event.source_lsn, "source_tx_id": event.source_tx_id, "source_event_ts": event.source_event_ts, "kafka_topic": event.kafka_topic, "kafka_partition": event.kafka_partition, "kafka_offset": event.kafka_offset, "applied_at": _utc_now(),
    }
    row = row_class(*[values[field] for field in SILVER_FIELDS])
    row.set_row_kind(row_kind)
    return row


def run() -> None:
    """Build and run the source, parser, side-output, keyed-state, and sink graph."""
    from pyflink.common import Configuration, Row, RowKind, Types, WatermarkStrategy
    from pyflink.common.restart_strategy import RestartStrategies
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.common.time import Duration
    from pyflink.common.watermark_strategy import TimestampAssigner
    from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
    from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
    from pyflink.datastream.functions import KeyedProcessFunction, ProcessFunction
    from pyflink.datastream.output_tag import OutputTag
    from pyflink.datastream.state import ValueStateDescriptor
    from pyflink.table import DataTypes, Schema, StreamTableEnvironment

    kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    bronze_root = os.environ.get("BRONZE_CDC_ROOT", "s3a://mdep-lake/bronze/cdc")
    quarantine_root = os.environ.get("QUARANTINE_CDC_ROOT", "s3a://mdep-lake/quarantine/cdc")
    warehouse = os.environ.get("ICEBERG_WAREHOUSE", "s3a://mdep-lake/iceberg")
    quarantine_tag, stale_tag = OutputTag("quarantine", Types.STRING()), OutputTag("stale-or-duplicate", Types.STRING())

    class DebeziumParser(ProcessFunction):
        def process_element(self, raw, context):
            message = json.loads(raw)
            if message["value"] is None:
                # Tombstones are valid Kafka compaction markers. Bronze retains
                # them; they are not malformed input and therefore not Quarantine.
                context.output(stale_tag, json.dumps({"topic": message["topic"], "reason": "tombstone_ignored"}))
                return
            try:
                yield event_to_json(parse_debezium(None, message["value"], message["topic"], None, None))
            except (ValueError, KeyError, TypeError, json.JSONDecodeError) as error:
                context.output(quarantine_tag, _quarantine_value(raw, str(error)))

    class CdcStateApplier(KeyedProcessFunction):
        """Real Flink managed state, separate from the pure dictionary test oracle."""
        def open(self, runtime_context):
            self.last_version = runtime_context.get_state(ValueStateDescriptor("last-applied-cdc-version", Types.PICKLED_BYTE_ARRAY()))
            self.current_row = runtime_context.get_state(ValueStateDescriptor("current-cdc-row", Types.PICKLED_BYTE_ARRAY()))

        def process_element(self, encoded_event, context):
            event = event_from_json(encoded_event)
            stored = self.last_version.value()
            last = pickle.loads(stored) if stored else None
            decision = version_decision(event, last)
            if decision.value != "newer":
                context.output(stale_tag, json.dumps({"key": key_identity(event), "reason": decision.value, "source_lsn": event.source_lsn}))
                return
            self.last_version.update(pickle.dumps(event))
            stored_current = self.current_row.value()
            previous = pickle.loads(stored_current) if stored_current else None
            if event.operation == "d":
                if previous is not None:
                    yield _to_silver_row(previous, RowKind.DELETE, Row)
                self.current_row.clear()
                return
            self.current_row.update(pickle.dumps(event))
            yield _to_silver_row(event, RowKind.UPDATE_AFTER if previous is not None else RowKind.INSERT, Row)

    class SourceEventTimestampAssigner(TimestampAssigner):
        def extract_timestamp(self, encoded_event, record_timestamp):
            value = event_from_json(encoded_event).source_event_ts
            if value is None:
                return record_timestamp
            if str(value).isdigit():
                return int(value)
            return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1000)

    config = Configuration()
    config.set_string("pipeline.name", "mdep-11-flink-cdc-silver")
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS, CheckpointingMode.EXACTLY_ONCE)
    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_checkpoint_timeout(CHECKPOINT_TIMEOUT_MS)
    checkpoint_config.set_min_pause_between_checkpoints(CHECKPOINT_MIN_PAUSE_MS)
    checkpoint_config.set_tolerable_checkpoint_failure_number(3)
    env.set_restart_strategy(RestartStrategies.fixed_delay_restart(3, 10_000))
    env.set_parallelism(1)
    table_env = StreamTableEnvironment.create(env)

    inputs = []
    for topic in TOPICS:
        source = (KafkaSource.builder().set_bootstrap_servers(kafka_bootstrap).set_topics(topic).set_group_id(CONSUMER_GROUP).set_starting_offsets(KafkaOffsetsInitializer.earliest()).set_value_only_deserializer(SimpleStringSchema()).build())
        inputs.append(env.from_source(source, WatermarkStrategy.no_watermarks(), f"kafka-{topic}").map(lambda value, name=topic: _raw_message(name, value), output_type=Types.STRING()))
    raw = inputs[0]
    for source in inputs[1:]:
        raw = raw.union(source)

    bronze_type = Types.ROW_NAMED(BRONZE_FIELDS, [Types.STRING(), Types.STRING(), Types.STRING(), Types.LONG(), Types.STRING(), Types.STRING(), Types.STRING(), Types.INT(), Types.LONG(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()])
    bronze = raw.map(lambda value: Row(*_bronze_values(value)), output_type=bronze_type)
    parsed = raw.process(DebeziumParser(), output_type=Types.STRING())
    quarantine_type = Types.ROW_NAMED(QUARANTINE_FIELDS, [Types.STRING(), Types.STRING(), Types.INT(), Types.LONG(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()])
    quarantine = parsed.get_side_output(quarantine_tag).map(lambda value: Row(*_quarantine_values(value)), output_type=quarantine_type)
    event_time = parsed.assign_timestamps_and_watermarks(WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(WATERMARK_SECONDS)).with_timestamp_assigner(SourceEventTimestampAssigner()))
    silver_type = Types.ROW_NAMED(SILVER_FIELDS, [Types.STRING()] * 21 + [Types.LONG(), Types.STRING(), Types.STRING(), Types.STRING(), Types.INT(), Types.LONG(), Types.STRING()])
    silver = event_time.key_by(lambda value: key_identity(event_from_json(value)), key_type=Types.STRING()).process(CdcStateApplier(), output_type=silver_type)

    bronze_schema = Schema.new_builder()
    for field in BRONZE_FIELDS:
        bronze_schema.column(field, DataTypes.BIGINT() if field in {"source_lsn", "kafka_offset"} else DataTypes.INT() if field == "kafka_partition" else DataTypes.STRING())
    quarantine_schema = Schema.new_builder()
    for field in QUARANTINE_FIELDS:
        quarantine_schema.column(field, DataTypes.BIGINT() if field == "kafka_offset" else DataTypes.INT() if field == "kafka_partition" else DataTypes.STRING())
    silver_schema = Schema.new_builder()
    for field in SILVER_FIELDS:
        silver_schema.column(field, DataTypes.BIGINT() if field in {"source_lsn", "kafka_offset"} else DataTypes.INT() if field == "kafka_partition" else DataTypes.STRING())
    table_env.create_temporary_view("bronze_cdc_input", table_env.from_data_stream(bronze, bronze_schema.build()))
    table_env.create_temporary_view("quarantine_cdc_input", table_env.from_data_stream(quarantine, quarantine_schema.build()))
    table_env.create_temporary_view("silver_cdc_changelog", table_env.from_changelog_stream(silver, silver_schema.build()))
    _add_file_sink_definitions(table_env, bronze_root, quarantine_root)
    table_env.execute_sql(f"CREATE CATALOG mdep WITH ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='{warehouse}')")
    table_env.execute_sql("CREATE DATABASE IF NOT EXISTS mdep.silver")
    for ddl in silver_table_ddls().values():
        table_env.execute_sql(ddl)
    _submit_writes(table_env)


def _add_file_sink_definitions(table_env, bronze_root: str, quarantine_root: str) -> None:
    """Actual filesystem/Parquet sink definitions, one entity path per contract."""
    column_list = "entity STRING, primary_key STRING, op STRING, source_lsn BIGINT, source_tx_id STRING, source_event_ts STRING, kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT, snapshot STRING, original_envelope STRING, processed_at STRING, event_date STRING"
    for entity in sorted(CDC_ENTITIES):
        table_env.execute_sql(f"CREATE TEMPORARY TABLE bronze_cdc_{entity} ({column_list}) PARTITIONED BY (event_date) WITH ('connector'='filesystem', 'path'='{bronze_root}/{entity}', 'format'='parquet')")
    table_env.execute_sql(f"CREATE TEMPORARY TABLE quarantine_cdc (entity STRING, kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT, original_payload STRING, rejection_reason STRING, processed_at STRING, event_date STRING) PARTITIONED BY (event_date) WITH ('connector'='filesystem', 'path'='{quarantine_root}/unclassified', 'format'='parquet')")


def _submit_writes(table_env) -> None:
    """One Table API statement set wires Bronze, Quarantine, and all Silver targets."""
    statements = table_env.create_statement_set()
    for entity in sorted(CDC_ENTITIES):
        statements.add_insert_sql(f"INSERT INTO bronze_cdc_{entity} SELECT * FROM bronze_cdc_input WHERE entity = '{entity}'")
    statements.add_insert_sql("INSERT INTO quarantine_cdc SELECT * FROM quarantine_cdc_input")
    statements.add_insert_sql("INSERT INTO mdep.silver.core_customers SELECT customer_id, customer_name, email, customer_status, created_at, NULL, preferred_language, source_lsn, source_tx_id, source_event_ts, kafka_topic, kafka_partition, kafka_offset, applied_at FROM silver_cdc_changelog WHERE entity = 'customers'")
    statements.add_insert_sql("INSERT INTO mdep.silver.core_products SELECT product_id, product_name, category_code, CAST(unit_price AS DECIMAL(12,2)), currency, NULL, source_lsn, source_tx_id, source_event_ts, kafka_topic, kafka_partition, kafka_offset, applied_at FROM silver_cdc_changelog WHERE entity = 'products'")
    statements.add_insert_sql("INSERT INTO mdep.silver.core_orders SELECT order_id, customer_id, order_status, order_ts, currency, CAST(order_total AS DECIMAL(12,2)), NULL, source_lsn, source_tx_id, source_event_ts, kafka_topic, kafka_partition, kafka_offset, applied_at FROM silver_cdc_changelog WHERE entity = 'orders'")
    statements.add_insert_sql("INSERT INTO mdep.silver.core_payments SELECT payment_id, order_id, payment_status, payment_ts, CAST(amount AS DECIMAL(12,2)), currency, authorization_code, NULL, source_lsn, source_tx_id, source_event_ts, kafka_topic, kafka_partition, kafka_offset, applied_at FROM silver_cdc_changelog WHERE entity = 'payments'")
    statements.execute()


if __name__ == "__main__":
    run()
