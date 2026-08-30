"""MDEP-11 topology entrypoint; runtime connector jars are documented in the runbook."""
from processing.flink.cdc_model import parse_debezium

TOPICS = ["mdep.commerce.customers", "mdep.commerce.products", "mdep.commerce.orders", "mdep.commerce.payments"]
CHECKPOINT_INTERVAL_MS = 30_000
BRONZE_LAYOUT = "bronze/cdc/<entity>/event_date=YYYY-MM-DD/"
SILVER_TABLES = ["mdep.silver.core_customers", "mdep.silver.core_products", "mdep.silver.core_orders", "mdep.silver.core_payments"]

def topology_spec() -> dict:
    """Inspectable runtime contract; deployment needs Flink Kafka + Iceberg runtime jars."""
    return {"topics": TOPICS, "checkpoint_interval_ms": CHECKPOINT_INTERVAL_MS, "bronze_layout": BRONZE_LAYOUT, "silver_tables": SILVER_TABLES, "keyed_state": "last applied (LSN, partition, offset) per primary key", "watermark": "bounded 60 seconds; used for event-date/late classification, never CDC version ordering"}

def run():
    # Imports are deferred so pure CDC behavior remains testable without a Flink JVM.
    from pyflink.datastream import StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS)
    env.set_parallelism(1)
    raise RuntimeError("MDEP-11 runtime submission requires Flink Kafka and Iceberg connector JARs; use the documented job package before calling run().")
