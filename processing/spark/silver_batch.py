"""MDEP-9: compact PySpark Bronze/Parquet to Silver/Iceberg job family.

Run with Spark plus the Iceberg Spark runtime package; see docs/learning/mdep-9.
The module intentionally has no top-level pyspark import so its pure contract
rules can be inspected and tested without a JVM runtime.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

try:  # supports both `python -m` and the documented spark-submit file path
    from processing.spark.contracts import BATCH_SILVER_ENTITIES, COUNTRY_REGIONS
except ModuleNotFoundError:  # pragma: no cover - direct script invocation
    from contracts import BATCH_SILVER_ENTITIES, COUNTRY_REGIONS


CATALOG_NAME = "mdep"
SILVER_NAMESPACE = "silver"
QUARANTINE_PREFIX = "quarantine/silver"


@dataclass(frozen=True)
class JobConfig:
    bronze_root: str
    warehouse: str
    logical_date: str
    start: str | None = None
    end: str | None = None
    entity: str = "all"
    additive_schema_evolution: bool = False
    inspect: bool = False
    skew_exercise: bool = False


def table_name(entity: str) -> str:
    if entity not in BATCH_SILVER_ENTITIES:
        raise ValueError(f"{entity!r} is outside MDEP-9 batch Silver ownership")
    return f"{CATALOG_NAME}.{SILVER_NAMESPACE}.ref_{entity}"


def bronze_path(root: str, entity: str) -> str:
    # This mirrors MDEP-8's source/entity/ingest_date landing convention.
    return f"{root.rstrip('/')}/bronze/rest_api/{entity}/**/*.parquet"


def quarantine_path(root: str, entity: str) -> str:
    return f"{root.rstrip('/')}/{QUARANTINE_PREFIX}/{entity}"


def build_spark(warehouse: str):
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("mdep-9-silver-batch")
        .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", warehouse.rstrip("/"))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def create_namespace_and_tables(spark) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.{SILVER_NAMESPACE}")
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {table_name('exchange_rates')} (
          rate_date DATE NOT NULL, base_currency STRING NOT NULL, quote_currency STRING NOT NULL,
          rate DECIMAL(18,6) NOT NULL, retrieved_at TIMESTAMP NOT NULL,
          ingestion_id STRING NOT NULL, source_locator STRING NOT NULL, source_extract_ts TIMESTAMP,
          ingested_at TIMESTAMP NOT NULL, record_hash STRING NOT NULL
        ) USING iceberg PARTITIONED BY (days(rate_date))"""
    )
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {table_name('locations')} (
          location_id STRING NOT NULL, location_name STRING NOT NULL, country_code STRING NOT NULL,
          timezone STRING NOT NULL, region STRING NOT NULL, updated_at TIMESTAMP NOT NULL,
          ingestion_id STRING NOT NULL, source_locator STRING NOT NULL, source_extract_ts TIMESTAMP,
          ingested_at TIMESTAMP NOT NULL, record_hash STRING NOT NULL
        ) USING iceberg"""
    )


def _metadata_columns(frame):
    from pyspark.sql import functions as F
    return frame.withColumn("source_extract_ts", F.to_timestamp("source_extract_ts")).withColumn("ingested_at", F.to_timestamp("ingested_at"))


def load_bronze(spark, config: JobConfig, entity: str):
    """Read source-aligned MDEP-8 Parquet; explicit business schema is applied next."""
    return _metadata_columns(spark.read.parquet(bronze_path(config.bronze_root, entity)))


def apply_incremental_boundary(frame, config: JobConfig):
    from pyspark.sql import functions as F
    if not config.start or not config.end:
        return frame
    return frame.where((F.col("ingested_at") >= F.to_timestamp(F.lit(config.start))) & (F.col("ingested_at") < F.to_timestamp(F.lit(config.end))))


def validate_exchange_rates(frame):
    from pyspark.sql import functions as F
    normalized = frame.select(
        F.to_date("rate_date").alias("rate_date"),
        F.upper(F.trim("base_currency")).alias("base_currency"),
        F.upper(F.trim("quote_currency")).alias("quote_currency"),
        F.col("rate").cast("decimal(18,6)").alias("rate"), F.to_timestamp("retrieved_at").alias("retrieved_at"),
        "ingestion_id", "source_locator", "source_extract_ts", "ingested_at", "record_hash",
        F.to_json(F.struct(*frame.columns)).alias("original_payload"),
    )
    reason = F.concat_ws(";",
        F.when(F.col("rate_date").isNull(), F.lit("invalid_or_missing_rate_date")),
        F.when(F.col("base_currency").isNull() | F.col("quote_currency").isNull(), F.lit("missing_currency")),
        F.when((F.length("base_currency") != 3) | (F.length("quote_currency") != 3), F.lit("invalid_currency_code")),
        F.when(F.col("base_currency") == F.col("quote_currency"), F.lit("base_currency_equals_quote_currency")),
        F.when(F.col("rate").isNull(), F.lit("invalid_or_missing_rate")),
        F.when(F.col("rate") <= 0, F.lit("non_positive_rate")),
        F.when(F.col("retrieved_at").isNull(), F.lit("invalid_or_missing_retrieved_at")),
    )
    return normalized.withColumn("rejection_reason", F.when(reason == "", F.lit(None)).otherwise(reason))


def validate_locations(frame):
    from pyspark.sql import functions as F
    countries = frame.sparkSession.createDataFrame([(code, region) for code, region in COUNTRY_REGIONS.items()], ["country_code", "region"])
    normalized = frame.select(
        F.trim("location_id").alias("location_id"), F.trim("location_name").alias("location_name"),
        F.upper(F.trim("country_code")).alias("country_code"), F.trim("timezone").alias("timezone"),
        F.to_timestamp("updated_at").alias("updated_at"), "ingestion_id", "source_locator",
        "source_extract_ts", "ingested_at", "record_hash", F.to_json(F.struct(*frame.columns)).alias("original_payload"),
    ).join(F.broadcast(countries), "country_code", "left")
    reason = F.concat_ws(";",
        F.when(F.col("location_id").isNull() | (F.col("location_id") == ""), F.lit("missing_location_id")),
        F.when(F.col("location_name").isNull() | (F.col("location_name") == ""), F.lit("missing_location_name")),
        F.when(F.col("timezone").isNull() | (F.col("timezone") == ""), F.lit("missing_timezone")),
        F.when(F.col("updated_at").isNull(), F.lit("invalid_or_missing_updated_at")),
        F.when(F.col("region").isNull(), F.lit("unknown_country_reference")),
    )
    return normalized.withColumn("rejection_reason", F.when(reason == "", F.lit(None)).otherwise(reason))


def split_valid_and_quarantine(frame, entity: str, logical_date: str):
    """Use a deterministic timestamp/hash window: newest source record wins ties by hash."""
    from pyspark.sql import Window, functions as F
    keys = ["rate_date", "base_currency", "quote_currency"] if entity == "exchange_rates" else ["location_id"]
    version = "retrieved_at" if entity == "exchange_rates" else "updated_at"
    valid_candidates = frame.where(F.col("rejection_reason").isNull())
    ranked = valid_candidates.withColumn("_rank", F.row_number().over(Window.partitionBy(*keys).orderBy(F.col(version).desc_nulls_last(), F.col("source_extract_ts").desc_nulls_last(), F.col("ingested_at").desc(), F.col("record_hash").desc())))
    winners = ranked.where("_rank = 1").drop("_rank", "rejection_reason", "original_payload")
    duplicates = ranked.where("_rank > 1").drop("_rank").withColumn("rejection_reason", F.lit("duplicate_business_key_non_winner"))
    invalid = frame.where(F.col("rejection_reason").isNotNull())
    rejected = invalid.unionByName(duplicates, allowMissingColumns=True).withColumn("source_entity", F.lit(entity)).withColumn("logical_date", F.lit(logical_date))
    return winners, rejected


def write_quarantine(rejected, root: str, entity: str) -> None:
    # overwrite one deterministic logical-date partition so replay does not add duplicate evidence.
    rejected.write.mode("overwrite").partitionBy("logical_date").parquet(quarantine_path(root, entity))


def merge_iceberg(spark, winners, entity: str) -> None:
    name = table_name(entity)
    keys = "t.rate_date = s.rate_date AND t.base_currency = s.base_currency AND t.quote_currency = s.quote_currency" if entity == "exchange_rates" else "t.location_id = s.location_id"
    winners.createOrReplaceTempView("mdep_9_staged")
    # A retry's identical natural key/hash has no matching update action; a changed newer version replaces it.
    spark.sql(
        f"""MERGE INTO {name} t USING mdep_9_staged s ON {keys}
        WHEN MATCHED AND (s.source_extract_ts >= t.source_extract_ts OR s.record_hash <> t.record_hash) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *"""
    )


def add_nullable_source_note(spark, entity: str) -> None:
    # Controlled, additive-only exercise: never alter a pre-existing field.
    try:
        spark.sql(f"ALTER TABLE {table_name(entity)} ADD COLUMN source_note STRING")
    except Exception as error:
        if "already exists" not in str(error).lower():
            raise


def inspect_spark(spark, winners, entity: str, skew_exercise: bool) -> None:
    from pyspark.sql import functions as F
    print(f"{entity}: input/output partitions={winners.rdd.getNumPartitions()}")
    winners.explain("formatted")
    # groupBy is a deliberate wide transformation/shuffle for inspection.
    winners.groupBy().count().explain("formatted")
    if skew_exercise:
        key = "base_currency" if entity == "exchange_rates" else "country_code"
        skewed = winners.select(key).unionByName(winners.select(key)).repartition(4, key)
        print(f"{entity}: skew exercise partitions={skewed.rdd.getNumPartitions()}")
        skewed.groupBy(key).count().explain("formatted")


def inspect_snapshots(spark, entity: str) -> None:
    spark.sql(f"SELECT committed_at, operation, summary FROM {table_name(entity)}.snapshots ORDER BY committed_at DESC").show(truncate=False)


def run(config: JobConfig) -> None:
    spark = build_spark(config.warehouse)
    create_namespace_and_tables(spark)
    entities: Iterable[str] = BATCH_SILVER_ENTITIES if config.entity == "all" else (config.entity,)
    for entity in sorted(entities):
        raw = apply_incremental_boundary(load_bronze(spark, config, entity), config)
        checked = validate_exchange_rates(raw) if entity == "exchange_rates" else validate_locations(raw)
        winners, rejected = split_valid_and_quarantine(checked, entity, config.logical_date)
        if config.additive_schema_evolution:
            add_nullable_source_note(spark, entity)
            from pyspark.sql import functions as F
            winners = winners.withColumn("source_note", F.lit("mdep-9-additive-exercise"))
        write_quarantine(rejected, config.bronze_root, entity)
        merge_iceberg(spark, winners, entity)
        if config.inspect:
            inspect_spark(spark, winners, entity, config.skew_exercise)
            inspect_snapshots(spark, entity)
        print(f"{entity}: valid={winners.count()} quarantine={rejected.count()}")
    spark.stop()


def parse_args() -> JobConfig:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze-root", required=True, help="S3A/local root that contains bronze/ from MDEP-8")
    parser.add_argument("--warehouse", required=True, help="HadoopCatalog warehouse, e.g. s3a://bucket/iceberg")
    parser.add_argument("--logical-date", required=True)
    parser.add_argument("--start", help="inclusive ingestion timestamp for bounded incremental run")
    parser.add_argument("--end", help="exclusive ingestion timestamp for bounded incremental run")
    parser.add_argument("--entity", choices=("all", *sorted(BATCH_SILVER_ENTITIES)), default="all")
    parser.add_argument("--additive-schema-evolution", action="store_true")
    parser.add_argument("--inspect", action="store_true")
    parser.add_argument("--skew-exercise", action="store_true")
    return JobConfig(**vars(parser.parse_args()))


if __name__ == "__main__":
    run(parse_args())
