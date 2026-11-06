"""Executable Spark 3.5 / Iceberg 1.10 Bronze-to-Silver job logic."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Callable

from .manifest import VerifiedBatch


COUNTRY_REGIONS = {"DK": "nordics", "NO": "nordics"}
OWNED_ENTITIES = frozenset({"exchange_rates", "locations"})


@dataclass(frozen=True)
class Reconciliation:
    manifest_key: str
    source_entity: str
    input_count: int
    accepted_count: int
    rejected_count: int
    duplicate_count: int
    unique_winner_count: int
    inserted_count: int
    updated_count: int
    unchanged_count: int

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))


def build_local_spark(warehouse: str, *, app_name: str = "mdep-69-aws03-local"):
    """Create the pinned local runtime's HadoopCatalog-backed Spark session."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse)
        .getOrCreate()
    )


def create_tables(spark, catalog: str, namespace: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {catalog}.{namespace}.ref_exchange_rates (
          rate_date DATE NOT NULL,
          base_currency STRING NOT NULL,
          quote_currency STRING NOT NULL,
          rate DECIMAL(20,10) NOT NULL,
          retrieved_at TIMESTAMP NOT NULL,
          provider_name STRING,
          source_payload_json STRING NOT NULL,
          ingestion_id STRING NOT NULL,
          source_name STRING NOT NULL,
          source_entity STRING NOT NULL,
          source_record_key STRING,
          source_extract_ts TIMESTAMP,
          ingested_at TIMESTAMP NOT NULL,
          source_version STRING,
          source_locator STRING NOT NULL,
          record_hash STRING NOT NULL,
          manifest_key STRING NOT NULL,
          logical_date DATE NOT NULL,
          interval_start TIMESTAMP NOT NULL,
          interval_end TIMESTAMP NOT NULL
        ) USING iceberg
        PARTITIONED BY (days(rate_date))
        TBLPROPERTIES ('format-version'='2')"""
    )
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {catalog}.{namespace}.ref_locations (
          location_id STRING NOT NULL,
          location_name STRING NOT NULL,
          country_code STRING NOT NULL,
          timezone STRING NOT NULL,
          region STRING NOT NULL,
          updated_at TIMESTAMP NOT NULL,
          location_type STRING,
          source_payload_json STRING NOT NULL,
          ingestion_id STRING NOT NULL,
          source_name STRING NOT NULL,
          source_entity STRING NOT NULL,
          source_record_key STRING,
          source_extract_ts TIMESTAMP,
          ingested_at TIMESTAMP NOT NULL,
          source_version STRING,
          source_locator STRING NOT NULL,
          record_hash STRING NOT NULL,
          manifest_key STRING NOT NULL,
          logical_date DATE NOT NULL,
          interval_start TIMESTAMP NOT NULL,
          interval_end TIMESTAMP NOT NULL
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2')"""
    )
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {catalog}.{namespace}.silver_quarantine (
          quarantine_id STRING NOT NULL,
          source_name STRING NOT NULL,
          source_entity STRING NOT NULL,
          manifest_key STRING NOT NULL,
          logical_run_identity STRING NOT NULL,
          logical_date DATE NOT NULL,
          original_payload STRING NOT NULL,
          rejection_reason STRING NOT NULL,
          record_hash STRING NOT NULL,
          processed_at TIMESTAMP NOT NULL
        ) USING iceberg
        PARTITIONED BY (source_entity)
        TBLPROPERTIES ('format-version'='2')"""
    )
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {catalog}.{namespace}.processing_reconciliation (
          manifest_key STRING NOT NULL,
          source_entity STRING NOT NULL,
          input_count BIGINT NOT NULL,
          accepted_count BIGINT NOT NULL,
          rejected_count BIGINT NOT NULL,
          duplicate_count BIGINT NOT NULL,
          unique_winner_count BIGINT NOT NULL,
          inserted_count BIGINT NOT NULL,
          updated_count BIGINT NOT NULL,
          unchanged_count BIGINT NOT NULL,
          processed_at TIMESTAMP NOT NULL
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2')"""
    )


def _optional(frame, name: str, data_type: str = "string"):
    from pyspark.sql import functions as F

    return F.col(name).cast(data_type) if name in frame.columns else F.lit(None).cast(data_type)


def _provenance(frame, verified: VerifiedBatch):
    from pyspark.sql import functions as F

    return [
        F.col("ingestion_id"),
        F.col("source_name"),
        F.col("source_entity"),
        _optional(frame, "source_record_key").alias("source_record_key"),
        F.to_timestamp("source_extract_ts").alias("source_extract_ts"),
        F.to_timestamp("ingested_at").alias("ingested_at"),
        _optional(frame, "source_version").alias("source_version"),
        F.col("source_locator"),
        F.col("record_hash"),
        F.lit(verified.manifest_key).alias("manifest_key"),
        F.to_date(F.lit(verified.logical_date)).alias("logical_date"),
        F.to_timestamp(F.lit(verified.interval_start)).alias("interval_start"),
        F.to_timestamp(F.lit(verified.interval_end)).alias("interval_end"),
    ]


def transform(frame, verified: VerifiedBatch):
    """Normalize one verified entity and retain the complete Bronze row as JSON."""
    from pyspark.sql import functions as F

    original = F.to_json(F.struct(*[F.col(name) for name in sorted(frame.columns)])).alias("source_payload_json")
    if verified.source_entity == "exchange_rates":
        checked = frame.select(
            F.to_date("rate_date").alias("rate_date"),
            F.upper(F.trim("base_currency")).alias("base_currency"),
            F.upper(F.trim("quote_currency")).alias("quote_currency"),
            F.col("rate").cast("decimal(20,10)").alias("rate"),
            F.to_timestamp("retrieved_at").alias("retrieved_at"),
            _optional(frame, "provider_name").alias("provider_name"),
            original,
            *_provenance(frame, verified),
        )
        reason = F.concat_ws(
            ";",
            F.when(F.col("rate_date").isNull(), "invalid_or_missing_rate_date"),
            F.when(F.col("base_currency").isNull() | F.col("quote_currency").isNull(), "missing_currency"),
            F.when((F.length("base_currency") != 3) | (F.length("quote_currency") != 3), "invalid_currency_code"),
            F.when(F.col("base_currency") == F.col("quote_currency"), "base_currency_equals_quote_currency"),
            F.when(F.col("rate").isNull(), "invalid_or_missing_rate"),
            F.when(F.col("rate") <= 0, "non_positive_rate"),
            F.when(F.col("retrieved_at").isNull(), "invalid_or_missing_retrieved_at"),
        )
    elif verified.source_entity == "locations":
        countries = frame.sparkSession.createDataFrame(sorted(COUNTRY_REGIONS.items()), ["country_code", "region"])
        checked = frame.select(
            F.trim("location_id").alias("location_id"),
            F.trim("location_name").alias("location_name"),
            F.upper(F.trim("country_code")).alias("country_code"),
            F.trim("timezone").alias("timezone"),
            F.to_timestamp("updated_at").alias("updated_at"),
            _optional(frame, "location_type").alias("location_type"),
            original,
            *_provenance(frame, verified),
        ).join(F.broadcast(countries), "country_code", "left")
        checked = checked.select(
            "location_id", "location_name", "country_code", "timezone", "region", "updated_at", "location_type",
            "source_payload_json", "ingestion_id", "source_name", "source_entity", "source_record_key",
            "source_extract_ts", "ingested_at", "source_version", "source_locator", "record_hash",
            "manifest_key", "logical_date", "interval_start", "interval_end",
        )
        reason = F.concat_ws(
            ";",
            F.when(F.col("location_id").isNull() | (F.col("location_id") == ""), "missing_location_id"),
            F.when(F.col("location_name").isNull() | (F.col("location_name") == ""), "missing_location_name"),
            F.when(F.col("timezone").isNull() | (F.col("timezone") == ""), "missing_timezone"),
            F.when(F.col("updated_at").isNull(), "invalid_or_missing_updated_at"),
            F.when(F.col("region").isNull(), "unknown_country_reference"),
        )
    else:
        raise ValueError(f"{verified.source_entity!r} is outside AWS-03 ownership")
    return checked.withColumn("rejection_reason", F.when(reason == "", F.lit(None).cast("string")).otherwise(reason))


def deduplicate(checked, entity: str):
    """Pick one deterministic winner without letting hashes override freshness."""
    from pyspark.sql import Window, functions as F

    keys = ["rate_date", "base_currency", "quote_currency"] if entity == "exchange_rates" else ["location_id"]
    business_version = "retrieved_at" if entity == "exchange_rates" else "updated_at"
    valid = checked.where(F.col("rejection_reason").isNull())
    ranked = valid.withColumn(
        "_rank",
        F.row_number().over(
            Window.partitionBy(*keys).orderBy(
                F.col(business_version).desc_nulls_last(),
                F.col("source_extract_ts").desc_nulls_last(),
                F.col("ingested_at").desc_nulls_last(),
                F.col("record_hash").desc(),
            )
        ),
    )
    winners = ranked.where("_rank = 1").drop("_rank", "rejection_reason")
    duplicates = ranked.where("_rank > 1").drop("_rank").withColumn("rejection_reason", F.lit("duplicate_business_key_non_winner"))
    invalid = checked.where(F.col("rejection_reason").isNotNull())
    return winners, invalid.unionByName(duplicates, allowMissingColumns=True), duplicates.count()


def _newer_sql(entity: str, source: str = "s", target: str = "t") -> str:
    version = "retrieved_at" if entity == "exchange_rates" else "updated_at"
    min_ts = "TIMESTAMP '1900-01-01 00:00:00'"
    s_extract = f"COALESCE({source}.source_extract_ts, {min_ts})"
    t_extract = f"COALESCE({target}.source_extract_ts, {min_ts})"
    return f"""(
      {source}.{version} > {target}.{version}
      OR ({source}.{version} = {target}.{version} AND {s_extract} > {t_extract})
      OR ({source}.{version} = {target}.{version} AND {s_extract} = {t_extract} AND {source}.ingested_at > {target}.ingested_at)
      OR ({source}.{version} = {target}.{version} AND {s_extract} = {t_extract} AND {source}.ingested_at = {target}.ingested_at AND {source}.record_hash > {target}.record_hash)
    )"""


def _merge_business(spark, winners, entity: str, table: str) -> tuple[int, int, int]:
    keys = ["rate_date", "base_currency", "quote_currency"] if entity == "exchange_rates" else ["location_id"]
    target = spark.table(table).alias("t")
    joined = winners.alias("s").join(target, keys, "left")
    from pyspark.sql import functions as F

    missing = F.col(f"t.{keys[0]}").isNull()
    newer = F.expr(_newer_sql(entity))
    counts = joined.select(
        F.when(missing, "insert").when(newer, "update").otherwise("unchanged").alias("outcome")
    ).groupBy("outcome").count().collect()
    outcome_counts = {row["outcome"]: row["count"] for row in counts}
    inserted = outcome_counts.get("insert", 0)
    updated = outcome_counts.get("update", 0)
    unchanged = outcome_counts.get("unchanged", 0)
    # Iceberg may commit an empty MERGE snapshot even when no clause changes a
    # row. Avoid invoking MERGE for an exact/stale replay after classification.
    if inserted == 0 and updated == 0:
        return inserted, updated, unchanged
    winners.createOrReplaceTempView("aws03_incoming")
    key_sql = " AND ".join(f"t.{key} = s.{key}" for key in keys)
    columns = winners.columns
    update_sql = ", ".join(f"t.{column} = s.{column}" for column in columns if column not in keys)
    insert_columns = ", ".join(columns)
    insert_values = ", ".join(f"s.{column}" for column in columns)
    spark.sql(
        f"""MERGE INTO {table} t USING aws03_incoming s ON {key_sql}
        WHEN MATCHED AND {_newer_sql(entity)} THEN UPDATE SET {update_sql}
        WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"""
    )
    return inserted, updated, unchanged


def _merge_quarantine(spark, rejected, verified: VerifiedBatch, table: str) -> None:
    from pyspark.sql import functions as F

    evidence = rejected.select(
        F.sha2(F.concat_ws("|", F.lit(verified.manifest_key), F.col("record_hash"), F.col("rejection_reason")), 256).alias("quarantine_id"),
        "source_name", "source_entity", F.lit(verified.manifest_key).alias("manifest_key"),
        F.lit(verified.logical_run_identity).alias("logical_run_identity"),
        F.to_date(F.lit(verified.logical_date)).alias("logical_date"),
        F.col("source_payload_json").alias("original_payload"), "rejection_reason", "record_hash",
        F.current_timestamp().alias("processed_at"),
    )
    evidence.createOrReplaceTempView("aws03_rejected")
    spark.sql(
        f"""MERGE INTO {table} t USING aws03_rejected s ON t.quarantine_id = s.quarantine_id
        WHEN NOT MATCHED THEN INSERT *"""
    )


def _record_reconciliation(spark, result: Reconciliation, table: str) -> None:
    from pyspark.sql import functions as F

    frame = spark.createDataFrame([asdict(result)]).withColumn("processed_at", F.current_timestamp())
    frame.createOrReplaceTempView("aws03_reconciliation")
    spark.sql(
        f"""MERGE INTO {table} t USING aws03_reconciliation s ON t.manifest_key = s.manifest_key
        WHEN NOT MATCHED THEN INSERT *"""
    )


def process_verified_batch(
    spark,
    verified: VerifiedBatch,
    data_path: str,
    *,
    catalog: str = "local",
    namespace: str = "mdep_silver",
    acknowledge: Callable[[Reconciliation], None] | None = None,
) -> Reconciliation:
    """Process a verified manifest; every write is retry-safe but not cross-table atomic."""
    create_tables(spark, catalog, namespace)
    raw = spark.read.parquet(data_path)
    checked = transform(raw, verified).cache()
    input_count = checked.count()
    winners, rejected, duplicate_count = deduplicate(checked, verified.source_entity)
    unique_winner_count = winners.count()
    quarantined_count = rejected.count()
    rejected_count = quarantined_count - duplicate_count
    accepted_count = input_count - rejected_count
    business_table = f"{catalog}.{namespace}.ref_{verified.source_entity}"
    inserted, updated, unchanged = _merge_business(spark, winners, verified.source_entity, business_table)
    _merge_quarantine(spark, rejected, verified, f"{catalog}.{namespace}.silver_quarantine")
    result = Reconciliation(
        manifest_key=verified.manifest_key,
        source_entity=verified.source_entity,
        input_count=input_count,
        accepted_count=accepted_count,
        rejected_count=rejected_count,
        duplicate_count=duplicate_count,
        unique_winner_count=unique_winner_count,
        inserted_count=inserted,
        updated_count=updated,
        unchanged_count=unchanged,
    )
    _record_reconciliation(spark, result, f"{catalog}.{namespace}.processing_reconciliation")
    if acknowledge is not None:
        acknowledge(result)
    checked.unpersist()
    return result
