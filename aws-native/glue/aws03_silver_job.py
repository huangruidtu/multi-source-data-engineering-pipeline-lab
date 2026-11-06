"""AWS Glue 5.1 entry point for the MDEP-69 Silver job."""

from __future__ import annotations

import argparse

from mdep_ingestion.object_store import S3ObjectStore
from mdep_silver.manifest import verify_committed_batch
from mdep_silver.spark_job import process_verified_batch


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-bucket", required=True)
    parser.add_argument("--manifest-key", action="append", required=True)
    parser.add_argument("--catalog", default="glue_catalog")
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--max-payload-bytes", type=int, default=134_217_728)
    args, _glue_arguments = parser.parse_known_args()
    return args


def main() -> None:
    from pyspark.sql import SparkSession

    args = arguments()
    store = S3ObjectStore(args.data_bucket)
    spark = SparkSession.builder.appName("mdep-69-aws03-silver").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    for manifest_key in args.manifest_key:
        verified = verify_committed_batch(store, manifest_key, max_payload_bytes=args.max_payload_bytes)
        result = process_verified_batch(
            spark,
            verified,
            f"s3://{args.data_bucket}/{verified.data_key}",
            catalog=args.catalog,
            namespace=args.namespace,
        )
        print(result.to_json())
    spark.stop()


if __name__ == "__main__":
    main()
