"""EMR Serverless entry point for read-only historical backfill validation."""

from __future__ import annotations

import argparse

from mdep_ingestion.models import sha256_hex
from mdep_ingestion.object_store import ConditionalWriteConflict, S3ObjectStore
from mdep_silver.backfill_validation import ValidationRequest, validate_backfill


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-bucket", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--manifest-key", action="append", required=True)
    parser.add_argument("--validation-run-identity", required=True)
    parser.add_argument("--expected-interval-start", required=True)
    parser.add_argument("--expected-interval-end", required=True)
    parser.add_argument("--report-key", required=True)
    parser.add_argument("--kms-key-id", required=True)
    parser.add_argument("--max-payload-bytes", type=int, default=134_217_728)
    return parser.parse_args()


def main() -> None:
    from pyspark.sql import SparkSession

    args = arguments()
    expected_report_key = f"validation/reports/{args.validation_run_identity}.json"
    if args.report_key != expected_report_key:
        raise ValueError("report_key_must_match_validation_run_identity")
    store = S3ObjectStore(args.data_bucket, kms_key_id=args.kms_key_id)
    spark = SparkSession.builder.appName("mdep-74-aws08-backfill-validation").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    report = validate_backfill(
        spark,
        store,
        ValidationRequest(
            catalog=args.catalog,
            namespace=args.namespace,
            manifest_keys=tuple(args.manifest_key),
            validation_run_identity=args.validation_run_identity,
            expected_interval_start=args.expected_interval_start,
            expected_interval_end=args.expected_interval_end,
            max_payload_bytes=args.max_payload_bytes,
        ),
        data_path=lambda key: f"s3://{args.data_bucket}/{key}",
    )
    payload = (report.to_json() + "\n").encode("utf-8")
    checksum = sha256_hex(payload)
    try:
        store.put_bytes_if_absent(
            args.report_key,
            payload,
            content_type="application/json",
            checksum=checksum,
        )
    except ConditionalWriteConflict:
        if store.get_bytes(args.report_key) != payload:
            raise ValueError("validation_report_identity_conflict")
    print("AWS08_BACKFILL_VALIDATION=" + report.to_json())
    spark.stop()


if __name__ == "__main__":
    main()
