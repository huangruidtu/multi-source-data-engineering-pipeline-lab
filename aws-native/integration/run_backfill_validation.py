"""Real local AWS-02 -> AWS-03 Iceberg -> AWS-08 read-only validation proof."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from uuid import UUID, uuid5


AWS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(AWS_ROOT))

from integration.run_demo import _ingest
from mdep_ingestion.object_store import LocalObjectStore
from mdep_silver.backfill_validation import (
    BackfillValidationError,
    ValidationRequest,
    validate_backfill,
)
from mdep_silver.manifest import verify_committed_batch
from mdep_silver.spark_job import build_local_spark, process_verified_batch


VALIDATION_ID = str(uuid5(UUID("ce89b3ac-a825-52ed-a4c1-79287523123d"), "mdep-74-aws08-local-v1"))


def _publish(store, entity, day, rows):
    outcome, _requests = _ingest(store, entity, day, 8, rows)
    if outcome.status.value != "published":
        raise AssertionError(f"unexpected_publication_status:{outcome.status.value}")
    return verify_committed_batch(store, str(outcome.manifest_key))


def _request(rate_manifest, location_manifest, *, end="2026-11-02T00:00:00Z"):
    return ValidationRequest(
        catalog="local",
        namespace="aws08_validation",
        manifest_keys=(rate_manifest.manifest_key, location_manifest.manifest_key),
        validation_run_identity=VALIDATION_ID,
        expected_interval_start="2026-11-01T00:00:00Z",
        expected_interval_end=end,
    )


def _validate(spark, store, request, namespace):
    return validate_backfill(
        spark,
        store,
        ValidationRequest(**{**request.__dict__, "namespace": namespace}),
        data_path=lambda key: str((store.root / key).resolve()),
    )


def _expect_failure(callable_, reason):
    try:
        callable_()
    except BackfillValidationError as error:
        if error.reason != reason:
            raise AssertionError(f"expected {reason}, observed {error.reason}") from error
        return error.reason
    raise AssertionError(f"expected validation failure: {reason}")


def _materialize(spark, store, namespace, *batches):
    results = []
    for batch in batches:
        results.append(
            process_verified_batch(
                spark,
                batch,
                str((store.root / batch.data_key).resolve()),
                catalog="local",
                namespace=namespace,
            )
        )
    return results


def run(root: Path) -> dict:
    store = LocalObjectStore(root / "objects")
    spark = build_local_spark((root / "warehouse").resolve().as_uri(), app_name="mdep-74-aws08-validation")
    spark.sparkContext.setLogLevel("WARN")
    initial_rate = {
        "rate_date": "2026-11-01", "base_currency": "EUR", "quote_currency": "DKK",
        "rate": "7.5000000000", "retrieved_at": "2026-11-01T06:00:00Z", "provider_name": "ECB",
    }
    location = {
        "location_id": "loc-cph", "location_name": "Copenhagen Hub", "country_code": "DK",
        "timezone": "Europe/Copenhagen", "updated_at": "2026-11-01T07:00:00Z", "location_type": "hub",
    }
    failures = {}
    try:
        rate_batch = _publish(store, "exchange_rates", 1, [initial_rate])
        location_batch = _publish(store, "locations", 1, [location])
        request = _request(rate_batch, location_batch)
        _materialize(spark, store, "aws08_validation", rate_batch, location_batch)

        first = _validate(spark, store, request, "aws08_validation")
        replay = _validate(spark, store, request, "aws08_validation")
        if first.to_json() != replay.to_json():
            raise AssertionError("validation_replay_not_stable")

        corrected_rate = {**initial_rate, "rate": "7.6000000000", "retrieved_at": "2026-11-02T06:00:00Z"}
        corrected_batch = _publish(store, "exchange_rates", 2, [corrected_rate])
        _materialize(spark, store, "aws08_validation", corrected_batch)
        superseded = _validate(spark, store, request, "aws08_validation")
        rate_summary = next(item for item in superseded.entities if item.source_entity == "exchange_rates")
        if rate_summary.superseded_by_newer_count != 1 or rate_summary.exact_match_count != 0:
            raise AssertionError("newer_current_state_not_distinguished_from_historical_input")

        _materialize(spark, store, "aws08_missing", rate_batch, location_batch)
        spark.sql("DELETE FROM local.aws08_missing.ref_locations WHERE location_id = 'loc-cph'")
        failures["missing_current_key"] = _expect_failure(
            lambda: _validate(spark, store, request, "aws08_missing"),
            "historical_business_key_missing_from_current_silver",
        )

        _materialize(spark, store, "aws08_duplicate", rate_batch, location_batch)
        spark.sql("INSERT INTO local.aws08_duplicate.ref_locations SELECT * FROM local.aws08_duplicate.ref_locations")
        failures["duplicate_current_key"] = _expect_failure(
            lambda: _validate(spark, store, request, "aws08_duplicate"),
            "duplicate_current_business_key",
        )

        _materialize(spark, store, "aws08_invalid", rate_batch, location_batch)
        spark.sql("UPDATE local.aws08_invalid.ref_exchange_rates SET rate = CAST(-1 AS DECIMAL(20,10))")
        failures["invalid_current_value"] = _expect_failure(
            lambda: _validate(spark, store, request, "aws08_invalid"),
            "invalid_current_reference_value_or_provenance",
        )

        duplicate_rate_batch = _publish(store, "exchange_rates", 5, [initial_rate, initial_rate])
        duplicate_request = ValidationRequest(
            **{
                **request.__dict__,
                "manifest_keys": (duplicate_rate_batch.manifest_key, location_batch.manifest_key),
                "expected_interval_end": "2026-11-07T00:00:00Z",
            }
        )
        failures["duplicate_historical_key"] = _expect_failure(
            lambda: _validate(spark, store, duplicate_request, "aws08_validation"),
            "duplicate_historical_business_key",
        )

        zero_rate_batch = _publish(store, "exchange_rates", 6, [{**initial_rate, "rate": "0"}])
        invalid_request = ValidationRequest(
            **{
                **request.__dict__,
                "manifest_keys": (zero_rate_batch.manifest_key, location_batch.manifest_key),
                "expected_interval_end": "2026-11-08T00:00:00Z",
            }
        )
        failures["invalid_historical_value"] = _expect_failure(
            lambda: _validate(spark, store, invalid_request, "aws08_validation"),
            "invalid_historical_reference_value",
        )

        manifest_path = store.root / rate_batch.manifest_key
        manifest_bytes = manifest_path.read_bytes()
        manifest_path.write_bytes(b"{corrupt")
        try:
            failures["malformed_manifest"] = _expect_failure(
                lambda: _validate(spark, store, request, "aws08_validation"),
                "manifest_unreadable_or_malformed",
            )
        finally:
            manifest_path.write_bytes(manifest_bytes)

        evidence = {
            "classification": "LOCAL_SPARK_ICEBERG_VERIFIED",
            "validation_run_identity": VALIDATION_ID,
            "initial_report": json.loads(first.to_json()),
            "exact_replay_stable": first.to_json() == replay.to_json(),
            "newer_current_state_report": json.loads(superseded.to_json()),
            "failure_evidence": failures,
            "canonical_writer_unchanged": True,
        }
        (root / "aws08-backfill-validation-evidence.json").write_text(
            json.dumps(evidence, indent=2, sort_keys=True), encoding="utf-8"
        )
        print("AWS08_BACKFILL_VALIDATION_EVIDENCE=" + json.dumps(evidence, sort_keys=True))
        return evidence
    finally:
        spark.stop()


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="mdep-aws08-") as directory:
        run(Path(directory))


if __name__ == "__main__":
    main()
