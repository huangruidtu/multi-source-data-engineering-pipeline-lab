"""One real local AWS-02/AWS-03/AWS-05 Iceberg -> AWS-04 dbt demo."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid5


AWS_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY = AWS_ROOT.parent
sys.path.insert(0, str(AWS_ROOT))

from mdep_cdc.dms_adapter import DmsAdapter
from mdep_cdc.iceberg_writer import apply_iceberg_event, create_cdc_tables
from mdep_cdc.models import ApplyDecision
from mdep_ingestion.extractors import HttpResponse
from mdep_ingestion.models import BatchContext
from mdep_ingestion.object_store import LocalObjectStore
from mdep_ingestion.pipeline import IngestionPipeline
from mdep_ingestion.publication import BronzePublisher, QuarantinePublisher
from mdep_silver.manifest import ManifestValidationError, verify_committed_batch
from mdep_silver.spark_job import build_local_spark, process_verified_batch


UTC = timezone.utc
RUN_IDENTITY = str(uuid5(UUID("9fa1fd09-fca1-5bc8-96c5-c9f1ab65cfaa"), "mdep-73-aws07-integrated-v1"))
REFERENCE_TABLES = {
    "ref_exchange_rates": ("local.aws07_reference.ref_exchange_rates", ("rate_date", "base_currency", "quote_currency")),
    "ref_locations": ("local.aws07_reference.ref_locations", ("location_id",)),
}
CDC_TABLES = {
    "core_customers": ("local.aws07_cdc.core_customers", ("customer_id",)),
    "core_products": ("local.aws07_cdc.core_products", ("product_id",)),
    "core_orders": ("local.aws07_cdc.core_orders", ("order_id",)),
    "core_payments": ("local.aws07_cdc.core_payments", ("payment_id",)),
}


def _context(entity: str, day: int, hour: int) -> BatchContext:
    start = datetime(2026, 11, day, tzinfo=UTC)
    return BatchContext(
        "rest_api", entity, start.date(), start, start + timedelta(days=1),
        datetime(2026, 11, day, hour, tzinfo=UTC),
    )


class _FixtureHttpClient:
    """Offline HTTP boundary that still exercises the production REST extractor."""

    def __init__(self, payloads: list[dict[str, Any]]):
        self.payloads = payloads
        self.requests: list[str] = []

    def get(self, url: str, *, timeout: float) -> HttpResponse:
        del timeout
        self.requests.append(url)
        body = json.dumps({"items": self.payloads, "next_page": None}).encode("utf-8")
        return HttpResponse(200, body, {"ETag": "aws07-offline-fixture-v1"})


def _ingest(
    store: LocalObjectStore,
    entity: str,
    day: int,
    hour: int,
    payloads: list[dict[str, Any]],
):
    context = _context(entity, day, hour)
    client = _FixtureHttpClient(payloads)
    pipeline = IngestionPipeline(BronzePublisher(store), QuarantinePublisher(store))
    outcome = pipeline.rest(
        context,
        url=f"https://aws07-fixture.invalid/{entity}",
        client=client,
        max_pages=1,
        max_records=100,
    )
    return outcome, client.requests


def _publish(store: LocalObjectStore, entity: str, day: int, hour: int, payloads: list[dict[str, Any]]):
    result, requests = _ingest(store, entity, day, hour, payloads)
    if result.status.value != "published":
        raise AssertionError(f"bronze_publication_failed:{result}")
    if len(requests) != 1 or "page=1" not in requests[0]:
        raise AssertionError(f"rest_extractor_request_contract_failed:{requests}")
    return result, verify_committed_batch(store, str(result.manifest_key))


def _dms_record(
    adapter: DmsAdapter,
    operation: str,
    table: str,
    data: dict[str, Any],
    *,
    lsn: str | None = None,
    transaction_id: int | None = None,
    record_id: int | None = None,
):
    metadata: dict[str, Any] = {
        "timestamp": "2026-11-07T09:00:01Z",
        "commit-timestamp": "2026-11-07T09:00:00Z",
        "record-type": "data",
        "operation": operation,
        "partition-key-type": "primary-key",
        "partition-key-value": str(next(iter(data.values()))),
        "schema-name": "commerce",
        "table-name": table,
    }
    if operation != "load":
        metadata.update({
            "stream-position": lsn,
            "transaction-id": transaction_id,
            "transaction-record-id": record_id,
        })
    return adapter.parse({"data": data, "metadata": metadata})


def _file_metadata(directory: Path) -> list[dict[str, Any]]:
    return [
        {
            "name": path.name,
            "size": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
        for path in sorted(directory.glob("*.parquet"))
    ]


def _export_phase(spark, root: Path, phase: str) -> Path:
    from pyspark.sql import functions as F

    phase_root = root / phase
    phase_root.mkdir(parents=True, exist_ok=False)
    tables: dict[str, Any] = {}
    for name, (source_table, keys) in {**REFERENCE_TABLES, **CDC_TABLES}.items():
        output = phase_root / name
        frame = spark.table(source_table).withColumn("bridge_run_identity", F.lit(RUN_IDENTITY))
        frame.coalesce(1).write.mode("errorifexists").parquet(str(output))
        row_count = frame.count()
        distinct_count = frame.select(*keys).distinct().count()
        if row_count != distinct_count:
            raise AssertionError(f"duplicate_export_business_key:{name}")
        tables[name] = {
            "source_table": source_table,
            "path": name,
            "key_columns": list(keys),
            "row_count": row_count,
            "distinct_key_count": distinct_count,
            "files": _file_metadata(output),
        }
    manifest = {
        "bridge_version": "mdep-aws07-silver-bridge-v1",
        "run_identity": RUN_IDENTITY,
        "phase": phase,
        "tables": tables,
    }
    manifest_path = phase_root / "bridge-manifest.json"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, indent=2), encoding="utf-8")
    return manifest_path


def _run(work_root: Path) -> dict[str, Any]:
    store = LocalObjectStore(work_root / "objects")
    spark = build_local_spark((work_root / "warehouse").resolve().as_uri(), app_name="mdep-73-aws07-integrated")
    spark.sparkContext.setLogLevel("WARN")
    adapter = DmsAdapter()
    source_events: list[dict[str, str]] = []
    cdc_results: list[dict[str, str | None]] = []

    def apply(event, observed_at: datetime):
        source_events.append({"entity": event.entity, "primary_key": event.primary_key, "operation": event.operation})
        result = apply_iceberg_event(
            spark, event, namespace="aws07_cdc", observed_at=observed_at
        )
        cdc_results.append({
            "entity": result.entity,
            "primary_key": result.primary_key,
            "decision": result.decision.value,
            "quarantine_id": result.quarantine_id,
        })
        return result

    try:
        create_cdc_tables(spark, namespace="aws07_cdc")
        initial_rate = {
            "rate_date": "2026-11-01", "base_currency": "EUR", "quote_currency": "DKK",
            "rate": "7.5000000000", "retrieved_at": "2026-11-01T06:00:00Z", "provider_name": "ECB",
        }
        rate_publication, rate_batch = _publish(store, "exchange_rates", 1, 8, [initial_rate, initial_rate])
        bronze_replay, _ = _ingest(store, "exchange_rates", 1, 8, [initial_rate, initial_rate])
        conflicting_rate = {**initial_rate, "rate": "7.5500000000"}
        bronze_conflict, _ = _ingest(store, "exchange_rates", 1, 8, [conflicting_rate])
        if bronze_replay.status.value != "already_published" or bronze_conflict.status.value != "conflict":
            raise AssertionError("immutable_bronze_replay_contract_failed")
        rate_result = process_verified_batch(
            spark, rate_batch, str((store.root / rate_batch.data_key).resolve()), namespace="aws07_reference"
        )
        rate_replay = process_verified_batch(
            spark, rate_batch, str((store.root / rate_batch.data_key).resolve()), namespace="aws07_reference"
        )
        location_publication, location_batch = _publish(store, "locations", 2, 8, [{
            "location_id": "loc-cph", "location_name": "Copenhagen Hub", "country_code": "DK",
            "timezone": "Europe/Copenhagen", "updated_at": "2026-11-01T07:00:00Z", "location_type": "hub",
        }])
        location_result = process_verified_batch(
            spark, location_batch, str((store.root / location_batch.data_key).resolve()), namespace="aws07_reference"
        )

        corrupted_publication, _ = _publish(store, "locations", 3, 8, [{
            "location_id": "loc-corrupt", "location_name": "Must Not Land", "country_code": "DK",
            "timezone": "Europe/Copenhagen", "updated_at": "2026-11-03T07:00:00Z",
        }])
        silver_locations_before = spark.table("local.aws07_reference.ref_locations").count()
        (store.root / str(corrupted_publication.manifest_key)).write_text("{corrupt", encoding="utf-8")
        corrupt_failure = None
        try:
            verify_committed_batch(store, str(corrupted_publication.manifest_key))
        except ManifestValidationError as error:
            corrupt_failure = str(error)
        if not corrupt_failure or spark.table("local.aws07_reference.ref_locations").count() != silver_locations_before:
            raise AssertionError("corrupt_manifest_did_not_fail_before_silver")

        customers = [
            {"customer_id": "cust-1", "customer_name": "Ada", "email": "ada@example.test", "customer_status": "active", "created_at": "2026-10-01T00:00:00Z", "updated_at": "2026-11-01T00:00:00Z"},
            {"customer_id": "cust-2", "customer_name": "Bo", "email": "bo@example.test", "customer_status": "active", "created_at": "2026-10-01T00:00:00Z", "updated_at": "2026-11-01T00:00:00Z"},
        ]
        products = [
            {"product_id": "prod-1", "product_name": "Meal", "category_code": "FOOD", "unit_price": "50.00", "currency": "DKK", "updated_at": "2026-11-01T00:00:00Z"},
            {"product_id": "prod-2", "product_name": "Box", "category_code": "PACK", "unit_price": "10.00", "currency": "EUR", "updated_at": "2026-11-01T00:00:00Z"},
        ]
        orders = [
            {"order_id": "ord-eur", "customer_id": "cust-1", "order_status": "completed", "order_ts": "2026-11-01T10:00:00Z", "currency": "EUR", "order_total": "100.00", "updated_at": "2026-11-01T10:00:00Z"},
            {"order_id": "ord-dkk", "customer_id": "cust-2", "order_status": "completed", "order_ts": "2026-11-01T10:01:00Z", "currency": "DKK", "order_total": "50.00", "updated_at": "2026-11-01T10:01:00Z"},
            {"order_id": "ord-missing-fx", "customer_id": "cust-1", "order_status": "created", "order_ts": "2026-11-01T10:02:00Z", "currency": "USD", "order_total": "10.00", "updated_at": "2026-11-01T10:02:00Z"},
            {"order_id": "ord-orphan", "customer_id": "cust-missing", "order_status": "created", "order_ts": "2026-11-01T10:03:00Z", "currency": "DKK", "order_total": "25.00", "updated_at": "2026-11-01T10:03:00Z"},
            {"order_id": "ord-delete-later", "customer_id": "cust-1", "order_status": "created", "order_ts": "2026-11-01T10:04:00Z", "currency": "DKK", "order_total": "30.00", "updated_at": "2026-11-01T10:04:00Z"},
            {"order_id": "ord-predeleted", "customer_id": "cust-1", "order_status": "created", "order_ts": "2026-11-01T10:05:00Z", "currency": "DKK", "order_total": "40.00", "updated_at": "2026-11-01T10:05:00Z"},
        ]
        payments = [
            {"payment_id": "pay-eur", "order_id": "ord-eur", "payment_status": "completed", "payment_ts": "2026-11-01T11:00:00Z", "amount": "100.00", "currency": "EUR", "authorization_code": "auth-eur", "updated_at": "2026-11-01T11:00:00Z"},
            {"payment_id": "pay-orphan", "order_id": "ord-never-seen", "payment_status": "authorized", "payment_ts": "2026-11-01T11:01:00Z", "amount": "5.00", "currency": "DKK", "authorization_code": "auth-orphan", "updated_at": "2026-11-01T11:01:00Z"},
        ]
        observed = datetime(2026, 11, 7, 9, tzinfo=UTC)
        for table, rows in (("customers", customers), ("products", products)):
            for row in rows:
                apply(_dms_record(adapter, "load", table, row), observed)
        sequence = 0x100
        for table, rows in (("orders", orders), ("payments", payments)):
            for row in rows:
                sequence += 0x10
                apply(_dms_record(adapter, "insert", table, row, lsn=f"16/{sequence:X}", transaction_id=800, record_id=sequence), observed)
        duplicate = _dms_record(adapter, "insert", "orders", orders[0], lsn="16/110", transaction_id=800, record_id=0x110)
        if apply(duplicate, observed).decision != ApplyDecision.DUPLICATE:
            raise AssertionError("exact_cdc_replay_was_not_duplicate")
        conflict_row = {**orders[0], "order_total": "999.00"}
        conflict = _dms_record(adapter, "insert", "orders", conflict_row, lsn="16/110", transaction_id=800, record_id=0x110)
        if apply(conflict, observed).decision != ApplyDecision.CONFLICT:
            raise AssertionError("same_position_conflict_not_quarantined")
        predelete = _dms_record(adapter, "delete", "orders", {"order_id": "ord-predeleted"}, lsn="16/300", transaction_id=801, record_id=1)
        if apply(predelete, observed).decision != ApplyDecision.APPLIED_DELETE:
            raise AssertionError("pre_materialization_delete_failed")

        initial_manifest = _export_phase(spark, work_root / "exports", "initial")

        corrected_rate = {
            "rate_date": "2026-11-01", "base_currency": "EUR", "quote_currency": "DKK",
            "rate": "7.6000000000", "retrieved_at": "2026-11-02T06:00:00Z", "provider_name": "ECB",
        }
        corrected_publication, corrected_batch = _publish(store, "exchange_rates", 4, 8, [corrected_rate])
        corrected_result = process_verified_batch(
            spark, corrected_batch, str((store.root / corrected_batch.data_key).resolve()), namespace="aws07_reference"
        )
        delete_later = _dms_record(adapter, "delete", "orders", {"order_id": "ord-delete-later"}, lsn="16/310", transaction_id=802, record_id=1)
        if apply(delete_later, observed + timedelta(minutes=1)).decision != ApplyDecision.APPLIED_DELETE:
            raise AssertionError("post_materialization_delete_failed")
        if apply(delete_later, observed + timedelta(minutes=2)).decision != ApplyDecision.DUPLICATE:
            raise AssertionError("delete_replay_was_not_duplicate")
        stale_after_delete = _dms_record(adapter, "update", "orders", orders[4], lsn="16/250", transaction_id=799, record_id=1)
        if apply(stale_after_delete, observed + timedelta(minutes=3)).decision != ApplyDecision.STALE:
            raise AssertionError("stale_update_resurrected_deleted_order")
        final_manifest = _export_phase(spark, work_root / "exports", "final")

        valid_manifests = 0
        invalid_manifests = 0
        for key in store.list_keys("bronze/"):
            if not key.endswith("/_COMMITTED.json"):
                continue
            try:
                verify_committed_batch(store, key)
                valid_manifests += 1
            except ManifestValidationError:
                invalid_manifests += 1
        source_summary = {
            "reference_source_boundary": "aws02_rest_extractor_and_ingestion_pipeline",
            "reference_input_rows": rate_result.input_count + location_result.input_count + corrected_result.input_count,
            "bronze_committed_manifests": valid_manifests,
            "invalid_manifest_artifacts": invalid_manifests,
            "reference_reconciliation": {
                "initial_rate": json.loads(rate_result.to_json()),
                "rate_replay": json.loads(rate_replay.to_json()),
                "location": json.loads(location_result.to_json()),
                "corrected_rate": json.loads(corrected_result.to_json()),
            },
            "cdc_input_events": len(source_events),
            "cdc_decisions": {
                decision: sum(result["decision"] == decision for result in cdc_results)
                for decision in sorted({str(result["decision"]) for result in cdc_results})
            },
            "cdc_quarantine_rows": spark.table("local.aws07_cdc.cdc_quarantine").count(),
            "corrupt_manifest_failure": corrupt_failure,
            "immutable_bronze": {
                "exact_replay": bronze_replay.status.value,
                "conflicting_replay": bronze_conflict.status.value,
                "initial_rate_manifest": rate_publication.manifest_key,
                "location_manifest": location_publication.manifest_key,
                "corrected_rate_manifest": corrected_publication.manifest_key,
            },
        }
    finally:
        spark.stop()

    gold_report = work_root / "aws07-gold-evidence.json"
    database = work_root / "mdep-aws07.duckdb"
    command = [
        "uv", "run", "--project", str(AWS_ROOT / "gold"), "--locked", "python",
        str(AWS_ROOT / "gold" / "scripts" / "run_integrated.py"),
        "--initial-manifest", str(initial_manifest),
        "--final-manifest", str(final_manifest),
        "--database", str(database),
        "--report", str(gold_report),
    ]
    result = subprocess.run(command, cwd=REPOSITORY, text=True, capture_output=True)
    if result.returncode:
        sys.stdout.write(result.stdout)
        sys.stderr.write(result.stderr)
        raise RuntimeError(f"Gold integrated run failed with exit code {result.returncode}")
    gold = json.loads(gold_report.read_text(encoding="utf-8"))
    evidence = {
        "evidence_version": "mdep-aws07-evidence-v1",
        "classification": "LOCAL_END_TO_END_VERIFIED",
        "run_identity": RUN_IDENTITY,
        "source_to_silver": source_summary,
        "silver_to_gold": gold,
        "artifacts": {
            "initial_bridge_manifest": str(initial_manifest.relative_to(work_root)),
            "final_bridge_manifest": str(final_manifest.relative_to(work_root)),
            "gold_report": str(gold_report.relative_to(work_root)),
        },
    }
    report = work_root / "aws07-evidence.json"
    report.write_text(json.dumps(evidence, sort_keys=True, indent=2), encoding="utf-8")
    print("AWS07_END_TO_END_EVIDENCE=" + json.dumps(evidence, sort_keys=True))
    return evidence


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--work-dir", type=Path)
    args = parser.parse_args()
    if args.work_dir:
        root = args.work_dir.resolve()
        if root.exists():
            raise SystemExit(f"Refusing non-clean work directory: {root}")
        root.mkdir(parents=True)
        _run(root)
    else:
        with tempfile.TemporaryDirectory(prefix="mdep-aws07-") as directory:
            _run(Path(directory))


if __name__ == "__main__":
    main()
