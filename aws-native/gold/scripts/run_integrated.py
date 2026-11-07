"""Load verified Iceberg Parquet exports and execute the real AWS-04 dbt project."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from decimal import Decimal
from pathlib import Path
import subprocess
import sys
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
EXPECTED_TABLES = {
    "ref_exchange_rates": ("silver_iceberg", ("rate_date", "base_currency", "quote_currency")),
    "ref_locations": ("silver_iceberg", ("location_id",)),
    "core_customers": ("silver_cdc", ("customer_id",)),
    "core_products": ("silver_cdc", ("product_id",)),
    "core_orders": ("silver_cdc", ("order_id",)),
    "core_payments": ("silver_cdc", ("payment_id",)),
}


def normalized(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


class GoldHarness:
    def __init__(self, database: Path):
        self.database = database
        self.commands: list[str] = []

    def dbt(self, *args: str, expect_failure: bool = False) -> subprocess.CompletedProcess[str]:
        environment = os.environ.copy()
        environment.update({
            "DBT_PROFILES_DIR": str(ROOT),
            "MDEP_GOLD_DUCKDB_PATH": str(self.database),
            "MDEP_GOLD_INPUT_MODE": "integrated",
            "MDEP_CDC_SILVER_SCHEMA": "silver_cdc",
            "MDEP_SILVER_EXTERNAL_SCHEMA": "silver_iceberg",
        })
        command = ["dbt", *args]
        self.commands.append(" ".join(command))
        result = subprocess.run(command, cwd=ROOT, env=environment, text=True, capture_output=True)
        if expect_failure:
            if result.returncode == 0:
                raise AssertionError(f"Expected dbt failure: {command}")
            return result
        if result.returncode:
            sys.stdout.write(result.stdout)
            sys.stderr.write(result.stderr)
            raise RuntimeError(f"dbt command failed ({result.returncode}): {command}")
        return result

    def rows(self, sql: str) -> list[dict[str, Any]]:
        with duckdb.connect(str(self.database), read_only=True) as connection:
            cursor = connection.execute(sql)
            names = [item[0] for item in cursor.description]
            return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def _safe_path(root: Path, value: str) -> Path:
    candidate = (root / value).resolve()
    if candidate != root and root not in candidate.parents:
        raise ValueError(f"export_path_outside_root:{value}")
    return candidate


def _file_checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_bridge(database: Path, manifest_path: Path, expected_phase: str) -> dict[str, Any]:
    document = json.loads(manifest_path.read_text(encoding="utf-8"))
    if document.get("bridge_version") != "mdep-aws07-silver-bridge-v1":
        raise ValueError("unsupported_bridge_version")
    if document.get("phase") != expected_phase:
        raise ValueError(f"unexpected_bridge_phase:{document.get('phase')}")
    run_identity = str(document.get("run_identity", ""))
    if not run_identity:
        raise ValueError("missing_bridge_run_identity")
    if set(document.get("tables", {})) != set(EXPECTED_TABLES):
        raise ValueError("bridge_tables_do_not_match_contract")
    root = manifest_path.parent.resolve()
    if database.exists() and expected_phase == "initial":
        database.unlink()
    database.parent.mkdir(parents=True, exist_ok=True)
    validated: dict[str, Any] = {}
    with duckdb.connect(str(database)) as connection:
        for schema in {value[0] for value in EXPECTED_TABLES.values()}:
            connection.execute(f"create schema if not exists {schema}")
        for table, (schema, keys) in EXPECTED_TABLES.items():
            metadata = document["tables"][table]
            directory = _safe_path(root, str(metadata["path"]))
            files = sorted(directory.glob("*.parquet"))
            if not files:
                raise ValueError(f"missing_parquet_export:{table}")
            observed_files = [
                {"name": item.name, "size": item.stat().st_size, "sha256": _file_checksum(item)}
                for item in files
            ]
            if observed_files != metadata.get("files"):
                raise ValueError(f"export_checksum_mismatch:{table}")
            glob = str(directory / "*.parquet").replace("'", "''")
            connection.execute(
                f"create or replace table {schema}.{table} as select * from read_parquet('{glob}')"
            )
            row_count = connection.execute(f"select count(*) from {schema}.{table}").fetchone()[0]
            key_sql = ", ".join(keys)
            distinct_count = connection.execute(
                f"select count(*) from (select distinct {key_sql} from {schema}.{table})"
            ).fetchone()[0]
            duplicate_count = row_count - distinct_count
            identities = connection.execute(
                f"select distinct bridge_run_identity from {schema}.{table}"
            ).fetchall()
            if (
                row_count != metadata.get("row_count")
                or distinct_count != metadata.get("distinct_key_count")
                or duplicate_count != 0
                or identities != [(run_identity,)]
            ):
                raise ValueError(f"bridge_reconciliation_mismatch:{table}")
            validated[table] = {
                "row_count": row_count,
                "distinct_key_count": distinct_count,
                "keys": [row[0] for row in connection.execute(
                    f"select concat_ws('|', {key_sql}) from {schema}.{table} order by {key_sql}"
                ).fetchall()],
            }
    return {"run_identity": run_identity, "phase": expected_phase, "tables": validated}


def _fact_rows(harness: GoldHarness) -> list[dict[str, Any]]:
    return harness.rows(
        "select order_id, customer_id, source_currency, source_order_total, order_total_dkk, "
        "missing_dkk_rate, orphan_customer from gold.fct_orders order by order_id"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--initial-manifest", type=Path, required=True)
    parser.add_argument("--final-manifest", type=Path, required=True)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    database = args.database.resolve()
    harness = GoldHarness(database)

    initial_bridge = load_bridge(database, args.initial_manifest.resolve(), "initial")
    if initial_bridge["run_identity"] != json.loads(args.final_manifest.read_text())["run_identity"]:
        raise AssertionError("initial_and_final_exports_have_different_run_identity")
    harness.dbt("run")
    initial = _fact_rows(harness)
    initial_by_id = {row["order_id"]: row for row in initial}
    if set(initial_by_id) != {"ord-eur", "ord-dkk", "ord-missing-fx", "ord-orphan", "ord-delete-later"}:
        raise AssertionError(f"unexpected_initial_gold_orders:{sorted(initial_by_id)}")
    if str(initial_by_id["ord-eur"]["order_total_dkk"]) != "750.00":
        raise AssertionError("initial_eur_conversion_failed")
    if str(initial_by_id["ord-dkk"]["order_total_dkk"]) != "50.00":
        raise AssertionError("native_dkk_passthrough_failed")
    if initial_by_id["ord-missing-fx"]["order_total_dkk"] is not None or not initial_by_id["ord-missing-fx"]["missing_dkk_rate"]:
        raise AssertionError("missing_fx_was_not_preserved")
    if not initial_by_id["ord-orphan"]["orphan_customer"]:
        raise AssertionError("missing_customer_order_was_dropped")
    if harness.rows("select count(*) n from gold.fct_payments where orphan_order")[0]["n"] != 1:
        raise AssertionError("missing_order_payment_was_dropped")

    harness.dbt("run")
    replay = _fact_rows(harness)
    if replay != initial:
        raise AssertionError("exact_integrated_gold_replay_changed_rows")

    final_bridge = load_bridge(database, args.final_manifest.resolve(), "final")
    harness.dbt("run")
    before_tombstone_delete = _fact_rows(harness)
    if "ord-delete-later" not in {row["order_id"] for row in before_tombstone_delete}:
        raise AssertionError("incremental_boundary_did_not_demonstrate_retained_deleted_order")
    guard = harness.dbt("run-operation", "apply_verified_order_tombstones", expect_failure=True)
    if "cdc_tombstones_verified=true" not in guard.stdout + guard.stderr:
        raise AssertionError("tombstone_delete_guard_did_not_fail_closed")
    harness.dbt(
        "run-operation", "apply_verified_order_tombstones",
        "--vars", '{"cdc_tombstones_verified": true}',
    )
    harness.dbt("run", "--select", "fct_orders+")
    harness.dbt("test")

    final = _fact_rows(harness)
    final_by_id = {row["order_id"]: row for row in final}
    if set(final_by_id) != {"ord-eur", "ord-dkk", "ord-missing-fx", "ord-orphan"}:
        raise AssertionError(f"unexpected_final_gold_orders:{sorted(final_by_id)}")
    if str(final_by_id["ord-eur"]["order_total_dkk"]) != "760.00":
        raise AssertionError("corrected_silver_fx_did_not_update_gold")
    reconciliation = harness.rows("select * from audit.reconciliation_orders")[0]
    expected_reconciliation = {
        "source_key_count": 4,
        "source_distinct_key_count": 4,
        "gold_key_count": 4,
        "gold_distinct_key_count": 4,
        "source_duplicate_count": 0,
        "gold_duplicate_count": 0,
        "orphan_customer_count": 1,
        "missing_fx_count": 1,
        "source_dkk_total": Decimal("75.00"),
        "source_eur_total": Decimal("100.00"),
        "known_converted_total_dkk": Decimal("835.00"),
    }
    if reconciliation != expected_reconciliation:
        raise AssertionError(f"gold_reconciliation_mismatch:{reconciliation}")
    model_count = len([line for line in harness.dbt("--quiet", "ls", "--resource-type", "model", "--output", "name").stdout.splitlines() if line])
    test_count = len([line for line in harness.dbt("--quiet", "ls", "--resource-type", "test", "--output", "name").stdout.splitlines() if line])
    report = {
        "evidence_version": "mdep-aws07-evidence-v1",
        "classification": "LOCAL_END_TO_END_VERIFIED",
        "run_identity": initial_bridge["run_identity"],
        "bridge": {"initial": initial_bridge, "final": final_bridge},
        "dbt": {"model_count": model_count, "test_count": test_count, "commands": harness.commands},
        "gold": {
            "initial_orders": initial,
            "final_orders": final,
            "exact_replay_changed_rows": 0,
            "explicit_tombstone_deleted_rows": len(before_tombstone_delete) - len(final),
            "reconciliation": reconciliation,
            "daily_sales": harness.rows("select * from gold.mart_daily_sales order by order_date, source_currency"),
            "customer_value": harness.rows("select * from gold.mart_customer_value order by customer_id"),
        },
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, default=normalized, sort_keys=True, indent=2), encoding="utf-8")
    print("AWS07_GOLD_EVIDENCE=" + json.dumps(report, default=normalized, sort_keys=True))


if __name__ == "__main__":
    main()
