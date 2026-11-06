"""Execute the AWS-04 DuckDB build and stateful incremental scenarios."""

from __future__ import annotations

import json
import os
from decimal import Decimal
from pathlib import Path
import subprocess
import sys

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "target" / "mdep_gold.duckdb"
DBT = ["dbt"]


def run(*args: str, expect_failure: bool = False) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment["DBT_PROFILES_DIR"] = str(ROOT)
    result = subprocess.run(
        [*DBT, *args], cwd=ROOT, text=True, capture_output=True, env=environment
    )
    if expect_failure:
        if result.returncode == 0:
            raise AssertionError(f"Expected dbt command to fail: {args}")
        return result
    if result.returncode:
        sys.stdout.write(result.stdout)
        sys.stderr.write(result.stderr)
        raise RuntimeError(f"dbt command failed ({result.returncode}): {args}")
    return result


def rows(sql: str) -> list[dict[str, object]]:
    with duckdb.connect(str(DB_PATH), read_only=True) as connection:
        cursor = connection.execute(sql)
        names = [item[0] for item in cursor.description]
        return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def execute(sql: str) -> None:
    with duckdb.connect(str(DB_PATH)) as connection:
        connection.execute(sql)


def normalized(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def evidence(payload: dict[str, object]) -> None:
    print("AWS04_GOLD_EVIDENCE=" + json.dumps(payload, default=normalized, sort_keys=True))


def main() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        DB_PATH.unlink()

    run("seed", "--full-refresh")
    run("build")
    initial_orders = rows(
        "select order_id, customer_id, source_currency, source_order_total, "
        "order_total_dkk, missing_dkk_rate, orphan_customer from gold.fct_orders order by order_id"
    )
    if len(initial_orders) != 3:
        raise AssertionError(initial_orders)
    if rows("select count(*) n from gold.fct_orders where orphan_customer")[0]["n"] != 1:
        raise AssertionError("Customer orphan was not preserved")
    if rows("select count(*) n from gold.fct_payments where orphan_order")[0]["n"] != 1:
        raise AssertionError("Payment orphan was not preserved")

    before_replay = rows("select * from gold.fct_orders order by order_id")
    run("run", "--select", "fct_orders+")
    after_replay = rows("select * from gold.fct_orders order by order_id")
    if before_replay != after_replay:
        raise AssertionError("Exact replay changed fct_orders")

    execute("""
      insert into fixture_bridge.fixture_orders values
        ('ord-400','cust-100','created','2025-02-03 09:00:00','DKK',25.00,
         '2025-02-03 09:01:00','403','2025-02-03 09:01:01','2025-02-03 09:01:02','order-400-v1');
      update fixture_bridge.fixture_orders
         set order_total=110.00, updated_at='2025-02-03 10:00:00', source_lsn='404',
             applied_at='2025-02-03 10:00:01', ingested_at='2025-02-03 10:00:02', record_hash='order-100-v2'
       where order_id='ord-100';
      update fixture_bridge.fixture_customers
         set customer_status='inactive', updated_at='2025-02-03 11:00:00', source_lsn='201',
             applied_at='2025-02-03 11:00:01', record_hash='customer-100-v2'
       where customer_id='cust-100';
    """)
    run("run", "--select", "dim_customers", "fct_orders+")
    if rows("select count(*) n from gold.fct_orders")[0]["n"] != 4:
        raise AssertionError("New order was not inserted")
    if str(rows("select source_order_total v from gold.fct_orders where order_id='ord-100'")[0]["v"]) != "110.00":
        raise AssertionError("Newer order was not updated")
    if rows("select customer_status from gold.dim_customers where customer_id='cust-100'")[0]["customer_status"] != "INACTIVE":
        raise AssertionError("Type 1 customer update was not reflected")

    execute("""
      update fixture_bridge.fixture_orders
         set order_total=999.00, updated_at='2025-01-31 10:00:00', source_lsn='399',
             applied_at='2025-01-31 10:00:01', record_hash='order-100-stale'
       where order_id='ord-100'
    """)
    run("run", "--select", "fct_orders+")
    if str(rows("select source_order_total v from gold.fct_orders where order_id='ord-100'")[0]["v"]) != "110.00":
        raise AssertionError("Stale order overwrote a newer Gold row")
    execute("""
      update fixture_bridge.fixture_orders
         set order_total=110.00, updated_at='2025-02-03 10:00:00', source_lsn='404',
             applied_at='2025-02-03 10:00:01', ingested_at='2025-02-03 10:00:02', record_hash='order-100-v2'
       where order_id='ord-100';
      insert into fixture_bridge.silver_bridge_exchange_rates values
        ('2025-02-02','EUR','DKK',7.5000000000,'2025-02-04 06:00:00','ECB',
         '2025-02-04 06:00:00','2025-02-04 06:05:00','rate-eur-dkk-20250202',
         'bronze/rest_api/exchange_rates/2025-02-04/_COMMITTED.json');
    """)
    run("run", "--select", "fct_orders+")
    corrected = rows("select order_total_dkk, missing_dkk_rate from gold.fct_orders where order_id='ord-300'")[0]
    if str(corrected["order_total_dkk"]) != "225.00" or corrected["missing_dkk_rate"]:
        raise AssertionError(f"FX correction did not propagate: {corrected}")

    execute("delete from fixture_bridge.fixture_orders where order_id='ord-400'")
    run("run", "--select", "fct_orders+")
    if rows("select count(*) n from gold.fct_orders where order_id='ord-400'")[0]["n"] != 1:
        raise AssertionError("Ordinary partial-feed run incorrectly deleted Gold")
    guard = run("run-operation", "apply_verified_order_deletes", expect_failure=True)
    if "full_snapshot_verified=true" not in guard.stdout + guard.stderr:
        raise AssertionError("Delete proof guard did not explain the failure")
    run(
        "run-operation", "apply_verified_order_deletes",
        "--vars", '{"full_snapshot_verified": true, "maximum_delete_fraction": 0.25}',
    )
    if rows("select count(*) n from gold.fct_orders where order_id='ord-400'")[0]["n"] != 0:
        raise AssertionError("Verified full-snapshot delete did not apply")
    run("run", "--select", "fct_orders+")

    execute("create table fixture_bridge.fixture_orders_backup as select * from fixture_bridge.fixture_orders; delete from fixture_bridge.fixture_orders")
    mass_guard = run(
        "run-operation", "apply_verified_order_deletes", "--vars",
        '{"full_snapshot_verified": true, "maximum_delete_fraction": 0.25}', expect_failure=True,
    )
    if "exceeds maximum_delete_fraction" not in mass_guard.stdout + mass_guard.stderr:
        raise AssertionError("Mass-delete guard did not explain the failure")
    execute("insert into fixture_bridge.fixture_orders select * from fixture_bridge.fixture_orders_backup; drop table fixture_bridge.fixture_orders_backup")

    run("test")
    if rows("select sum(order_count) n from gold.mart_daily_sales")[0]["n"] != 3:
        raise AssertionError("Downstream marts did not reconcile after the verified delete")
    evidence({
        "classification": "LOCAL_DUCKDB_FIXTURE_BRIDGE_VERIFIED",
        "mutations": {"inserted": 1, "updated": 1, "deleted": 1, "fx_corrected": 1},
        "exact_replay_changed_rows": 0,
        "stale_replay_changed_rows": 0,
        "partial_feed_deleted_rows": 0,
        "delete_guards": {"snapshot_proof_required": True, "mass_delete_blocked": True},
        "initial_orders": initial_orders,
        "final_orders": rows("select order_id, customer_id, source_currency, source_order_total, order_total_dkk, missing_dkk_rate, orphan_customer from gold.fct_orders order by order_id"),
        "daily_sales": rows("select * from gold.mart_daily_sales order by order_date, source_currency"),
        "customer_value": rows("select * from gold.mart_customer_value order by customer_id"),
        "reconciliation": rows("select * from audit.reconciliation_orders"),
    })


if __name__ == "__main__":
    main()
