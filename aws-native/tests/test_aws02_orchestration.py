import importlib.util
import os
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

AWS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(AWS_ROOT))

from mdep_ingestion.orchestration import resolve_run


DAG_FILE = AWS_ROOT / "orchestration" / "dags" / "aws_bronze_ingestion.py"


class IntervalResolutionTests(unittest.TestCase):
    def test_scheduled_interval_keeps_logical_date_explicit(self):
        run = resolve_run(
            logical_date=datetime(2025, 3, 5, tzinfo=timezone.utc),
            data_interval_start="2025-03-04T00:00:00Z",
            data_interval_end="2025-03-05T00:00:00Z",
        )
        context = run.context("rest_api", "locations")
        self.assertEqual("2025-03-05", context.logical_date.isoformat())
        self.assertEqual("2025-03-04T00:00:00+00:00", context.interval_start.isoformat())

    def test_manual_backfill_overrides_interval_not_logical_date(self):
        run = resolve_run(
            logical_date="2025-04-20T09:00:00Z",
            data_interval_start="2025-04-20T09:00:00Z",
            data_interval_end="2025-04-20T09:00:01Z",
            backfill_start="2025-02-01T00:00:00Z",
            backfill_end="2025-02-03T00:00:00Z",
        )
        self.assertEqual("2025-04-20", run.logical_date.isoformat())
        self.assertEqual("2025-02-01T00:00:00+00:00", run.interval_start.isoformat())
        self.assertEqual("2025-02-03T00:00:00+00:00", run.interval_end.isoformat())

    def test_backfill_requires_both_bounds_and_valid_order(self):
        common = {
            "logical_date": "2025-04-20",
            "data_interval_start": "2025-04-19T00:00:00Z",
            "data_interval_end": "2025-04-20T00:00:00Z",
        }
        with self.assertRaises(ValueError):
            resolve_run(**common, backfill_start="2025-02-01T00:00:00Z")
        with self.assertRaises(ValueError):
            resolve_run(**common, backfill_start="2025-02-02T00:00:00Z", backfill_end="2025-02-01T00:00:00Z")


@unittest.skipIf(sys.platform == "win32", "Apache Airflow supports POSIX; CI parses this DAG on Ubuntu")
class AirflowDagTests(unittest.TestCase):
    def test_dag_parses_with_expected_tasks_schedule_and_retries(self):
        os.environ["MDEP_AIRFLOW_SCHEDULE"] = "0 3 * * *"
        spec = importlib.util.spec_from_file_location("mdep_aws02_dag", DAG_FILE)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader
        spec.loader.exec_module(module)
        dag = module.dag
        self.assertEqual("0 3 * * *", dag.schedule)
        self.assertEqual(1, dag.max_active_runs)
        self.assertEqual(
            {"postgres_snapshot", "rest_reference", "file_reference", "completion_summary"},
            set(dag.task_dict),
        )
        for task_id in ("postgres_snapshot", "rest_reference", "file_reference"):
            self.assertEqual(2, dag.task_dict[task_id].retries)
        self.assertIn("backfill_start", dag.params)
        self.assertIn("backfill_end", dag.params)


if __name__ == "__main__":
    unittest.main()
