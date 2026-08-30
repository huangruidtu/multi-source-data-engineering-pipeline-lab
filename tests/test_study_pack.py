"""Static completeness and accuracy checks for the documentation-only Study Pack."""
from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[1]
STUDY = ROOT / "docs" / "study"
MODULES = ["module-00-master", "module-01-data-contracts-sources", "module-02-airflow-batch-ingestion", "module-03-spark-iceberg", "module-04-debezium-kafka", "module-05-flink-streaming", "module-06-snowflake-dbt", "module-07-reliability-validation"]
CORE = ["01-architecture-overview.md", "02-core-concepts.md", "03-code-walkthrough.md", "04-data-flow-and-runtime.md", "05-failures-and-tradeoffs.md", "06-interview-qa.md", "07-interview-talking-points.md"]


class StudyPackTests(unittest.TestCase):
    def test_tree_and_nonempty_core_documents(self):
        for name in MODULES:
            for filename in CORE:
                path = STUDY / name / filename
                self.assertTrue(path.is_file(), path)
                self.assertGreater(len(path.read_text(encoding="utf-8").strip()), 120, path)
        for filename in ("00-study-guide.md", "01-master-architecture.md", "98-top-100-interview-questions.md", "99-final-interview-playbook.md"):
            self.assertTrue((STUDY / filename).is_file())

    def test_references_lessons_and_top_100(self):
        text = "\n".join(path.read_text(encoding="utf-8") for path in STUDY.rglob("*.md"))
        for token in ("MDEP-6", "MDEP-9", "MDEP-10", "MDEP-11", "MDEP-12", "MDEP-13", "MDEP-14", "provide.transaction.metadata", "GOLD_GOLD", "false-PASSED", "RD-08", "RD-13"):
            self.assertIn(token, text)
        questions = re.findall(r"(?m)^([1-9][0-9]{0,1}|100)\. \*\*", (STUDY / "98-top-100-interview-questions.md").read_text(encoding="utf-8"))
        self.assertEqual([str(number) for number in range(1, 101)], questions)

    def test_no_placeholder_secret_or_unsupported_runtime_success_claim(self):
        text = "\n".join(path.read_text(encoding="utf-8") for path in STUDY.rglob("*.md"))
        self.assertNotRegex(text, r"(?i)\\b(TODO|FIXME)\\b")
        self.assertNotRegex(text, r"AKIA[0-9A-Z]{16}|(?i:password\\s*=|secret\\s*=)")
        self.assertNotIn("end-to-end passed", text.lower())
        self.assertNotIn("runtime validated", text.lower())


if __name__ == "__main__":
    unittest.main()
