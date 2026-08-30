import re
import unittest
from pathlib import Path


ROOT = Path(".")
VALIDATION = ROOT / "validation"


class Mdep13ValidationFrameworkTests(unittest.TestCase):
    def test_matrix_preserves_all_prior_runtime_debt(self):
        matrix = (VALIDATION / "mdep-13-validation-matrix.yml").read_text(encoding="utf-8")
        for item in ("M8-AIRFLOW-BATCH", "M9-SPARK-ICEBERG", "M10-DEBEZIUM-KAFKA", "M11-FLINK-CDC", "M12-SNOWFLAKE-DBT"):
            self.assertIn(f"id: {item}", matrix)
        for field in ("story:", "component:", "description:", "command:", "required_environment:", "expected_result:", "actual_status:", "evidence_path:", "blocker:", "notes:"):
            self.assertIn(field, matrix)

    def test_no_passed_matrix_item_lacks_evidence(self):
        matrix = (VALIDATION / "mdep-13-validation-matrix.yml").read_text(encoding="utf-8")
        for block in re.split(r"\n  - id: ", matrix)[1:]:
            if "actual_status: PASSED" in block:
                evidence = re.search(r"\nevidence_path: (.+)", block)
                self.assertIsNotNone(evidence)
                self.assertNotIn("<run-id>", evidence.group(1))

    def test_failure_catalog_has_all_required_cases_and_fields(self):
        catalog = (VALIDATION / "failure-scenarios.yml").read_text(encoding="utf-8")
        for index in range(1, 21):
            self.assertRegex(catalog, rf"id: F{index:02d},")
        for field in ("trigger:", "expected_behavior:", "recovery:", "data_loss_risk:", "duplicate_risk:", "evidence:", "executed_status:"):
            self.assertIn(field, catalog)

    def test_reconciliation_checks_and_quality_gates_exist(self):
        guide = (VALIDATION / "reconciliation" / "README.md").read_text(encoding="utf-8")
        for check in ("R01", "R02", "R03", "R04", "R05", "R06", "R07", "R08", "R09", "R10"):
            self.assertIn(check, guide)
        gates = (VALIDATION / "quality-gates.yml").read_text(encoding="utf-8")
        for classification in ("BLOCKING", "WARNING", "INFORMATIONAL"):
            self.assertIn(classification, gates)

    def test_evidence_docs_scripts_and_interview_pack_are_complete(self):
        self.assertIn("IMPLEMENTED", (ROOT / "docs/project-evidence.md").read_text(encoding="utf-8"))
        self.assertIn("BLOCKED / UNVALIDATED", (ROOT / "docs/project-evidence.md").read_text(encoding="utf-8"))
        for name in ("preflight-mdep-13.ps1", "validate-mdep-13-e2e.ps1"):
            self.assertTrue((ROOT / "scripts" / name).is_file())
        for name in ("project-summary.md", "architecture-walkthrough.md", "failure-scenarios.md", "tradeoffs.md", "production-improvements.md", "top-questions.md"):
            self.assertTrue((ROOT / "docs/interview" / name).is_file())
        for name in ("implementation-guide.md", "architecture-notes.md", "runbook.md", "learning-notes.md", "interview-qa.md", "interview-talking-points.md"):
            self.assertTrue((ROOT / "docs/learning/mdep-13" / name).is_file())

    def test_runner_has_explicit_exit_code_and_self_test_contracts(self):
        runner = (ROOT / "scripts/validate-mdep-13-e2e.ps1").read_text(encoding="utf-8")
        self.assertIn("exit_code = $ExitCode", runner)
        self.assertIn("if ($exitCode -ne 0)", runner)
        self.assertIn("Set-Location -LiteralPath $WorkingDirectory", runner)
        for stage in ("SELFTEST-SUCCESS", "SELFTEST-NATIVE-FAILURE", "SELFTEST-POWERSHELL-THROW", "SELFTEST-BLOCKED", "SELFTEST-NOT-RUN"):
            self.assertIn(stage, runner)

    def test_new_mdep13_text_has_no_unexpected_control_characters(self):
        paths = [ROOT / "docs/project-evidence.md"]
        paths += list((ROOT / "docs/learning/mdep-13").glob("*.md"))
        paths += list((ROOT / "docs/interview").glob("*.md"))
        paths += list(VALIDATION.rglob("*.yml"))
        paths += list((ROOT / "scripts").glob("*mdep-13*.ps1"))
        for path in paths:
            text = path.read_text(encoding="utf-8")
            self.assertIsNone(re.search(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", text), path)

    def test_no_literal_secrets_in_new_validation_assets(self):
        paths = list(VALIDATION.rglob("*")) + list((ROOT / "scripts").glob("*mdep-13*.ps1"))
        text = "\n".join(path.read_text(encoding="utf-8") for path in paths if path.is_file())
        self.assertNotRegex(text, r"(?i)(aws_secret_access_key\s*=|password\s*=|BEGIN [A-Z ]*PRIVATE KEY)")


if __name__ == "__main__":
    unittest.main()
