import sys
import unittest
from pathlib import Path


AWS_ROOT = Path(__file__).resolve().parents[1]
TF_ROOT = AWS_ROOT / "terraform"


def terraform_text() -> str:
    return "\n".join(path.read_text(encoding="utf-8") for path in sorted(TF_ROOT.glob("*.tf")))


class AwsNativePolicyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tf = terraform_text()

    def test_original_v1_is_referenced_not_copied_or_modified(self):
        architecture = (AWS_ROOT / "architecture.md").read_text(encoding="utf-8")
        self.assertIn("one canonical writer", architecture.lower())
        self.assertIn("source-data/contracts/commerce-operations.md", (AWS_ROOT / "README.md").read_text(encoding="utf-8"))

    def test_s3_has_defense_in_depth(self):
        for required in (
            "force_destroy = false",
            "prevent_destroy = true",
            'sse_algorithm     = "aws:kms"',
            "block_public_acls       = true",
            "block_public_policy     = true",
            "ignore_public_acls      = true",
            "restrict_public_buckets = true",
            'variable = "aws:SecureTransport"',
            'values   = ["false"]',
        ):
            self.assertIn(required, self.tf)

    def test_ci_role_has_no_mutating_or_wildcard_action(self):
        identity = (TF_ROOT / "identity.tf").read_text(encoding="utf-8")
        plan_policy = identity.split('data "aws_iam_policy_document" "github_plan"', 1)[1].split('resource "aws_iam_role_policy" "github_plan"', 1)[0]
        self.assertIn('actions   = ["sts:GetCallerIdentity"]', plan_policy)
        self.assertNotIn('actions   = ["*"]', plan_policy)
        for verb in ("Create", "Delete", "Put", "Update", "Attach", "PassRole"):
            self.assertNotIn(verb, plan_policy)

    def test_oidc_trust_is_repository_and_audience_scoped(self):
        locals_text = (TF_ROOT / "locals.tf").read_text(encoding="utf-8")
        identity = (TF_ROOT / "identity.tf").read_text(encoding="utf-8")
        self.assertIn('repo:${var.github_repository}:ref:refs/heads/main', locals_text)
        self.assertIn('repo:${var.github_repository}:pull_request', locals_text)
        self.assertIn('variable = "token.actions.githubusercontent.com:aud"', identity)
        self.assertIn('values   = ["sts.amazonaws.com"]', identity)

    def test_expensive_services_are_bounded(self):
        for required in (
            'default     = false',
            'reserved_concurrent_executions = 2',
            'default     = 8',
            'condition     = var.redshift_max_capacity_rpus == 8',
            'default     = 25',
        ):
            self.assertIn(required, self.tf)

    def test_lambda_handler_accepts_only_approved_prefixes(self):
        sys.path.insert(0, str(AWS_ROOT / "lambda"))
        try:
            from bronze_dispatcher import handler

            event = {"Records": [{"s3": {"bucket": {"name": "example"}, "object": {"key": "bronze%2Frest_api%2Flocations%2Fpart.parquet"}}}]}
            result = handler(event, None)
            self.assertEqual(1, result["accepted"])
            self.assertEqual("bronze", result["routes"][0]["prefix"])
            with self.assertRaises(ValueError):
                handler({"Records": [{"s3": {"bucket": {"name": "example"}, "object": {"key": "iceberg/table/data.parquet"}}}]}, None)
        finally:
            sys.path.pop(0)


if __name__ == "__main__":
    unittest.main()
