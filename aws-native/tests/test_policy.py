import re
import unittest
from pathlib import Path


AWS_ROOT = Path(__file__).resolve().parents[1]
TF_ROOT = AWS_ROOT / "terraform"
WORKFLOW = AWS_ROOT.parent / ".github" / "workflows" / "aws-native-terraform.yml"


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

    def test_oidc_trust_matches_actual_environment_job_and_immutable_repository(self):
        locals_text = (TF_ROOT / "locals.tf").read_text(encoding="utf-8")
        identity = (TF_ROOT / "identity.tf").read_text(encoding="utf-8")
        workflow = WORKFLOW.read_text(encoding="utf-8")
        oidc_job = workflow.split("  oidc-no-refresh-plan:", 1)[1]
        environment = re.search(r"(?m)^    environment: ([a-z0-9-]+)$", oidc_job)
        subject = re.search(r'github_oidc_subject\s*=\s*"([^"]+)"', locals_text)
        self.assertIsNotNone(environment)
        self.assertIsNotNone(subject)
        rendered_subject = subject.group(1).replace("${local.github_environment}", environment.group(1))
        self.assertEqual(
            "repo:huangruidtu@45176295/multi-source-data-engineering-pipeline-lab@1350903098:environment:aws-sandbox-plan",
            rendered_subject,
        )
        self.assertIn('github_environment  = "aws-sandbox-plan"', locals_text)
        self.assertIn("github.event_name == 'workflow_dispatch' && inputs.run_aws_plan", oidc_job)
        self.assertIn('environment_json="$(gh api', oidc_job)
        self.assertIn('type == "required_reviewers" and .prevent_self_review == true', oidc_job)
        self.assertIn('.total_count == 1 and .branch_policies[0].name == "main"', oidc_job)
        self.assertIn('variable = "token.actions.githubusercontent.com:aud"', identity)
        self.assertIn('values   = ["sts.amazonaws.com"]', identity)
        self.assertRegex(
            identity,
            r'test\s*=\s*"StringEquals"\s+variable\s*=\s*"token\.actions\.githubusercontent\.com:sub"\s+values\s*=\s*\[local\.github_oidc_subject\]',
        )
        self.assertNotIn("ForAnyValue:StringLike", identity)
        self.assertNotIn(":pull_request", locals_text)
        self.assertNotIn(":ref:refs/heads/", locals_text)

    def test_account_allow_list_and_redshift_budget_fail_closed(self):
        variables = (TF_ROOT / "variables.tf").read_text(encoding="utf-8")
        versions = (TF_ROOT / "versions.tf").read_text(encoding="utf-8")
        locals_text = (TF_ROOT / "locals.tf").read_text(encoding="utf-8")
        account_block = variables.split('variable "allowed_account_ids"', 1)[1].split('variable "create_github_oidc_provider"', 1)[0]
        self.assertNotIn("default", account_block)
        self.assertIn("length(var.allowed_account_ids) == 1", account_block)
        self.assertIn("allowed_account_ids = var.allowed_account_ids", versions)
        self.assertIn("contains(var.allowed_account_ids, data.aws_caller_identity.current.account_id)", locals_text)
        self.assertNotIn("length(var.allowed_account_ids) == 0 ||", locals_text)
        self.assertIn("!var.enable_redshift_serverless || var.budget_alert_email != null", locals_text)

    def test_expensive_services_are_bounded(self):
        for required in (
            'default     = false',
            'reserved_concurrent_executions = 2',
            'memory_size                    = 1024',
            'timeout                        = 300',
            'default     = 8',
            'condition     = var.redshift_max_capacity_rpus == 8',
            'default     = 25',
        ):
            self.assertIn(required, self.tf)

    def test_ingestion_lambda_is_explicit_and_least_privilege(self):
        identity = (TF_ROOT / "identity.tf").read_text(encoding="utf-8")
        services = (TF_ROOT / "services.tf").read_text(encoding="utf-8")
        lambda_policy = identity.split('data "aws_iam_policy_document" "lambda"', 1)[1].split(
            'resource "aws_iam_role_policy" "lambda"', 1
        )[0]
        for prefix in ("/source/reference/*", "/bronze/*", "/quarantine/*"):
            self.assertIn(prefix, lambda_policy)
        self.assertIn('actions   = ["s3:PutObject"]', lambda_policy)
        self.assertIn('actions   = ["s3:ListBucket"]', lambda_policy)
        self.assertIn('variable = "s3:prefix"', lambda_policy)
        self.assertIn('values   = ["bronze/*", "quarantine/*"]', lambda_policy)
        self.assertIn('actions   = ["secretsmanager:GetSecretValue"]', lambda_policy)
        self.assertIn('variable = "kms:ViaService"', lambda_policy)
        self.assertNotIn('"s3:*"', lambda_policy)
        self.assertNotIn('resources = ["*"]', lambda_policy)
        self.assertNotIn("aws_s3_bucket_notification", services)
        self.assertNotIn("aws_lambda_permission", services)
        self.assertIn("enable_ingestion_lambda ? 1 : 0", services)


if __name__ == "__main__":
    unittest.main()
