data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  bucket_name = "${local.name_prefix}-${data.aws_caller_identity.current.account_id}-${var.aws_region}"

  required_tags = {
    Project            = "MDEP"
    Environment        = var.environment
    ManagedBy          = "Terraform"
    JiraStory          = "MDEP-67"
    DataClassification = "portfolio-synthetic"
    CostCenter         = "interview-lab"
  }

  # This repository was created after GitHub's 2026-07-15 immutable-subject
  # rollout. An environment job replaces branch/PR context with its exact
  # environment context; owner and repository IDs prevent rename/reuse attacks.
  github_environment  = "aws-sandbox-plan"
  github_oidc_subject = "repo:huangruidtu@45176295/multi-source-data-engineering-pipeline-lab@1350903098:environment:${local.github_environment}"

  github_oidc_provider_arn = var.create_github_oidc_provider ? aws_iam_openid_connect_provider.github[0].arn : var.existing_github_oidc_provider_arn
  glue_database_name       = replace("${var.project_name}_${var.environment}_silver", "-", "_")
}

resource "terraform_data" "guardrails" {
  input = {
    account_id = data.aws_caller_identity.current.account_id
    region     = var.aws_region
  }

  lifecycle {
    precondition {
      condition     = contains(var.allowed_account_ids, data.aws_caller_identity.current.account_id)
      error_message = "Current AWS account is not in allowed_account_ids."
    }
    precondition {
      condition     = var.create_github_oidc_provider || var.existing_github_oidc_provider_arn != null
      error_message = "Pass existing_github_oidc_provider_arn when provider creation is disabled."
    }
    precondition {
      condition     = !var.enable_redshift_serverless || (length(var.redshift_subnet_ids) >= 3 && length(var.redshift_security_group_ids) >= 1)
      error_message = "Redshift requires at least three reviewed private subnets and one security group."
    }
    precondition {
      condition     = !var.enable_redshift_serverless || var.budget_alert_email != null
      error_message = "Redshift requires budget_alert_email so paid capacity cannot be enabled without an alert recipient."
    }
  }
}
