mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "123456789012"
      arn        = "arn:aws:iam::123456789012:user/mock"
      user_id    = "mock"
    }
  }

  mock_data "aws_partition" {
    defaults = {
      partition  = "aws"
      dns_suffix = "amazonaws.com"
    }
  }

  # aws_iam_policy_document is an AWS-provider data source. The mock provider
  # cannot render it, so provide structurally valid JSON while resource-level
  # assertions and the Python policy tests inspect the real HCL controls.
  mock_data "aws_iam_policy_document" {
    defaults = {
      json          = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
      minified_json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }
}

run "secure_and_cost_bounded_defaults" {
  command = plan

  variables {
    allowed_account_ids = ["123456789012"]
  }

  assert {
    condition     = aws_s3_bucket.data.force_destroy == false
    error_message = "The lake bucket must never force-delete data."
  }

  assert {
    condition     = aws_s3_bucket_public_access_block.data.block_public_acls && aws_s3_bucket_public_access_block.data.block_public_policy && aws_s3_bucket_public_access_block.data.ignore_public_acls && aws_s3_bucket_public_access_block.data.restrict_public_buckets
    error_message = "All four S3 public-access controls must remain enabled."
  }

  assert {
    condition = alltrue([
      for rule in aws_s3_bucket_server_side_encryption_configuration.data.rule : alltrue([
        for encryption in rule.apply_server_side_encryption_by_default : encryption.sse_algorithm == "aws:kms"
      ])
    ])
    error_message = "The lake must default to SSE-KMS."
  }

  assert {
    condition     = aws_kms_key.data.enable_key_rotation && aws_kms_key.data.deletion_window_in_days == 30
    error_message = "The data key must rotate and retain a 30-day deletion recovery window."
  }

  assert {
    condition     = aws_lambda_function.bronze_dispatcher.memory_size == 128 && aws_lambda_function.bronze_dispatcher.timeout == 30 && aws_lambda_function.bronze_dispatcher.reserved_concurrent_executions == 2
    error_message = "Lambda size, timeout and concurrency must remain bounded."
  }

  assert {
    condition     = length(aws_redshiftserverless_workgroup.platform) == 0
    error_message = "Redshift Serverless must be disabled by default."
  }

  assert {
    condition     = aws_budgets_budget.monthly.limit_amount == "25"
    error_message = "The default monthly budget must stay at USD 25."
  }

  assert {
    condition     = local.github_oidc_subject == "repo:huangruidtu@45176295/multi-source-data-engineering-pipeline-lab@1350903098:environment:aws-sandbox-plan"
    error_message = "OIDC trust must match the immutable repository IDs and exact plan environment."
  }

  assert {
    condition     = output.evidence_status.aws_runtime == "RUNTIME_DEFERRED"
    error_message = "Offline Terraform must not claim AWS runtime verification."
  }
}

run "rejects_unapproved_region" {
  command = plan

  variables {
    allowed_account_ids = ["123456789012"]
    aws_region          = "us-east-1"
  }

  expect_failures = [var.aws_region]
}

run "rejects_oversized_budget" {
  command = plan

  variables {
    allowed_account_ids = ["123456789012"]
    monthly_budget_usd  = 101
  }

  expect_failures = [var.monthly_budget_usd]
}

run "rejects_redshift_without_private_network_inputs" {
  command = plan

  variables {
    allowed_account_ids        = ["123456789012"]
    budget_alert_email         = "owner@example.com"
    enable_redshift_serverless = true
  }

  expect_failures = [terraform_data.guardrails]
}

run "rejects_empty_account_allow_list" {
  command = plan

  variables {
    allowed_account_ids = []
  }

  expect_failures = [var.allowed_account_ids]
}

run "rejects_redshift_without_budget_recipient" {
  command = plan

  variables {
    allowed_account_ids         = ["123456789012"]
    enable_redshift_serverless  = true
    redshift_subnet_ids         = ["subnet-11111111", "subnet-22222222", "subnet-33333333"]
    redshift_security_group_ids = ["sg-11111111"]
  }

  expect_failures = [terraform_data.guardrails]
}
