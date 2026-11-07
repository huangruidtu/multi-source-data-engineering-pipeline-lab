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
    condition     = length(aws_lambda_function.bronze_ingestion) == 0
    error_message = "The ingestion Lambda must be disabled by default."
  }

  assert {
    condition     = length(aws_glue_job.silver) == 0
    error_message = "The Glue Silver job must be disabled by default."
  }

  assert {
    condition     = length(aws_redshiftserverless_workgroup.platform) == 0
    error_message = "Redshift Serverless must be disabled by default."
  }

  assert {
    condition     = length(aws_redshiftserverless_usage_limit.monthly_compute) == 0
    error_message = "The Redshift usage limit must also be absent while Redshift is disabled."
  }

  assert {
    condition     = length(aws_dms_replication_instance.cdc) == 0 && length(aws_kinesis_stream.cdc) == 0 && length(aws_kinesis_firehose_delivery_stream.cdc_archive) == 0
    error_message = "All paid AWS-05 resources must be disabled by default."
  }

  assert {
    condition     = length(aws_cloudwatch_log_metric_filter.platform_failed) == 0 && length(aws_cloudwatch_metric_alarm.platform_failed) == 0
    error_message = "Platform custom metrics and alarms must be disabled by default."
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

run "bounded_glue_silver_job_when_explicitly_enabled" {
  command = plan

  variables {
    allowed_account_ids    = ["123456789012"]
    budget_alert_email     = "owner@example.com"
    enable_glue_silver_job = true
  }

  assert {
    condition     = length(aws_glue_job.silver) == 1
    error_message = "Explicit enablement must plan exactly one Glue Silver job."
  }

  assert {
    condition     = aws_glue_job.silver[0].glue_version == "5.1" && aws_glue_job.silver[0].worker_type == "G.1X" && aws_glue_job.silver[0].number_of_workers == 2
    error_message = "The Glue version and worker bounds must stay pinned."
  }

  assert {
    condition     = aws_glue_job.silver[0].timeout == 30 && aws_glue_job.silver[0].max_retries == 0 && aws_glue_job.silver[0].execution_property[0].max_concurrent_runs == 1
    error_message = "Glue timeout, retries and concurrency must remain bounded."
  }
}

run "rejects_glue_without_budget_recipient" {
  command = plan

  variables {
    allowed_account_ids    = ["123456789012"]
    enable_glue_silver_job = true
  }

  expect_failures = [terraform_data.guardrails]
}

run "bounded_ingestion_lambda_when_explicitly_enabled" {
  command = plan

  variables {
    allowed_account_ids          = ["123456789012"]
    budget_alert_email           = "owner@example.com"
    enable_ingestion_lambda      = true
    lambda_dependency_layer_arns = ["arn:aws:lambda:eu-west-1:123456789012:layer:mdep-pyarrow-psycopg:1"]
    postgres_secret_arn          = "arn:aws:secretsmanager:eu-west-1:123456789012:secret:mdep/postgres-AbCdEf"
    rest_exchange_rates_url      = "https://sources.example.test/exchange-rates"
    rest_locations_url           = "https://sources.example.test/locations"
  }

  assert {
    condition     = length(aws_lambda_function.bronze_ingestion) == 1
    error_message = "Explicitly enabled ingestion must create exactly one Lambda."
  }

  assert {
    condition     = aws_lambda_function.bronze_ingestion[0].memory_size == 1024 && aws_lambda_function.bronze_ingestion[0].timeout == 300 && aws_lambda_function.bronze_ingestion[0].reserved_concurrent_executions == 2
    error_message = "Lambda memory, timeout and concurrency must remain bounded."
  }

  assert {
    condition     = length(aws_lambda_function.bronze_ingestion[0].layers) == 1
    error_message = "The enabled Lambda must use the reviewed binary dependency layer."
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

run "bounded_redshift_when_explicitly_enabled" {
  command = plan

  variables {
    allowed_account_ids         = ["123456789012"]
    budget_alert_email          = "owner@example.com"
    enable_redshift_serverless  = true
    redshift_subnet_ids         = ["subnet-11111111", "subnet-22222222", "subnet-33333333"]
    redshift_security_group_ids = ["sg-11111111"]
  }

  assert {
    condition     = aws_redshiftserverless_workgroup.platform[0].publicly_accessible == false && aws_redshiftserverless_workgroup.platform[0].enhanced_vpc_routing == true
    error_message = "Enabled Redshift must remain private with enhanced VPC routing."
  }

  assert {
    condition     = aws_redshiftserverless_workgroup.platform[0].base_capacity == 8 && aws_redshiftserverless_workgroup.platform[0].max_capacity == 8
    error_message = "Enabled Redshift must retain the reviewed 8 RPU capacity cap."
  }

  assert {
    condition     = aws_redshiftserverless_usage_limit.monthly_compute[0].usage_type == "serverless-compute" && aws_redshiftserverless_usage_limit.monthly_compute[0].amount == 16 && aws_redshiftserverless_usage_limit.monthly_compute[0].period == "monthly" && aws_redshiftserverless_usage_limit.monthly_compute[0].breach_action == "deactivate"
    error_message = "Redshift compute must deactivate at the bounded monthly RPU-hour limit."
  }
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

run "rejects_ingestion_lambda_without_budget_recipient" {
  command = plan

  variables {
    allowed_account_ids          = ["123456789012"]
    enable_ingestion_lambda      = true
    lambda_dependency_layer_arns = ["arn:aws:lambda:eu-west-1:123456789012:layer:mdep-pyarrow-psycopg:1"]
    postgres_secret_arn          = "arn:aws:secretsmanager:eu-west-1:123456789012:secret:mdep/postgres-AbCdEf"
    rest_exchange_rates_url      = "https://sources.example.test/exchange-rates"
    rest_locations_url           = "https://sources.example.test/locations"
  }

  expect_failures = [terraform_data.guardrails]
}

run "bounded_dms_cdc_when_explicitly_enabled" {
  command = plan

  variables {
    allowed_account_ids     = ["123456789012"]
    budget_alert_email      = "owner@example.com"
    enable_dms_cdc          = true
    cdc_postgres_secret_arn = "arn:aws:secretsmanager:eu-west-1:123456789012:secret:mdep/cdc-postgres-AbCdEf"
    cdc_subnet_ids          = ["subnet-11111111", "subnet-22222222"]
    cdc_security_group_ids  = ["sg-11111111"]
  }

  assert {
    condition     = aws_dms_replication_instance.cdc[0].replication_instance_class == "dms.t3.micro" && aws_dms_replication_instance.cdc[0].allocated_storage == 20 && aws_dms_replication_instance.cdc[0].multi_az == false && aws_dms_replication_instance.cdc[0].publicly_accessible == false
    error_message = "The DMS instance must stay private and bounded to the reviewed interview size."
  }

  assert {
    condition     = aws_kinesis_stream.cdc[0].shard_count == 1 && aws_kinesis_stream.cdc[0].retention_period == 24 && aws_kinesis_stream.cdc[0].encryption_type == "KMS"
    error_message = "Kinesis must retain one encrypted shard for only 24 hours."
  }

  assert {
    condition     = aws_dms_endpoint.kinesis[0].kinesis_settings[0].message_format == "json-unformatted" && aws_dms_endpoint.kinesis[0].kinesis_settings[0].include_transaction_details == true && aws_dms_endpoint.kinesis[0].kinesis_settings[0].partition_include_schema_table == true
    error_message = "The target must emit real JSON_UNFORMATTED DMS events with transaction details."
  }

  assert {
    condition     = jsondecode(aws_dms_replication_task.cdc[0].replication_task_settings).TargetMetadata.BatchApplyEnabled == false && jsondecode(aws_dms_replication_task.cdc[0].replication_task_settings).TargetMetadata.ParallelApplyThreads == 0
    error_message = "DMS batch apply and parallel target apply must remain disabled."
  }

  assert {
    condition     = strcontains(aws_kinesis_firehose_delivery_stream.cdc_archive[0].extended_s3_configuration[0].prefix, "bronze/cdc/!{partitionKeyFromQuery:entity}") && strcontains(aws_kinesis_firehose_delivery_stream.cdc_archive[0].extended_s3_configuration[0].error_output_prefix, "bronze/cdc-errors/")
    error_message = "Firehose must separate per-entity raw CDC and explicit error prefixes."
  }

  assert {
    condition     = aws_cloudwatch_metric_alarm.cdc_source_latency[0].threshold == 300 && aws_cloudwatch_metric_alarm.cdc_iterator_age[0].threshold == 300000 && aws_cloudwatch_metric_alarm.cdc_kinesis_throttles[0].threshold == 0
    error_message = "CDC source lag, iterator age and write throttles must remain observable."
  }
}

run "rejects_dms_cdc_without_budget_recipient" {
  command = plan

  variables {
    allowed_account_ids     = ["123456789012"]
    enable_dms_cdc          = true
    cdc_postgres_secret_arn = "arn:aws:secretsmanager:eu-west-1:123456789012:secret:mdep/cdc-postgres-AbCdEf"
    cdc_subnet_ids          = ["subnet-11111111", "subnet-22222222"]
    cdc_security_group_ids  = ["sg-11111111"]
  }

  expect_failures = [terraform_data.guardrails]
}

run "bounded_platform_observability_on_existing_log_group" {
  command = plan

  variables {
    allowed_account_ids           = ["123456789012"]
    budget_alert_email            = "owner@example.com"
    enable_ingestion_lambda       = true
    enable_platform_observability = true
    lambda_dependency_layer_arns  = ["arn:aws:lambda:eu-west-1:123456789012:layer:mdep-pyarrow-psycopg:1"]
    postgres_secret_arn           = "arn:aws:secretsmanager:eu-west-1:123456789012:secret:mdep/postgres-AbCdEf"
    rest_exchange_rates_url       = "https://sources.example.test/exchange-rates"
    rest_locations_url            = "https://sources.example.test/locations"
  }

  assert {
    condition     = length(aws_cloudwatch_log_metric_filter.platform_failed) == 1 && length(aws_cloudwatch_log_metric_filter.platform_conflicted) == 1 && length(aws_cloudwatch_log_metric_filter.platform_quarantined) == 1
    error_message = "The opt-in must create exactly three bounded filters on actual platform statuses."
  }

  assert {
    condition     = aws_cloudwatch_log_metric_filter.platform_failed[0].log_group_name == aws_cloudwatch_log_group.lambda[0].name && aws_cloudwatch_log_metric_filter.platform_failed[0].pattern == "{ $.event_version = \"mdep-execution-event-v1\" && $.status = \"failed\" }"
    error_message = "Failure metrics must reuse the Lambda log group and match the emitted JSON contract exactly."
  }

  assert {
    condition     = aws_cloudwatch_metric_alarm.platform_failed[0].treat_missing_data == "notBreaching" && aws_cloudwatch_metric_alarm.platform_failed[0].threshold == 0 && aws_cloudwatch_metric_alarm.platform_conflicted[0].treat_missing_data == "notBreaching"
    error_message = "Missing execution events must not page and any observed failure/conflict must alarm."
  }

  assert {
    condition     = output.platform_resource_summary.platform_observability == true && output.platform_resource_summary.ingestion_lambda == true
    error_message = "The resource inventory must report enabled observability and its producer."
  }
}

run "rejects_observability_without_producer_or_budget" {
  command = plan

  variables {
    allowed_account_ids           = ["123456789012"]
    enable_platform_observability = true
  }

  expect_failures = [terraform_data.guardrails]
}
