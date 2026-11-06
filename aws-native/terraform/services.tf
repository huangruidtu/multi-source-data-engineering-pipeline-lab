data "archive_file" "lambda" {
  type        = "zip"
  output_path = "${path.module}/.terraform/bronze_ingestion.zip"

  source {
    content  = file("${path.module}/../lambda/bronze_ingestion.py")
    filename = "bronze_ingestion.py"
  }

  dynamic "source" {
    for_each = fileset("${path.module}/../mdep_ingestion", "*.py")
    content {
      content  = file("${path.module}/../mdep_ingestion/${source.value}")
      filename = "mdep_ingestion/${source.value}"
    }
  }
}

resource "aws_cloudwatch_log_group" "lambda" {
  count = var.enable_ingestion_lambda ? 1 : 0

  name              = "/aws/lambda/${local.name_prefix}-bronze-ingestion"
  retention_in_days = 14
}

resource "aws_lambda_function" "bronze_ingestion" {
  count = var.enable_ingestion_lambda ? 1 : 0

  function_name = "${local.name_prefix}-bronze-ingestion"
  description   = "Runs one explicitly invoked MDEP source operation through the shared Bronze pipeline"
  role          = aws_iam_role.lambda[0].arn
  handler       = "bronze_ingestion.handler"
  runtime       = "python3.12"

  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256
  layers           = var.lambda_dependency_layer_arns

  memory_size                    = 1024
  timeout                        = 300
  reserved_concurrent_executions = 2

  ephemeral_storage {
    size = 1024
  }

  environment {
    variables = {
      MDEP_DATA_BUCKET             = aws_s3_bucket.data.id
      MDEP_KMS_KEY_ID              = aws_kms_key.data.arn
      MDEP_POSTGRES_SECRET_ARN     = coalesce(var.postgres_secret_arn, "")
      MDEP_REST_EXCHANGE_RATES_URL = coalesce(var.rest_exchange_rates_url, "")
      MDEP_REST_LOCATIONS_URL      = coalesce(var.rest_locations_url, "")
      MDEP_MAX_PAGES               = "100"
      MDEP_MAX_RECORDS             = "50000"
      MDEP_MAX_SOURCE_BYTES        = "10485760"
    }
  }

  depends_on = [aws_iam_role_policy.lambda, aws_cloudwatch_log_group.lambda]
}

resource "aws_glue_catalog_database" "silver" {
  name        = local.glue_database_name
  description = "MDEP AWS-native Silver Iceberg catalog boundary"

  create_table_default_permission {
    permissions = []
  }
}
resource "aws_redshiftserverless_namespace" "platform" {
  count = var.enable_redshift_serverless ? 1 : 0

  namespace_name        = "${local.name_prefix}-analytics"
  db_name               = "mdep"
  manage_admin_password = true
  kms_key_id            = aws_kms_key.data.arn
  iam_roles             = [aws_iam_role.redshift.arn]
  default_iam_role_arn  = aws_iam_role.redshift.arn
  log_exports           = ["connectionlog", "useractivitylog", "userlog"]
}

resource "aws_redshiftserverless_workgroup" "platform" {
  count = var.enable_redshift_serverless ? 1 : 0

  workgroup_name       = "${local.name_prefix}-analytics"
  namespace_name       = aws_redshiftserverless_namespace.platform[0].namespace_name
  base_capacity        = var.redshift_base_capacity_rpus
  max_capacity         = var.redshift_max_capacity_rpus
  publicly_accessible  = false
  enhanced_vpc_routing = true
  subnet_ids           = var.redshift_subnet_ids
  security_group_ids   = var.redshift_security_group_ids
}

resource "aws_redshiftserverless_usage_limit" "monthly_compute" {
  count = var.enable_redshift_serverless ? 1 : 0

  resource_arn  = aws_redshiftserverless_workgroup.platform[0].arn
  usage_type    = "serverless-compute"
  amount        = var.redshift_monthly_rpu_hours
  period        = "monthly"
  breach_action = "deactivate"
}

resource "aws_budgets_budget" "monthly" {
  name         = "${local.name_prefix}-monthly-cost"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  dynamic "notification" {
    for_each = var.budget_alert_email == null ? [] : [
      { threshold = 80, notification_type = "FORECASTED" },
      { threshold = 100, notification_type = "ACTUAL" },
    ]
    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value.threshold
      threshold_type             = "PERCENTAGE"
      notification_type          = notification.value.notification_type
      subscriber_email_addresses = [var.budget_alert_email]
    }
  }
}
