data "archive_file" "lambda" {
  type        = "zip"
  source_file = "${path.module}/../lambda/bronze_dispatcher.py"
  output_path = "${path.module}/.terraform/bronze_dispatcher.zip"
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${local.name_prefix}-bronze-dispatcher"
  retention_in_days = 14
}

resource "aws_lambda_function" "bronze_dispatcher" {
  function_name = "${local.name_prefix}-bronze-dispatcher"
  description   = "Validates MDEP Bronze/quarantine S3 event routing metadata"
  role          = aws_iam_role.lambda.arn
  handler       = "bronze_dispatcher.handler"
  runtime       = "python3.12"

  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256

  memory_size                    = 128
  timeout                        = 30
  reserved_concurrent_executions = 2

  environment {
    variables = {
      DATA_BUCKET   = aws_s3_bucket.data.id
      GLUE_DATABASE = aws_glue_catalog_database.silver.name
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
