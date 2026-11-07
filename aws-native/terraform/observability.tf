locals {
  platform_observability_enabled = var.enable_platform_observability && var.enable_ingestion_lambda
  platform_event_version         = "mdep-execution-event-v1"
}

resource "aws_cloudwatch_log_metric_filter" "platform_failed" {
  count = local.platform_observability_enabled ? 1 : 0

  name           = "${local.name_prefix}-platform-failed"
  pattern        = "{ $.event_version = \"${local.platform_event_version}\" && $.status = \"failed\" }"
  log_group_name = aws_cloudwatch_log_group.lambda[0].name

  metric_transformation {
    name          = "FailedExecutions"
    namespace     = "MDEP/Platform"
    value         = "1"
    default_value = "0"
  }
}

resource "aws_cloudwatch_log_metric_filter" "platform_conflicted" {
  count = local.platform_observability_enabled ? 1 : 0

  name           = "${local.name_prefix}-platform-conflicted"
  pattern        = "{ $.event_version = \"${local.platform_event_version}\" && $.status = \"conflicted\" }"
  log_group_name = aws_cloudwatch_log_group.lambda[0].name

  metric_transformation {
    name          = "ConflictedExecutions"
    namespace     = "MDEP/Platform"
    value         = "1"
    default_value = "0"
  }
}

resource "aws_cloudwatch_log_metric_filter" "platform_quarantined" {
  count = local.platform_observability_enabled ? 1 : 0

  name           = "${local.name_prefix}-platform-quarantined"
  pattern        = "{ $.event_version = \"${local.platform_event_version}\" && $.status = \"quarantined\" }"
  log_group_name = aws_cloudwatch_log_group.lambda[0].name

  metric_transformation {
    name          = "QuarantinedExecutions"
    namespace     = "MDEP/Platform"
    value         = "1"
    default_value = "0"
  }
}

resource "aws_cloudwatch_metric_alarm" "platform_failed" {
  count = local.platform_observability_enabled ? 1 : 0

  alarm_name          = "${local.name_prefix}-platform-failed"
  alarm_description   = "One or more structured platform executions failed in five minutes."
  namespace           = "MDEP/Platform"
  metric_name         = "FailedExecutions"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
}

resource "aws_cloudwatch_metric_alarm" "platform_conflicted" {
  count = local.platform_observability_enabled ? 1 : 0

  alarm_name          = "${local.name_prefix}-platform-conflicted"
  alarm_description   = "An immutable publication conflict requires operator review."
  namespace           = "MDEP/Platform"
  metric_name         = "ConflictedExecutions"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
}
