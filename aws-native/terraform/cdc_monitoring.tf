resource "aws_cloudwatch_metric_alarm" "cdc_source_latency" {
  count = var.enable_dms_cdc ? 1 : 0

  alarm_name          = "${local.name_prefix}-cdc-source-latency"
  alarm_description   = "DMS source latency exceeds five minutes; inspect WAL retention and task health."
  namespace           = "AWS/DMS"
  metric_name         = "CDCLatencySource"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 2
  threshold           = 300
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "breaching"

  dimensions = {
    ReplicationInstanceIdentifier = aws_dms_replication_instance.cdc[0].replication_instance_id
    ReplicationTaskIdentifier     = aws_dms_replication_task.cdc[0].replication_task_id
  }
}

resource "aws_cloudwatch_metric_alarm" "cdc_kinesis_throttles" {
  count = var.enable_dms_cdc ? 1 : 0

  alarm_name          = "${local.name_prefix}-cdc-kinesis-write-throttles"
  alarm_description   = "Any producer throttle can delay DMS CDC delivery."
  namespace           = "AWS/Kinesis"
  metric_name         = "WriteProvisionedThroughputExceeded"
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 2
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = { StreamName = aws_kinesis_stream.cdc[0].name }
}

resource "aws_cloudwatch_metric_alarm" "cdc_iterator_age" {
  count = var.enable_dms_cdc ? 1 : 0

  alarm_name          = "${local.name_prefix}-cdc-iterator-age"
  alarm_description   = "A consumer is more than five minutes behind the Kinesis head."
  namespace           = "AWS/Kinesis"
  metric_name         = "GetRecords.IteratorAgeMilliseconds"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 5
  threshold           = 300000
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "breaching"

  dimensions = { StreamName = aws_kinesis_stream.cdc[0].name }
}

resource "aws_cloudwatch_metric_alarm" "cdc_firehose_freshness" {
  count = var.enable_dms_cdc ? 1 : 0

  alarm_name          = "${local.name_prefix}-cdc-firehose-freshness"
  alarm_description   = "Raw CDC delivery to S3 is more than five minutes behind."
  namespace           = "AWS/Firehose"
  metric_name         = "DeliveryToS3.DataFreshness"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 5
  threshold           = 300
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "breaching"

  dimensions = { DeliveryStreamName = aws_kinesis_firehose_delivery_stream.cdc_archive[0].name }
}
