resource "aws_kinesis_stream" "cdc" {
  count = var.enable_dms_cdc ? 1 : 0

  name             = "${local.name_prefix}-dms-cdc"
  shard_count      = 1
  retention_period = 24

  encryption_type = "KMS"
  kms_key_id      = aws_kms_key.data.arn

  shard_level_metrics = ["IncomingBytes", "IncomingRecords", "IteratorAgeMilliseconds", "OutgoingBytes", "OutgoingRecords"]

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }
}

data "aws_iam_policy_document" "dms_kinesis_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["dms.${data.aws_partition.current.dns_suffix}"]
    }
  }
}

resource "aws_iam_role" "dms_kinesis" {
  count = var.enable_dms_cdc ? 1 : 0

  name               = "${local.name_prefix}-dms-kinesis"
  assume_role_policy = data.aws_iam_policy_document.dms_kinesis_assume.json
}

data "aws_iam_policy_document" "dms_kinesis" {
  statement {
    sid       = "WriteOnlyToExactCdcStream"
    actions   = ["kinesis:DescribeStream", "kinesis:PutRecord", "kinesis:PutRecords"]
    resources = var.enable_dms_cdc ? [aws_kinesis_stream.cdc[0].arn] : ["arn:${data.aws_partition.current.partition}:kinesis:${var.aws_region}:${data.aws_caller_identity.current.account_id}:stream/disabled"]
  }

  statement {
    sid       = "UseDataKeyThroughKinesisOnly"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.data.arn]
    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["kinesis.${var.aws_region}.${data.aws_partition.current.dns_suffix}"]
    }
  }
}

resource "aws_iam_role_policy" "dms_kinesis" {
  count  = var.enable_dms_cdc ? 1 : 0
  name   = "exact-cdc-stream"
  role   = aws_iam_role.dms_kinesis[0].id
  policy = data.aws_iam_policy_document.dms_kinesis.json
}

resource "aws_iam_role" "dms_secret" {
  count = var.enable_dms_cdc ? 1 : 0

  name               = "${local.name_prefix}-dms-source-secret"
  assume_role_policy = data.aws_iam_policy_document.dms_kinesis_assume.json
}

data "aws_iam_policy_document" "dms_secret" {
  statement {
    sid       = "ReadExactSourceSecret"
    actions   = ["secretsmanager:DescribeSecret", "secretsmanager:GetSecretValue"]
    resources = var.cdc_postgres_secret_arn != null ? [var.cdc_postgres_secret_arn] : ["arn:${data.aws_partition.current.partition}:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:disabled"]
  }

  dynamic "statement" {
    for_each = var.cdc_secret_kms_key_arn == null ? [] : [var.cdc_secret_kms_key_arn]
    content {
      sid       = "DecryptExactSecretKey"
      actions   = ["kms:Decrypt"]
      resources = [statement.value]
      condition {
        test     = "StringEquals"
        variable = "kms:ViaService"
        values   = ["secretsmanager.${var.aws_region}.${data.aws_partition.current.dns_suffix}"]
      }
    }
  }
}

resource "aws_iam_role_policy" "dms_secret" {
  count  = var.enable_dms_cdc ? 1 : 0
  name   = "exact-source-secret"
  role   = aws_iam_role.dms_secret[0].id
  policy = data.aws_iam_policy_document.dms_secret.json
}

resource "aws_dms_replication_subnet_group" "cdc" {
  count = var.enable_dms_cdc ? 1 : 0

  replication_subnet_group_description = "Reviewed private subnets for MDEP AWS-05"
  replication_subnet_group_id          = "${local.name_prefix}-cdc"
  subnet_ids                           = var.cdc_subnet_ids
}

resource "aws_dms_replication_instance" "cdc" {
  count = var.enable_dms_cdc ? 1 : 0

  replication_instance_id     = "${local.name_prefix}-cdc"
  replication_instance_class  = "dms.t3.micro"
  allocated_storage           = 20
  multi_az                    = false
  publicly_accessible         = false
  auto_minor_version_upgrade  = true
  apply_immediately           = false
  kms_key_arn                 = aws_kms_key.data.arn
  replication_subnet_group_id = aws_dms_replication_subnet_group.cdc[0].id
  vpc_security_group_ids      = var.cdc_security_group_ids
}

resource "aws_dms_endpoint" "postgres" {
  count = var.enable_dms_cdc ? 1 : 0

  endpoint_id                     = "${local.name_prefix}-postgres-source"
  endpoint_type                   = "source"
  engine_name                     = "postgres"
  database_name                   = var.cdc_postgres_database_name
  ssl_mode                        = "require"
  secrets_manager_arn             = var.cdc_postgres_secret_arn
  secrets_manager_access_role_arn = aws_iam_role.dms_secret[0].arn
}

resource "aws_dms_endpoint" "kinesis" {
  count = var.enable_dms_cdc ? 1 : 0

  endpoint_id   = "${local.name_prefix}-kinesis-target"
  endpoint_type = "target"
  engine_name   = "kinesis"

  kinesis_settings {
    stream_arn                     = aws_kinesis_stream.cdc[0].arn
    service_access_role_arn        = aws_iam_role.dms_kinesis[0].arn
    message_format                 = "json-unformatted"
    include_control_details        = true
    include_null_and_empty         = true
    include_partition_value        = true
    include_table_alter_operations = true
    include_transaction_details    = true
    partition_include_schema_table = true
  }
}

locals {
  cdc_table_mappings = jsonencode({
    rules = flatten([
      for index, entity in local.cdc_entities : [
        {
          "rule-type"   = "selection", "rule-id" = tostring(index * 2 + 1),
          "rule-name"   = "select-${entity}", "object-locator" = { "schema-name" = "commerce", "table-name" = entity },
          "rule-action" = "include"
        },
        {
          "rule-type"          = "object-mapping", "rule-id" = tostring(index * 2 + 2),
          "rule-name"          = "partition-${entity}-by-primary-key", "rule-action" = "map-record-to-record",
          "object-locator"     = { "schema-name" = "commerce", "table-name" = entity },
          "mapping-parameters" = { "partition-key-type" = "primary-key" }
        }
      ]
    ])
  })

  cdc_task_settings = jsonencode({
    TargetMetadata = {
      BatchApplyEnabled            = false
      ParallelApplyThreads         = 0
      ParallelApplyBufferSize      = 0
      ParallelApplyQueuesPerThread = 0
    }
    FullLoadSettings       = { MaxFullLoadSubTasks = 1, CommitRate = 1000 }
    ChangeProcessingTuning = { BatchApplyPreserveTransaction = true, MemoryLimitTotal = 256 }
    ErrorBehavior = {
      DataErrorPolicy        = "STOP_TASK"
      TableErrorPolicy       = "SUSPEND_TABLE"
      ApplyErrorInsertPolicy = "STOP_TASK"
      ApplyErrorUpdatePolicy = "STOP_TASK"
      ApplyErrorDeletePolicy = "STOP_TASK"
    }
    Logging = {
      EnableLogging    = true
      EnableLogContext = true
      LogComponents = [
        { Id = "SOURCE_CAPTURE", Severity = "LOGGER_SEVERITY_DEFAULT" },
        { Id = "TARGET_APPLY", Severity = "LOGGER_SEVERITY_DEFAULT" },
        { Id = "TASK_MANAGER", Severity = "LOGGER_SEVERITY_DEFAULT" }
      ]
    }
  })
}

resource "aws_dms_replication_task" "cdc" {
  count = var.enable_dms_cdc ? 1 : 0

  replication_task_id       = "${local.name_prefix}-cdc"
  migration_type            = "full-load-and-cdc"
  replication_instance_arn  = aws_dms_replication_instance.cdc[0].replication_instance_arn
  source_endpoint_arn       = aws_dms_endpoint.postgres[0].endpoint_arn
  target_endpoint_arn       = aws_dms_endpoint.kinesis[0].endpoint_arn
  table_mappings            = local.cdc_table_mappings
  replication_task_settings = local.cdc_task_settings
  start_replication_task    = false
}

data "aws_iam_policy_document" "firehose_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["firehose.${data.aws_partition.current.dns_suffix}"]
    }
  }
}

resource "aws_iam_role" "firehose" {
  count = var.enable_dms_cdc ? 1 : 0

  name               = "${local.name_prefix}-cdc-firehose"
  assume_role_policy = data.aws_iam_policy_document.firehose_assume.json
}

data "aws_iam_policy_document" "firehose" {
  statement {
    sid       = "ReadExactCdcStream"
    actions   = ["kinesis:DescribeStream", "kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:ListShards"]
    resources = var.enable_dms_cdc ? [aws_kinesis_stream.cdc[0].arn] : ["arn:${data.aws_partition.current.partition}:kinesis:${var.aws_region}:${data.aws_caller_identity.current.account_id}:stream/disabled"]
  }
  statement {
    sid       = "ListOnlyCdcPrefixes"
    actions   = ["s3:GetBucketLocation", "s3:ListBucket", "s3:ListBucketMultipartUploads"]
    resources = [aws_s3_bucket.data.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["bronze/cdc/*", "bronze/cdc-errors/*"]
    }
  }
  statement {
    sid = "WriteOnlyCdcPrefixes"
    actions = [
      "s3:AbortMultipartUpload", "s3:PutObject"
    ]
    resources = ["${aws_s3_bucket.data.arn}/bronze/cdc/*", "${aws_s3_bucket.data.arn}/bronze/cdc-errors/*"]
  }
  statement {
    sid       = "UseDataKeyThroughStreamingAndS3"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.data.arn]
    condition {
      test     = "StringLike"
      variable = "kms:ViaService"
      values = [
        "kinesis.${var.aws_region}.${data.aws_partition.current.dns_suffix}",
        "s3.${var.aws_region}.${data.aws_partition.current.dns_suffix}"
      ]
    }
  }
  statement {
    sid       = "WriteExactFirehoseLogStream"
    actions   = ["logs:PutLogEvents"]
    resources = var.enable_dms_cdc ? ["${aws_cloudwatch_log_group.cdc_firehose[0].arn}:log-stream:delivery-errors"] : ["arn:${data.aws_partition.current.partition}:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/kinesisfirehose/disabled:log-stream:delivery-errors"]
  }
}

resource "aws_iam_role_policy" "firehose" {
  count  = var.enable_dms_cdc ? 1 : 0
  name   = "exact-stream-and-cdc-prefixes"
  role   = aws_iam_role.firehose[0].id
  policy = data.aws_iam_policy_document.firehose.json
}

resource "aws_cloudwatch_log_group" "cdc_firehose" {
  count = var.enable_dms_cdc ? 1 : 0

  name              = "/aws/kinesisfirehose/${local.name_prefix}-cdc-archive"
  retention_in_days = 14
  kms_key_id        = aws_kms_key.data.arn
}

resource "aws_cloudwatch_log_stream" "cdc_firehose" {
  count = var.enable_dms_cdc ? 1 : 0

  name           = "delivery-errors"
  log_group_name = aws_cloudwatch_log_group.cdc_firehose[0].name
}

resource "aws_kinesis_firehose_delivery_stream" "cdc_archive" {
  count = var.enable_dms_cdc ? 1 : 0

  name        = "${local.name_prefix}-cdc-archive"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.cdc[0].arn
    role_arn           = aws_iam_role.firehose[0].arn
  }

  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose[0].arn
    bucket_arn          = aws_s3_bucket.data.arn
    prefix              = "bronze/cdc/!{partitionKeyFromQuery:entity}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
    error_output_prefix = "bronze/cdc-errors/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
    buffering_size      = 64
    buffering_interval  = 60
    compression_format  = "GZIP"
    kms_key_arn         = aws_kms_key.data.arn

    dynamic_partitioning_configuration {
      enabled        = true
      retry_duration = 300
    }

    processing_configuration {
      enabled = true
      processors {
        type = "MetadataExtraction"
        parameters {
          parameter_name  = "MetadataExtractionQuery"
          parameter_value = "{entity:.metadata[\"table-name\"]}"
        }
        parameters {
          parameter_name  = "JsonParsingEngine"
          parameter_value = "JQ-1.6"
        }
      }
    }

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.cdc_firehose[0].name
      log_stream_name = aws_cloudwatch_log_stream.cdc_firehose[0].name
    }
  }
}
