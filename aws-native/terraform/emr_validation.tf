data "archive_file" "emr_validation_package" {
  type        = "zip"
  output_path = "${path.module}/.terraform/aws08_validation_package.zip"

  dynamic "source" {
    for_each = fileset("${path.module}/../mdep_silver", "*.py")
    content {
      content  = file("${path.module}/../mdep_silver/${source.value}")
      filename = "mdep_silver/${source.value}"
    }
  }

  dynamic "source" {
    for_each = fileset("${path.module}/../mdep_ingestion", "*.py")
    content {
      content  = file("${path.module}/../mdep_ingestion/${source.value}")
      filename = "mdep_ingestion/${source.value}"
    }
  }
}

resource "aws_s3_object" "emr_validation_script" {
  count = var.enable_emr_validation ? 1 : 0

  bucket                 = aws_s3_bucket.data.id
  key                    = "scripts/aws08/aws08_backfill_validation_job.py"
  source                 = "${path.module}/../emr/aws08_backfill_validation_job.py"
  source_hash            = filemd5("${path.module}/../emr/aws08_backfill_validation_job.py")
  server_side_encryption = "aws:kms"
  kms_key_id             = aws_kms_key.data.arn
  content_type           = "text/x-python"

  tags = { JiraStory = "MDEP-74" }
}

resource "aws_s3_object" "emr_validation_package" {
  count = var.enable_emr_validation ? 1 : 0

  bucket                 = aws_s3_bucket.data.id
  key                    = "scripts/aws08/aws08_validation_package.zip"
  source                 = data.archive_file.emr_validation_package.output_path
  source_hash            = data.archive_file.emr_validation_package.output_base64sha256
  server_side_encryption = "aws:kms"
  kms_key_id             = aws_kms_key.data.arn
  content_type           = "application/zip"

  tags = { JiraStory = "MDEP-74" }
}

data "aws_iam_policy_document" "emr_validation_assume" {
  count = var.enable_emr_validation ? 1 : 0

  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_iam_role" "emr_validation" {
  count = var.enable_emr_validation ? 1 : 0

  name               = "${local.name_prefix}-emr-backfill-validation"
  assume_role_policy = data.aws_iam_policy_document.emr_validation_assume[0].json

  tags = { JiraStory = "MDEP-74" }
}

data "aws_iam_policy_document" "emr_validation" {
  count = var.enable_emr_validation ? 1 : 0

  statement {
    sid       = "ListOnlyValidationInputsAndOutputs"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.data.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["bronze/*", "iceberg/*", "scripts/aws08/*", "validation/reports/*", "validation/logs/*"]
    }
  }

  statement {
    sid     = "ReadApprovedEvidenceSilverAndCode"
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = [
      "${aws_s3_bucket.data.arn}/bronze/*",
      "${aws_s3_bucket.data.arn}/iceberg/*",
      "${aws_s3_bucket.data.arn}/scripts/aws08/*",
    ]
  }

  statement {
    sid     = "PublishOnlyValidationReportsAndLogs"
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:PutObject"]
    resources = [
      "${aws_s3_bucket.data.arn}/validation/reports/*",
      "${aws_s3_bucket.data.arn}/validation/logs/*",
    ]
  }

  statement {
    sid    = "ReadOnlySilverCatalog"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:GetTable",
      "glue:GetTables",
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.silver.name}",
      "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.silver.name}/ref_exchange_rates",
      "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.silver.name}/ref_locations",
    ]
  }

  statement {
    sid    = "UseDataKeyOnlyThroughS3"
    effect = "Allow"
    actions = [
      "kms:Decrypt",
      "kms:DescribeKey",
      "kms:Encrypt",
      "kms:GenerateDataKey",
    ]
    resources = [aws_kms_key.data.arn]

    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["s3.${var.aws_region}.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy" "emr_validation" {
  count = var.enable_emr_validation ? 1 : 0

  name   = "read-reference-silver-write-validation-evidence"
  role   = aws_iam_role.emr_validation[0].id
  policy = data.aws_iam_policy_document.emr_validation[0].json
}

resource "aws_emrserverless_application" "validation" {
  count = var.enable_emr_validation ? 1 : 0

  name          = "${local.name_prefix}-backfill-validation"
  release_label = "emr-7.12.0"
  type          = "spark"

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = var.emr_validation_idle_timeout_minutes
  }

  maximum_capacity {
    cpu    = "${var.emr_validation_max_cpu} vCPU"
    memory = "${var.emr_validation_max_memory_gb} GB"
  }

  tags = { JiraStory = "MDEP-74" }
}
