data "archive_file" "glue_silver_package" {
  type        = "zip"
  output_path = "${path.module}/.terraform/aws03_silver_package.zip"

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


  dynamic "source" {
    for_each = fileset("${path.module}/../mdep_platform", "*.py")
    content {
      content  = file("${path.module}/../mdep_platform/${source.value}")
      filename = "mdep_platform/${source.value}"
    }
  }
}

resource "aws_s3_object" "glue_silver_script" {
  count = var.enable_glue_silver_job ? 1 : 0

  bucket                 = aws_s3_bucket.data.id
  key                    = "scripts/aws03/aws03_silver_job.py"
  source                 = "${path.module}/../glue/aws03_silver_job.py"
  source_hash            = filemd5("${path.module}/../glue/aws03_silver_job.py")
  server_side_encryption = "aws:kms"
  kms_key_id             = aws_kms_key.data.arn
  content_type           = "text/x-python"

  tags = { JiraStory = "MDEP-69" }
}

resource "aws_s3_object" "glue_silver_package" {
  count = var.enable_glue_silver_job ? 1 : 0

  bucket                 = aws_s3_bucket.data.id
  key                    = "scripts/aws03/aws03_silver_package.zip"
  source                 = data.archive_file.glue_silver_package.output_path
  source_hash            = data.archive_file.glue_silver_package.output_base64sha256
  server_side_encryption = "aws:kms"
  kms_key_id             = aws_kms_key.data.arn
  content_type           = "application/zip"

  tags = { JiraStory = "MDEP-69" }
}

resource "aws_cloudwatch_log_group" "glue_silver" {
  count = var.enable_glue_silver_job ? 1 : 0

  name              = "/aws-glue/jobs/${local.name_prefix}-silver"
  retention_in_days = 14

  tags = { JiraStory = "MDEP-69" }
}

resource "aws_glue_job" "silver" {
  count = var.enable_glue_silver_job ? 1 : 0

  name              = "${local.name_prefix}-silver"
  description       = "MDEP-69 verified AWS-02 Bronze manifests to Iceberg format-v2 Silver"
  role_arn          = aws_iam_role.glue_silver[0].arn
  glue_version      = "5.1"
  worker_type       = "G.1X"
  number_of_workers = var.glue_silver_workers
  timeout           = var.glue_silver_timeout_minutes
  max_retries       = 0

  execution_property {
    max_concurrent_runs = 1
  }

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_object.glue_silver_script[0].bucket}/${aws_s3_object.glue_silver_script[0].key}"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--datalake-formats"                 = "iceberg"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.glue_silver[0].name
    "--extra-py-files"                   = "s3://${aws_s3_object.glue_silver_package[0].bucket}/${aws_s3_object.glue_silver_package[0].key}"
    "--data-bucket"                      = aws_s3_bucket.data.id
    "--catalog"                          = "glue_catalog"
    "--namespace"                        = aws_glue_catalog_database.silver.name
    "--max-payload-bytes"                = "134217728"
    "--conf" = join(" ", [
      "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      "--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
      "--conf spark.sql.catalog.glue_catalog.warehouse=s3://${aws_s3_bucket.data.id}/iceberg/",
      "--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
      "--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
    ])
  }

  tags = { JiraStory = "MDEP-69" }

  depends_on = [aws_iam_role_policy.glue_silver]
}
