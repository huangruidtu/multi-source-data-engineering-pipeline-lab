resource "aws_iam_openid_connect_provider" "github" {
  count = var.create_github_oidc_provider ? 1 : 0

  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = var.github_oidc_thumbprints
}

data "aws_iam_policy_document" "github_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [local.github_oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values   = [local.github_oidc_subject]
    }
  }
}

resource "aws_iam_role" "github_plan" {
  name                 = "${local.name_prefix}-github-plan"
  assume_role_policy   = data.aws_iam_policy_document.github_assume.json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "github_plan" {
  statement {
    sid       = "IdentifyApprovedAccount"
    effect    = "Allow"
    actions   = ["sts:GetCallerIdentity"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "github_plan" {
  name   = "no-refresh-plan-identity-only"
  role   = aws_iam_role.github_plan.id
  policy = data.aws_iam_policy_document.github_plan.json
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  count = var.enable_ingestion_lambda ? 1 : 0

  name               = "${local.name_prefix}-bronze-ingestion"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "lambda" {
  count = var.enable_ingestion_lambda ? 1 : 0

  statement {
    sid    = "WriteOwnLogGroup"
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["${aws_cloudwatch_log_group.lambda[0].arn}:*"]
  }

  statement {
    sid    = "ReadSourceAndVerifyOwnedObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
    ]
    resources = [
      "${aws_s3_bucket.data.arn}/source/reference/*",
      "${aws_s3_bucket.data.arn}/bronze/*",
      "${aws_s3_bucket.data.arn}/quarantine/*",
    ]
  }

  statement {
    sid       = "ConditionallyPublishOwnedObjects"
    effect    = "Allow"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.data.arn}/bronze/*", "${aws_s3_bucket.data.arn}/quarantine/*"]
  }

  statement {
    sid    = "UseDataKeyViaS3"
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

  dynamic "statement" {
    for_each = var.postgres_secret_arn == null ? [] : [var.postgres_secret_arn]
    content {
      sid       = "ReadPostgresConnectionSecret"
      effect    = "Allow"
      actions   = ["secretsmanager:GetSecretValue"]
      resources = [statement.value]
    }
  }
}

resource "aws_iam_role_policy" "lambda" {
  count = var.enable_ingestion_lambda ? 1 : 0

  name   = "ingest-approved-bronze-sources"
  role   = aws_iam_role.lambda[0].id
  policy = data.aws_iam_policy_document.lambda[0].json
}

data "aws_iam_policy_document" "redshift_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["redshift-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "redshift" {
  name               = "${local.name_prefix}-redshift-read-lake"
  assume_role_policy = data.aws_iam_policy_document.redshift_assume.json
}

data "aws_iam_policy_document" "redshift" {
  statement {
    sid       = "ListApprovedLakePrefixes"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.data.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["bronze/*", "iceberg/*"]
    }
  }

  statement {
    sid       = "ReadApprovedLakePrefixes"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${aws_s3_bucket.data.arn}/bronze/*", "${aws_s3_bucket.data.arn}/iceberg/*"]
  }

  statement {
    sid     = "ReadSilverCatalog"
    effect  = "Allow"
    actions = ["glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:GetPartitions"]
    resources = [
      "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.silver.name}",
      "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.silver.name}/*",
    ]
  }

  statement {
    sid       = "DecryptLakeObjectsViaS3"
    effect    = "Allow"
    actions   = ["kms:Decrypt", "kms:DescribeKey"]
    resources = [aws_kms_key.data.arn]

    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["s3.${var.aws_region}.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy" "redshift" {
  name   = "read-approved-lake-and-catalog"
  role   = aws_iam_role.redshift.id
  policy = data.aws_iam_policy_document.redshift.json
}
