output "data_bucket_name" {
  description = "Encrypted MDEP lake bucket."
  value       = aws_s3_bucket.data.id
}

output "glue_silver_database" {
  description = "Glue database reserved for AWS-native Silver Iceberg tables."
  value       = aws_glue_catalog_database.silver.name
}

output "github_plan_role_arn" {
  description = "OIDC role for the optional no-refresh GitHub plan job."
  value       = aws_iam_role.github_plan.arn
}

output "redshift_enabled" {
  description = "Whether the opt-in Redshift Serverless skeleton is planned."
  value       = var.enable_redshift_serverless
}

output "evidence_status" {
  description = "Honest AWS-01 evidence boundary."
  value = {
    architecture = "CODED"
    terraform    = "CODED"
    validation   = "OFFLINE_TESTED"
    aws_runtime  = "RUNTIME_DEFERRED"
  }
}
