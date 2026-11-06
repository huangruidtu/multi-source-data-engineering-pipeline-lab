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

output "github_oidc_subject" {
  description = "Exact immutable, environment-scoped GitHub OIDC subject trusted by AWS."
  value       = local.github_oidc_subject
}

output "redshift_enabled" {
  description = "Whether the opt-in Redshift Serverless skeleton is planned."
  value       = var.enable_redshift_serverless
}

output "redshift_connection" {
  description = "Non-secret dbt connection inputs when the private workgroup is explicitly enabled."
  value = var.enable_redshift_serverless ? {
    host     = aws_redshiftserverless_workgroup.platform[0].endpoint[0].address
    port     = aws_redshiftserverless_workgroup.platform[0].endpoint[0].port
    database = aws_redshiftserverless_namespace.platform[0].db_name
    schema   = "gold"
  } : null
}

output "ingestion_lambda_enabled" {
  description = "Whether the dependency- and cost-gated AWS-02 Lambda is planned."
  value       = var.enable_ingestion_lambda
}

output "glue_silver_job_enabled" {
  description = "Whether the cost-gated AWS-03 Glue 5.1 job is planned."
  value       = var.enable_glue_silver_job
}

output "evidence_status" {
  description = "Honest AWS-01 evidence boundary."
  value = {
    architecture     = "CODED"
    ingestion        = "CODED"
    terraform        = "CODED"
    validation       = "OFFLINE_TESTED"
    airflow_linux_ci = "OFFLINE_CI_TESTED"
    spark_iceberg    = "LOCAL_LINUX_CI_TESTED"
    aws_runtime      = "RUNTIME_DEFERRED"
  }
}
