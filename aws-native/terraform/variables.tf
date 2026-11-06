variable "project_name" {
  description = "Short project identifier used in resource names."
  type        = string
  default     = "mdep"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,15}$", var.project_name))
    error_message = "project_name must be 3-16 lowercase letters, numbers or hyphens."
  }
}
variable "environment" {
  description = "Deployment environment. AWS-01 is intended for a sandbox."
  type        = string
  default     = "sandbox"

  validation {
    condition     = contains(["sandbox", "dev", "test"], var.environment)
    error_message = "Only sandbox, dev and test are allowed by this sprint configuration."
  }
}

variable "aws_region" {
  description = "Approved deployment region."
  type        = string
  default     = "eu-west-1"

  validation {
    condition     = contains(["eu-west-1", "eu-central-1"], var.aws_region)
    error_message = "AWS-01 permits only eu-west-1 or eu-central-1."
  }
}

variable "allowed_account_ids" {
  description = "The one exact AWS account allowed for a human-approved plan/apply."
  type        = list(string)

  validation {
    condition     = length(var.allowed_account_ids) == 1 && can(regex("^[0-9]{12}$", var.allowed_account_ids[0]))
    error_message = "allowed_account_ids must contain exactly one approved 12-digit AWS account ID."
  }
}

variable "create_github_oidc_provider" {
  description = "Create the account-global GitHub OIDC provider. Disable when one already exists."
  type        = bool
  default     = true
}

variable "existing_github_oidc_provider_arn" {
  description = "Existing GitHub OIDC provider ARN when creation is disabled."
  type        = string
  default     = null
  nullable    = true
}

variable "github_oidc_thumbprints" {
  description = "GitHub OIDC CA thumbprints; review against AWS/GitHub guidance before apply."
  type        = list(string)
  default     = ["6938fd4d98bab03faadb97b34396831e3780aea1"]

  validation {
    condition     = length(var.github_oidc_thumbprints) > 0 && alltrue([for value in var.github_oidc_thumbprints : can(regex("^[0-9a-f]{40}$", value))])
    error_message = "At least one lowercase 40-character SHA-1 thumbprint is required."
  }
}

variable "bronze_retention_days" {
  description = "Retention for Bronze object versions in the sprint sandbox."
  type        = number
  default     = 90

  validation {
    condition     = var.bronze_retention_days >= 30 && var.bronze_retention_days <= 365
    error_message = "Bronze retention must be between 30 and 365 days."
  }
}

variable "quarantine_retention_days" {
  description = "Retention for quarantine evidence in the sprint sandbox."
  type        = number
  default     = 30

  validation {
    condition     = var.quarantine_retention_days >= 14 && var.quarantine_retention_days <= 180
    error_message = "Quarantine retention must be between 14 and 180 days."
  }
}

variable "enable_ingestion_lambda" {
  description = "Explicit cost/runtime switch for the AWS-02 ingestion Lambda. Disabled until dependencies, sources and networking are reviewed."
  type        = bool
  default     = false
}

variable "lambda_dependency_layer_arns" {
  description = "Reviewed Python 3.12 Lambda layer ARNs containing compatible PyArrow and psycopg binaries."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for arn in var.lambda_dependency_layer_arns : can(regex("^arn:[a-z0-9-]+:lambda:[a-z0-9-]+:[0-9]{12}:layer:[A-Za-z0-9-_]+:[0-9]+$", arn))])
    error_message = "Every dependency layer must be a versioned Lambda layer ARN."
  }
}

variable "postgres_secret_arn" {
  description = "Exact Secrets Manager ARN containing the PostgreSQL DSN or connection fields."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.postgres_secret_arn == null || can(regex("^arn:[a-z0-9-]+:secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:[A-Za-z0-9/_+=.@-]+$", var.postgres_secret_arn))
    error_message = "postgres_secret_arn must be null or an exact Secrets Manager secret ARN."
  }
}

variable "rest_exchange_rates_url" {
  description = "HTTPS exchange-rates endpoint supplied to the ingestion runtime."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.rest_exchange_rates_url == null || startswith(var.rest_exchange_rates_url, "https://")
    error_message = "rest_exchange_rates_url must use HTTPS."
  }
}

variable "rest_locations_url" {
  description = "HTTPS locations endpoint supplied to the ingestion runtime."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.rest_locations_url == null || startswith(var.rest_locations_url, "https://")
    error_message = "rest_locations_url must use HTTPS."
  }
}

variable "monthly_budget_usd" {
  description = "Monthly cost alert threshold. This is not a hard spending limit."
  type        = number
  default     = 25

  validation {
    condition     = var.monthly_budget_usd >= 5 && var.monthly_budget_usd <= 100
    error_message = "The sprint budget must stay between USD 5 and USD 100."
  }
}

variable "budget_alert_email" {
  description = "Email for 80% forecast and 100% actual budget alerts."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.budget_alert_email == null || can(regex("^[^@[:space:]]+@[^@[:space:]]+\\.[^@[:space:]]+$", var.budget_alert_email))
    error_message = "budget_alert_email must be null or a valid email-shaped value."
  }
}

variable "enable_redshift_serverless" {
  description = "Explicit cost switch for the Redshift Serverless skeleton."
  type        = bool
  default     = false
}

variable "redshift_subnet_ids" {
  description = "At least three reviewed private subnet IDs when Redshift is enabled."
  type        = list(string)
  default     = []
}

variable "redshift_security_group_ids" {
  description = "Reviewed security group IDs when Redshift is enabled."
  type        = list(string)
  default     = []
}

variable "redshift_base_capacity_rpus" {
  description = "Base Redshift Serverless RPUs. AWS-01 deliberately caps this."
  type        = number
  default     = 8

  validation {
    condition     = var.redshift_base_capacity_rpus == 8
    error_message = "AWS-01 permits only the minimum reviewed base capacity of 8 RPUs."
  }
}

variable "redshift_max_capacity_rpus" {
  description = "Maximum Redshift Serverless RPUs."
  type        = number
  default     = 8

  validation {
    condition     = var.redshift_max_capacity_rpus == 8
    error_message = "AWS-01 caps Redshift Serverless at 8 RPUs."
  }
}
