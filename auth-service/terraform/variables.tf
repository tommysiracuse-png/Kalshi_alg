variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Short name used as a prefix for all resources"
  type        = string
  default     = "auth-svc"
}

variable "environment" {
  description = "Deployment environment label (dev / staging / prod)"
  type        = string
  default     = "prod"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod"
  }
}

# ── Domain & SSL ─────────────────────────────────────────────────────────────

variable "domain_name" {
  description = "Public domain for the service, e.g. auth.example.com. Must be in a Route53 hosted zone."
  type        = string
}

variable "route53_zone_id" {
  description = "Route53 hosted zone ID that owns the domain_name"
  type        = string
}

# ── SES ──────────────────────────────────────────────────────────────────────

variable "ses_from_email" {
  description = "Verified SES email address used to send 2FA codes"
  type        = string
}

variable "ses_from_domain" {
  description = "Domain to verify in SES for email sending (e.g. example.com)"
  type        = string
}

# ── Database ─────────────────────────────────────────────────────────────────

variable "db_name" {
  description = "PostgreSQL database name"
  type        = string
  default     = "authdb"
}

variable "db_username" {
  description = "PostgreSQL master username"
  type        = string
  default     = "authuser"
  sensitive   = true
}

variable "db_min_capacity" {
  description = "Aurora Serverless v2 minimum capacity (ACU)"
  type        = number
  default     = 0.5
}

variable "db_max_capacity" {
  description = "Aurora Serverless v2 maximum capacity (ACU)"
  type        = number
  default     = 4.0
}

# ── ECS / App ────────────────────────────────────────────────────────────────

variable "app_image" {
  description = "Full Docker image URI. Leave empty to use the ECR repo created by this config after pushing your image."
  type        = string
  default     = ""
}

variable "ecs_cpu" {
  description = "Fargate task CPU units (256=0.25 vCPU, 512=0.5, 1024=1)"
  type        = number
  default     = 512
}

variable "ecs_memory" {
  description = "Fargate task memory in MiB"
  type        = number
  default     = 1024
}

variable "ecs_min_tasks" {
  description = "Minimum number of running ECS tasks"
  type        = number
  default     = 2
}

variable "ecs_max_tasks" {
  description = "Maximum number of ECS tasks (scale ceiling)"
  type        = number
  default     = 10
}

variable "ecs_scale_out_cpu_threshold" {
  description = "Average CPU % that triggers scale-out"
  type        = number
  default     = 70
}

variable "ecs_scale_in_cpu_threshold" {
  description = "Average CPU % that triggers scale-in"
  type        = number
  default     = 30
}
