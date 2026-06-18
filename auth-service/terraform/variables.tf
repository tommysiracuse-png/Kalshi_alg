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

# ── EC2 / App ────────────────────────────────────────────────────────────────

variable "app_image" {
  description = "Full Docker image URI. Leave empty to use the ECR repo created by this config after pushing your image."
  type        = string
  default     = ""
}

variable "app_port" {
  description = "Port the API container/host listens on (behind the ALB)"
  type        = number
  default     = 8000
}

variable "instance_type" {
  description = "EC2 instance type for the API host"
  type        = string
  default     = "t3.small"
}

variable "root_volume_size" {
  description = "Root EBS volume size in GiB"
  type        = number
  default     = 20
}

variable "cors_allow_origins" {
  description = "Comma-separated CORS origins the API accepts ('*' for any)"
  type        = string
  default     = "*"
}

# ── Monitoring / Alarms ──────────────────────────────────────────────────────

variable "alarm_email" {
  description = "Email address to receive CloudWatch alarm notifications. Leave empty to skip the subscription."
  type        = string
  default     = ""
}

variable "alarm_5xx_threshold" {
  description = "Number of 5xx responses per minute that triggers an alarm"
  type        = number
  default     = 5
}

variable "alarm_latency_threshold_seconds" {
  description = "p90 target response time (seconds) that triggers an alarm"
  type        = number
  default     = 2
}

variable "alarm_cpu_threshold" {
  description = "Instance CPU % that triggers an alarm"
  type        = number
  default     = 80
}
