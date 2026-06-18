# terraform/terraform.tfvars

aws_region      = "us-east-1"
project_name    = "auth-svc"
environment     = "prod"

domain_name     = "williamsilberste.in"
route53_zone_id = "Z04124311U7X00857DJXM"   # Your Route53 hosted zone ID

ses_from_email  = "mail@wsilbersilberste.in"
ses_from_domain = "williamsilberste.in"

# ── API host ──────────────────────────────────────────────────────────────────
instance_type      = "t3.small"
cors_allow_origins = "*"   # tighten to your consuming app's origin(s) in production

# ── Monitoring ────────────────────────────────────────────────────────────────
alarm_email = ""           # set to receive CloudWatch alarm emails

# ── Database ──────────────────────────────────────────────────────────────────
db_max_capacity = 8.0