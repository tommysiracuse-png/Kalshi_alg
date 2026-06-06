# terraform/terraform.tfvars

aws_region      = "us-east-1"
project_name    = "auth-svc"
environment     = "prod"

domain_name     = "williamsilberste.in"
route53_zone_id = "Z03962003HJ274HFHQF7Z"   # Your Route53 hosted zone ID

ses_from_email  = "mail@wsilbersilberste.in"
ses_from_domain = "williamsilberste.in"