# terraform/terraform.tfvars

aws_region      = "us-east-1"
project_name    = "auth-svc"
environment     = "prod"

domain_name     = "williamsilberste.in"
route53_zone_id = "Z04124311U7X00857DJXM"   # Your Route53 hosted zone ID

ses_from_email  = "mail@wsilbersilberste.in"
ses_from_domain = "williamsilberste.in"

ecs_min_tasks = 2
ecs_max_tasks = 20
ecs_scale_out_cpu_threshold = 60
ecs_scale_in_cpu_threshold  = 20

db_max_capacity = 8.0