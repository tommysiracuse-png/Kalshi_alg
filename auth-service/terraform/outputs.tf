output "alb_dns_name" {
  description = "ALB DNS name — point your domain CNAME here if not using Route53"
  value       = aws_lb.main.dns_name
}

output "api_url" {
  description = "Public base URL of the auth API"
  value       = "https://${var.domain_name}"
}

output "api_docs_url" {
  description = "Interactive OpenAPI docs"
  value       = "https://${var.domain_name}/docs"
}

output "instance_id" {
  description = "EC2 instance ID running the API"
  value       = aws_instance.app.id
}

output "dashboard_url" {
  description = "CloudWatch dashboard for requests and server status"
  value       = "https://${var.aws_region}.console.aws.amazon.com/cloudwatch/home?region=${var.aws_region}#dashboards:name=${aws_cloudwatch_dashboard.main.dashboard_name}"
}

output "ssm_session_command" {
  description = "Open a shell on the instance without SSH"
  value       = "aws ssm start-session --target ${aws_instance.app.id} --region ${var.aws_region}"
}

output "ecr_repository_url" {
  description = "ECR repository URL — push your Docker image here"
  value       = aws_ecr_repository.app.repository_url
}

output "ecr_push_commands" {
  description = "Commands to build and push the Docker image to ECR"
  value = <<-EOT
    aws ecr get-login-password --region ${var.aws_region} | \
      docker login --username AWS --password-stdin ${aws_ecr_repository.app.repository_url}
    docker build -t ${var.project_name} ./app
    docker tag ${var.project_name}:latest ${aws_ecr_repository.app.repository_url}:latest
    docker push ${aws_ecr_repository.app.repository_url}:latest
  EOT
}

output "rds_endpoint" {
  description = "Aurora cluster writer endpoint (private, accessible only within VPC)"
  value       = aws_rds_cluster.main.endpoint
  sensitive   = true
}

output "db_secret_arn" {
  description = "Secrets Manager ARN containing the DB password"
  value       = aws_secretsmanager_secret.db_password.arn
}

output "redeploy_command" {
  description = "Pull the latest image and restart the API container on the instance"
  value       = "aws ssm send-command --instance-ids ${aws_instance.app.id} --document-name AWS-RunShellScript --region ${var.aws_region} --parameters 'commands=[\"/var/lib/cloud/instance/scripts/part-001\"]'"
}
