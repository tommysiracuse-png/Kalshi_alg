output "alb_dns_name" {
  description = "ALB DNS name — point your domain CNAME here if not using Route53"
  value       = aws_lb.main.dns_name
}

output "app_url" {
  description = "Public URL of the auth service"
  value       = "https://${var.domain_name}"
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

output "ecs_cluster_name" {
  description = "ECS cluster name"
  value       = aws_ecs_cluster.main.name
}
