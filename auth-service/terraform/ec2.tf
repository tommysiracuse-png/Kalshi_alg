locals {
  # Use provided image or fall back to the ECR repo (requires image to be pushed first).
  app_image = var.app_image != "" ? var.app_image : "${aws_ecr_repository.app.repository_url}:latest"
}

# Latest Amazon Linux 2023 AMI (x86_64) via the public SSM parameter.
data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

# The single API host. Lives in a private subnet; the ALB is its only ingress.
resource "aws_instance" "app" {
  ami                    = data.aws_ssm_parameter.al2023.value
  instance_type          = var.instance_type
  subnet_id              = aws_subnet.private[0].id
  vpc_security_group_ids = [aws_security_group.app.id]
  iam_instance_profile   = aws_iam_instance_profile.app.name

  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    aws_region         = var.aws_region
    ecr_image          = local.app_image
    db_secret_arn      = aws_secretsmanager_secret.db_password.arn
    jwt_secret_arn     = aws_secretsmanager_secret.jwt_secret.arn
    log_group          = aws_cloudwatch_log_group.app.name
    app_port           = var.app_port
    cw_agent_param     = aws_ssm_parameter.cw_agent_config.name
    ses_from_email     = var.ses_from_email
    cors_allow_origins = var.cors_allow_origins
  })

  metadata_options {
    http_tokens   = "required" # enforce IMDSv2
    http_endpoint = "enabled"
  }

  root_block_device {
    volume_size = var.root_volume_size
    volume_type = "gp3"
    encrypted   = true
  }

  monitoring = true # 1-minute detailed CloudWatch metrics

  tags = { Name = "${var.project_name}-api" }

  depends_on = [
    aws_secretsmanager_secret_version.db_password,
    aws_secretsmanager_secret_version.jwt_secret,
    aws_ssm_parameter.cw_agent_config,
  ]
}

# Register the instance with the ALB target group.
resource "aws_lb_target_group_attachment" "app" {
  target_group_arn = aws_lb_target_group.app.arn
  target_id        = aws_instance.app.id
  port             = var.app_port
}
