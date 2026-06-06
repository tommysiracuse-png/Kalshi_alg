resource "aws_cloudwatch_log_group" "app" {
  name              = "/ecs/${var.project_name}/app"
  retention_in_days = 30

  tags = { Name = "${var.project_name}-log-group" }
}
