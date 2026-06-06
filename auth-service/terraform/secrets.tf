# DB password stored in Secrets Manager and rotated automatically.
# The ECS task reads it at runtime — the plaintext password never appears in task env vars.

resource "aws_secretsmanager_secret" "db_password" {
  name                    = "${var.project_name}/db-password"
  description             = "Aurora master password for ${var.project_name}"
  recovery_window_in_days = 7

  tags = { Name = "${var.project_name}-db-secret" }
}

resource "aws_secretsmanager_secret_version" "db_password" {
  secret_id = aws_secretsmanager_secret.db_password.id
  secret_string = jsonencode({
    username = var.db_username
    password = random_password.db.result
    host     = aws_rds_cluster.main.endpoint
    port     = 5432
    dbname   = var.db_name
    # Full connection URL for SQLAlchemy
    database_url = "postgresql://${var.db_username}:${random_password.db.result}@${aws_rds_cluster.main.endpoint}:5432/${var.db_name}"
  })
}
