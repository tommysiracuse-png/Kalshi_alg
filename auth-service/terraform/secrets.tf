# DB password stored in Secrets Manager. The EC2 instance reads it at launch —
# the plaintext password never appears in user-data or instance metadata env vars.

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

# ── JWT signing secret ────────────────────────────────────────────────────────
# Used by the API to sign access tokens. Generated once and read by the instance
# at launch — never committed or placed in user-data plaintext.
resource "random_password" "jwt_secret" {
  length  = 64
  special = false
}

resource "aws_secretsmanager_secret" "jwt_secret" {
  name                    = "${var.project_name}/jwt-secret"
  description             = "JWT signing secret for ${var.project_name} API"
  recovery_window_in_days = 7

  tags = { Name = "${var.project_name}-jwt-secret" }
}

resource "aws_secretsmanager_secret_version" "jwt_secret" {
  secret_id     = aws_secretsmanager_secret.jwt_secret.id
  secret_string = jsonencode({ jwt_secret = random_password.jwt_secret.result })
}
