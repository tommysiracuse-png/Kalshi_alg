# Aurora PostgreSQL Serverless v2
# Chosen over RDS because it auto-scales to zero at low load (cost efficient),
# handles bursts well, and is fully managed with multi-AZ failover built in.

resource "random_password" "db" {
  length           = 32
  special          = true
  override_special = "!#$%&*()-_=+[]{}|;:,.<>?"
}

resource "aws_db_subnet_group" "main" {
  name       = "${var.project_name}-db-subnet-group"
  subnet_ids = aws_subnet.private[*].id
  tags       = { Name = "${var.project_name}-db-subnet-group" }
}

resource "aws_rds_cluster" "main" {
  cluster_identifier = "${var.project_name}-cluster"
  engine             = "aurora-postgresql"
  engine_mode        = "provisioned"
  engine_version     = "16.1"

  database_name   = var.db_name
  master_username = var.db_username
  master_password = random_password.db.result

  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]

  # Encryption at rest (mandatory for auth data)
  storage_encrypted = true

  serverlessv2_scaling_configuration {
    min_capacity = var.db_min_capacity
    max_capacity = var.db_max_capacity
  }

  # Automated backups retained 7 days
  backup_retention_period   = 7
  preferred_backup_window   = "03:00-04:00"
  deletion_protection       = true
  skip_final_snapshot       = false
  final_snapshot_identifier = "${var.project_name}-final-snapshot"

  tags = { Name = "${var.project_name}-aurora-cluster" }
}

resource "aws_rds_cluster_instance" "main" {
  count = 2 # writer + one reader for failover

  identifier         = "${var.project_name}-instance-${count.index + 1}"
  cluster_identifier = aws_rds_cluster.main.id
  instance_class     = "db.serverless"
  engine             = aws_rds_cluster.main.engine
  engine_version     = aws_rds_cluster.main.engine_version

  monitoring_interval = 60
  monitoring_role_arn = aws_iam_role.rds_enhanced_monitoring.arn

  tags = { Name = "${var.project_name}-aurora-${count.index + 1}" }
}

# Enhanced monitoring role
resource "aws_iam_role" "rds_enhanced_monitoring" {
  name = "${var.project_name}-rds-monitoring-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "monitoring.rds.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "rds_enhanced_monitoring" {
  role       = aws_iam_role.rds_enhanced_monitoring.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonRDSEnhancedMonitoringRole"
}
