# ── EC2 Instance Role ─────────────────────────────────────────────────────────
# Attached to the API EC2 instance. Grants exactly what the host needs:
#   * read the DB + JWT secrets at launch
#   * send 2FA codes via SES / SNS
#   * pull the container image from ECR
#   * ship logs/metrics to CloudWatch
#   * SSM Session Manager access (shell without opening SSH)

resource "aws_iam_role" "app_instance" {
  name = "${var.project_name}-instance-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = { Name = "${var.project_name}-instance-role" }
}

resource "aws_iam_instance_profile" "app" {
  name = "${var.project_name}-instance-profile"
  role = aws_iam_role.app_instance.name
}

# Read the DB and JWT secrets only.
resource "aws_iam_role_policy" "app_secrets" {
  name = "${var.project_name}-instance-secrets"
  role = aws_iam_role.app_instance.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["secretsmanager:GetSecretValue"]
      Resource = [
        aws_secretsmanager_secret.db_password.arn,
        aws_secretsmanager_secret.jwt_secret.arn,
      ]
    }]
  })
}

# Send 2FA codes via SES (scoped to the from-address) and SMS via SNS.
resource "aws_iam_role_policy" "app_ses_sns" {
  name = "${var.project_name}-ses-sns-policy"
  role = aws_iam_role.app_instance.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["ses:SendEmail", "ses:SendRawEmail"]
        Resource = "*"
        Condition = {
          StringEquals = { "ses:FromAddress" = var.ses_from_email }
        }
      },
      {
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = "*"
        Condition = {
          StringEquals = { "sns:Protocol" = "sms" }
        }
      }
    ]
  })
}

# Pull the container image from the private ECR repo.
resource "aws_iam_role_policy" "app_ecr_pull" {
  name = "${var.project_name}-ecr-pull"
  role = aws_iam_role.app_instance.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["ecr:GetAuthorizationToken"]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "ecr:BatchGetImage",
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchCheckLayerAvailability",
        ]
        Resource = aws_ecr_repository.app.arn
      }
    ]
  })
}

# CloudWatch agent (metrics + log shipping) and SSM Session Manager.
resource "aws_iam_role_policy_attachment" "cloudwatch_agent" {
  role       = aws_iam_role.app_instance.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
}

resource "aws_iam_role_policy_attachment" "ssm_core" {
  role       = aws_iam_role.app_instance.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

# Allow the Docker awslogs log driver to create streams in the app log group.
resource "aws_iam_role_policy" "app_logs" {
  name = "${var.project_name}-instance-logs"
  role = aws_iam_role.app_instance.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogStreams",
      ]
      Resource = "${aws_cloudwatch_log_group.app.arn}:*"
    }]
  })
}
