# Auth Service

A containerized authentication service with email/password login, two-factor authentication (2FA) via email or SMS, and license key access control. Built with Python/Streamlit, deployed on AWS using Docker and Terraform.

---

## Features

- Email/password registration and login with bcrypt hashing
- Two-factor authentication via email (AWS SES) or SMS (AWS SNS)
- Users must have at least one verified 2FA method to access their license key
- Persistent storage in Aurora PostgreSQL (AWS)
- Horizontally scalable — multiple containers behind an Application Load Balancer
- All AWS infrastructure defined in Terraform

---

## Architecture

```
Internet
   │
   ▼
Application Load Balancer (HTTPS, sticky sessions)
   │
   ├── ECS Fargate Task (Streamlit app) ─── AWS SES (email 2FA)
   ├── ECS Fargate Task (Streamlit app) ─── AWS SNS (SMS 2FA)
   └── ECS Fargate Task (Streamlit app)
              │
              ▼
   Aurora PostgreSQL Serverless v2 (private subnet)
```

**Sticky sessions** on the ALB keep each user's WebSocket connection pinned to a single container for the duration of their session — required for Streamlit's stateful architecture.

### Database — Aurora PostgreSQL Serverless v2

Chosen over standard RDS because:
- Auto-scales from 0.5 to 4 ACU (near-zero cost at idle, handles traffic bursts instantly)
- Multi-AZ failover is built in
- Fully managed with automated backups
- Encryption at rest enabled by default

### AWS Services Used

| Service | Purpose |
|---|---|
| ECS Fargate | Runs Streamlit app containers — no server management |
| Aurora PostgreSQL Serverless v2 | Persistent user data storage |
| Application Load Balancer | Routes traffic, terminates TLS, sticky sessions |
| AWS SES | Sends 2FA codes via email |
| AWS SNS | Sends 2FA codes via SMS |
| ECR | Stores Docker images |
| Secrets Manager | Stores DB credentials — never exposed in env vars |
| ACM | TLS certificate, auto-renewed |
| Route53 | DNS + automatic SES DKIM record provisioning |
| CloudWatch | Container logs (30-day retention) |

---

## Project Structure

```
auth-service/
├── app/
│   ├── main.py                  # Streamlit app (all pages + auth state machine)
│   ├── requirements.txt
│   ├── Dockerfile
│   ├── auth/
│   │   ├── authentication.py    # Login, registration, password hashing
│   │   └── two_factor.py        # 2FA code generation, SES/SNS sending, verification
│   ├── database/
│   │   ├── models.py            # SQLAlchemy ORM models
│   │   └── connection.py        # DB engine + session factory
│   └── utils/
│       └── validators.py        # Phone normalization, contact masking
├── terraform/
│   ├── main.tf                  # Provider config
│   ├── variables.tf             # All input variables
│   ├── outputs.tf               # Useful post-deploy values
│   ├── vpc.tf                   # VPC, subnets, NAT gateways, security groups
│   ├── rds.tf                   # Aurora PostgreSQL Serverless v2
│   ├── secrets.tf               # Secrets Manager (DB credentials)
│   ├── ecr.tf                   # Docker image registry
│   ├── ecs.tf                   # Fargate cluster, task definition, service
│   ├── alb.tf                   # Load balancer, HTTPS listener, ACM cert
│   ├── iam.tf                   # ECS execution and task IAM roles
│   ├── ses.tf                   # SES domain identity, DKIM/SPF/DMARC DNS records
│   ├── autoscaling.tf           # ECS auto-scaling policies (CPU-based)
│   └── cloudwatch.tf            # Log groups
├── docker-compose.yml           # Local development with local Postgres
└── .env.example                 # Environment variable template
```

---

## Local Development

### Prerequisites

- Docker and Docker Compose
- AWS credentials with SES and SNS permissions (for 2FA sending)

### Setup

**1. Copy the environment template:**

```bash
cp .env.example .env
```

**2. Fill in your AWS credentials in `.env`:**

```
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_REGION=us-east-1
SES_FROM_EMAIL=no-reply@yourdomain.com
```

> SES must be in production mode (out of sandbox) or both sender and recipient must be verified addresses. See [SES Production Access](#ses-production-access) below.

**3. Start the stack:**

```bash
docker compose up --build
```

The app will be available at `http://localhost:8501`.

A local PostgreSQL instance is started automatically. Database tables are created on first boot.

---

## Deploying to AWS

### Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.7
- [AWS CLI](https://aws.amazon.com/cli/) configured (`aws configure`)
- [Docker](https://docs.docker.com/get-docker/)
- A domain in a Route53 hosted zone
- SES production access granted for your sending domain

### Step 1 — Create a `terraform.tfvars` file

```hcl
# terraform/terraform.tfvars

aws_region      = "us-east-1"
project_name    = "auth-svc"
environment     = "prod"

domain_name     = "auth.yourdomain.com"
route53_zone_id = "Z0123456789ABCDEF"   # Your Route53 hosted zone ID

ses_from_email  = "no-reply@yourdomain.com"
ses_from_domain = "yourdomain.com"
```

### Step 2 — Deploy the infrastructure

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

This provisions everything: VPC, Aurora cluster, ECS cluster, ALB, ACM certificate (auto-validated via Route53), SES domain identity with DKIM/SPF/DMARC records, and IAM roles.

### Step 3 — Build and push the Docker image

After `terraform apply`, get the push commands:

```bash
terraform output ecr_push_commands
```

This outputs commands like:

```bash
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin 123456789.dkr.ecr.us-east-1.amazonaws.com

docker build -t auth-svc ../app
docker tag auth-svc:latest 123456789.dkr.ecr.us-east-1.amazonaws.com/auth-svc/app:latest
docker push 123456789.dkr.ecr.us-east-1.amazonaws.com/auth-svc/app:latest
```

### Step 4 — Deploy the updated image

Force a new ECS deployment to pick up the pushed image:

```bash
aws ecs update-service \
  --cluster auth-svc-cluster \
  --service auth-svc-service \
  --force-new-deployment
```

The app will be live at `https://auth.yourdomain.com`.

---

## User Flow

### Registration

1. Go to the app URL and click **Create an account**
2. Enter email and password
3. Click **Sign In** on the login page

### Setting up 2FA (required to access license key)

1. After signing in, go to the **Two-Factor Authentication** section on the dashboard
2. Click **Add Email 2FA** or **Add SMS 2FA**
3. Enter your contact (email address or US phone number)
4. Enter the 6-digit code that was sent
5. 2FA is now enabled — your license key section will unlock

### Signing in with 2FA

1. Enter email and password
2. A 6-digit code is automatically sent to your first active 2FA method
3. Enter the code to complete sign-in
4. Use **Use different method** if you have multiple methods set up

---

## Scaling

The ECS service auto-scales based on CPU utilization:

| Condition | Action |
|---|---|
| Average CPU ≥ 70% for 2 minutes | Add 2 tasks |
| Average CPU ≤ 30% for 5 minutes | Remove 1 task |

Default limits: min 2 tasks, max 10 tasks. These can be adjusted in `terraform.tfvars`:

```hcl
ecs_min_tasks = 2
ecs_max_tasks = 20
ecs_scale_out_cpu_threshold = 60
ecs_scale_in_cpu_threshold  = 20
```

Aurora Serverless v2 scales independently from 0.5 to 4 ACU. Adjust the ceiling in `terraform.tfvars` if you expect heavy query load:

```hcl
db_max_capacity = 8.0
```

---

## SES Production Access

AWS SES starts in **sandbox mode**, which only allows sending to verified email addresses. Before going live:

1. Open the [AWS SES console](https://console.aws.amazon.com/ses/home)
2. Navigate to **Account dashboard** → **Request production access**
3. Fill out the form describing your use case (transactional 2FA codes)
4. Approval typically takes 24 hours

SMS via SNS also has a default spending limit of $1/month. Request an increase under **SNS → Text messaging → Preferences** in the AWS console.

---

## Security Notes

- Passwords are hashed with bcrypt (cost factor 12)
- 2FA codes are also bcrypt-hashed before storage — the plaintext code only exists in memory and in transit
- Codes expire after 10 minutes and are single-use
- The DB password is stored in Secrets Manager and injected into ECS at launch — it never appears in task definition env vars or logs
- The RDS cluster is in a private subnet with no public access; only ECS tasks can connect via a scoped security group rule
- TLS 1.3 is enforced on the ALB listener (`ELBSecurityPolicy-TLS13-1-2-2021-06`)
- The IAM task role for SES is scoped to the configured `ses_from_email` address only

---

## Tearing Down

```bash
cd terraform
terraform destroy
```

> The Aurora cluster has `deletion_protection = true` and will take a final snapshot before being removed. You can disable deletion protection in `rds.tf` if you want a clean destroy without the snapshot prompt.
