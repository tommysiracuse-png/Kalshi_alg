# Auth Service API

A REST authentication API with email/password login, two-factor authentication
(2FA) via email or SMS, and license-key access control. Built with
**FastAPI**, packaged as a Docker container, and deployed on a single **EC2**
instance on AWS with Terraform. Designed to be consumed by a separate
application (e.g. another service hosted in AWS) over HTTP/JSON.

---

## Features

- REST endpoints for account creation, login, 2FA, and license-key retrieval
- **JWT access tokens** — login returns a signed bearer token the consuming app sends on subsequent requests
- Two-factor authentication via email (AWS SES) or SMS (AWS SNS)
- Email/password hashing with bcrypt; 2FA codes hashed, single-use, 10-min expiry
- Persistent storage in Aurora PostgreSQL (unchanged from the original design)
- Full request + server monitoring via CloudWatch (structured access logs, metrics, dashboard, alarms)
- Interactive OpenAPI docs at `/docs`
- All AWS infrastructure defined in Terraform

---

## Architecture

```
Consuming Application (AWS)
   │  HTTPS + Bearer JWT
   ▼
Application Load Balancer (HTTPS, TLS 1.3)
   │
   ▼
EC2 instance (private subnet)
   └── Docker container: FastAPI / Uvicorn  ─── AWS SES (email 2FA)
              │                              └── AWS SNS (SMS 2FA)
              ▼
   Aurora PostgreSQL Serverless v2 (private subnet)

Monitoring:  CloudWatch Logs (access logs) · CloudWatch metrics ·
             Dashboard · Alarms → SNS email
```

The EC2 instance runs in a **private subnet** — the ALB is its only inbound
path, and it reaches ECR / Secrets Manager / SES / SNS outbound through a NAT
gateway. Secrets (DB URL, JWT signing key) are pulled from Secrets Manager at
launch and never appear in user-data or environment dumps.

### AWS Services Used

| Service | Purpose |
|---|---|
| EC2 | Runs the API container (single instance, auto-recovers on failure) |
| Aurora PostgreSQL Serverless v2 | Persistent user data storage *(unchanged)* |
| Application Load Balancer | Routes traffic, terminates TLS |
| AWS SES | Sends 2FA codes via email |
| AWS SNS | Sends 2FA codes via SMS |
| ECR | Stores the Docker image |
| Secrets Manager | DB credentials + JWT signing secret |
| ACM | TLS certificate, auto-renewed |
| Route53 | DNS + SES DKIM record provisioning |
| CloudWatch | Logs, metrics, dashboard, alarms |
| SSM | Session Manager shell access + CloudWatch agent config |

---

## API Reference

Base URL: `https://<your-domain>` · Interactive docs: `https://<your-domain>/docs`

| Method | Path | Auth | Description |
|---|---|---|---|
| `POST` | `/auth/register` | — | Create an account |
| `POST` | `/auth/login` | — | Log in; returns a JWT, or a 2FA challenge |
| `POST` | `/auth/2fa/verify` | challenge token | Submit a 2FA code, returns a JWT |
| `GET`  | `/auth/me` | Bearer | Current user identity |
| `GET`  | `/auth/license-keys` | Bearer (+2FA) | List the user's license keys |
| `GET`  | `/auth/2fa/methods` | Bearer | List configured 2FA methods |
| `POST` | `/auth/2fa/setup` | Bearer | Begin adding an email/SMS 2FA method |
| `POST` | `/auth/2fa/confirm` | Bearer | Confirm a 2FA method with the emailed/texted code |
| `DELETE` | `/auth/2fa/{method_type}` | Bearer | Disable a 2FA method |
| `GET`  | `/health` | — | Liveness + DB check (ALB health check) |
| `GET`  | `/metrics` | — | Prometheus metrics |

### Example flow

```bash
BASE=https://auth.example.com

# 1. Register
curl -sX POST $BASE/auth/register \
  -H 'content-type: application/json' \
  -d '{"email":"you@example.com","password":"S3cur3!pass"}'

# 2. Log in
curl -sX POST $BASE/auth/login \
  -H 'content-type: application/json' \
  -d '{"email":"you@example.com","password":"S3cur3!pass"}'
# → {"access_token":"eyJ...","token_type":"bearer","expires_in":43200}
#   (or, if 2FA is enabled: {"challenge_required":true,"challenge_token":"eyJ...", ...})

# 2b. If a challenge was returned, submit the 6-digit code:
curl -sX POST $BASE/auth/2fa/verify \
  -H 'content-type: application/json' \
  -d '{"challenge_token":"eyJ...","code":"123456"}'

# 3. Call an authenticated endpoint
curl -s $BASE/auth/me -H "authorization: Bearer eyJ..."
```

---

## Local Development

```bash
cp .env.example .env          # fill in AWS creds + SES_FROM_EMAIL for 2FA
docker compose up --build
```

The API is then available at `http://localhost:8000` (docs at
`http://localhost:8000/docs`). A local PostgreSQL instance starts automatically;
tables are created on first boot. `JWT_SECRET` defaults to a dev value — set a
strong one for anything non-local.

### Browser test console

A point-and-click console for exercising every endpoint (it chains the flow:
register → login → 2FA → authenticated calls, auto-storing the JWT/challenge
tokens) is served at **`http://localhost:8000/test`**.

It is gated behind the `ENABLE_TEST_UI` env var, which `docker-compose.yml` sets
for local dev only. The AWS deployment never sets it, so `/test` is unreachable
in production.

---

## Project Structure

```
auth-service/
├── app/
│   ├── api/
│   │   ├── app.py            # FastAPI app: routes, startup, CORS
│   │   ├── schemas.py        # Pydantic request/response models
│   │   ├── security.py       # JWT issuance/verification + auth dependency
│   │   └── monitoring.py     # Request logging + Prometheus middleware
│   ├── auth/                 # Login, registration, 2FA logic (reused, unchanged)
│   ├── database/             # SQLAlchemy models + connection
│   ├── utils/                # Phone normalization, contact masking
│   ├── main.py               # Legacy Streamlit UI (local use only; not deployed)
│   ├── requirements.txt
│   └── Dockerfile            # Runs uvicorn on :8000
├── terraform/
│   ├── main.tf · variables.tf · outputs.tf
│   ├── vpc.tf                # VPC, subnets, NAT, security groups
│   ├── rds.tf                # Aurora PostgreSQL Serverless v2 (unchanged)
│   ├── secrets.tf            # DB password + JWT signing secret
│   ├── ecr.tf                # Docker image registry
│   ├── ec2.tf                # EC2 instance, instance profile, ALB attachment
│   ├── user_data.sh.tftpl    # Bootstrap: Docker, CloudWatch agent, run container
│   ├── alb.tf                # Load balancer, HTTPS listener, ACM cert
│   ├── iam.tf                # EC2 instance role (secrets, SES/SNS, ECR, CW, SSM)
│   ├── ses.tf                # SES domain identity + DNS records
│   └── cloudwatch.tf         # Log group, agent config, alarms, dashboard, SNS
├── docker-compose.yml        # Local dev with local Postgres
└── .env.example
```

---

## Deploying to AWS

### Prerequisites

- Terraform >= 1.7, AWS CLI configured, Docker
- A domain in a Route53 hosted zone, SES production access (see below)

### Step 1 — Configure `terraform/terraform.tfvars`

```hcl
aws_region      = "us-east-1"
project_name    = "auth-svc"
environment     = "prod"

domain_name     = "auth.yourdomain.com"
route53_zone_id = "Z0123456789ABCDEF"

ses_from_email  = "no-reply@yourdomain.com"
ses_from_domain = "yourdomain.com"

instance_type      = "t3.small"
cors_allow_origins = "https://your-consuming-app.com"
alarm_email        = "ops@yourdomain.com"   # receives CloudWatch alarm emails
```

### Step 2 — Provision infrastructure

```bash
cd terraform
terraform init
terraform apply
```

This creates the VPC, Aurora cluster, ECR repo, ALB + ACM cert, SES identity,
the EC2 instance, IAM roles, and all CloudWatch monitoring.

### Step 3 — Build and push the image

```bash
terraform output -raw ecr_push_commands | bash
```

### Step 4 — Roll out the image

The instance pulls `:latest` at boot. After pushing a new image, replace the
host (cleanest way to pick up a new image) or restart the container in place:

```bash
# Restart the container on the running host via SSM:
aws ssm start-session --target "$(terraform output -raw instance_id)"
#   then on the host:  sudo docker pull <image> && sudo docker restart auth-api

# — or — recreate the instance so user-data re-runs:
terraform apply -replace="aws_instance.app"
```

The API is live at `https://auth.yourdomain.com` (docs at `/docs`).

---

## Monitoring

Everything is visible in one CloudWatch **dashboard**
(`<project>-overview` — see `terraform output dashboard_url`):

- **Per-request view** — request count by status class (2xx/4xx/5xx), response
  time percentiles (p50/p90/p99), and a live table of the most recent requests
  from the structured access log.
- **Server status** — instance CPU & memory %, healthy-host count, and EC2
  status-check results.

**Access logs** — the API logs every request as a JSON line
(`method`, `route`, `status`, `duration_ms`, `client_ip`, `request_id`) shipped
to the `/auth-svc/<project>/api` CloudWatch log group via the Docker `awslogs`
driver. Query them in Logs Insights:

```
fields @timestamp, method, route, status, duration_ms, client_ip
| filter event = 'http_request' and status >= 500
| sort @timestamp desc
```

**Prometheus** — `/metrics` exposes `http_requests_total` and
`http_request_duration_seconds` for any Prometheus-compatible scraper.

**Alarms** (notify the `alarm_email` SNS topic):

| Alarm | Trigger |
|---|---|
| Target 5xx | > `alarm_5xx_threshold` 5xx/min |
| No healthy host | Healthy targets < 1 |
| High latency | p90 response time > `alarm_latency_threshold_seconds` |
| High CPU | Instance CPU > `alarm_cpu_threshold` % |
| Status check failed | System status check fails → **auto-recovers** the instance |

> Setting `alarm_email` subscribes the address to the SNS topic — confirm the
> subscription via the email AWS sends after `apply`.

---

## Security Notes

- Passwords and 2FA codes are bcrypt-hashed; codes expire after 10 minutes and are single-use
- JWT access tokens (HS256) are signed with a 64-char secret generated by Terraform and stored in Secrets Manager — never in user-data or task env
- The DB and JWT secrets are read by the instance at launch; the instance role is scoped to exactly those two secret ARNs
- Aurora is in a private subnet; only the API instance's security group can reach port 5432
- The EC2 instance is in a private subnet with no public IP; shell access is via SSM Session Manager (no open SSH port)
- IMDSv2 is enforced; the root volume is encrypted
- TLS 1.3 on the ALB listener; SES IAM permission is scoped to the configured from-address

---

## SES Production Access

AWS SES starts in **sandbox mode** (only sends to verified addresses). Request
production access in the SES console → **Account dashboard** → **Request
production access**. SMS via SNS has a default $1/month limit — raise it under
**SNS → Text messaging → Preferences**.

---

## Tearing Down

```bash
# 1. Empty the ECR repo (Terraform won't delete a non-empty repo)
aws ecr batch-delete-image --repository-name auth-svc/app \
  --image-ids "$(aws ecr list-images --repository-name auth-svc/app --query 'imageIds[*]' --output json)"

# 2. Destroy
cd terraform && terraform destroy
```

Aurora takes a final snapshot (`<project>-final-snapshot`) on destroy; delete it
manually afterward if you don't want to keep the recovery point.
