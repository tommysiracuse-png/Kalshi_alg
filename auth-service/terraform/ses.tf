# SES domain identity for sending 2FA emails.
# After apply, you must add the DKIM CNAME records shown in the output to your DNS.
# SES starts in "sandbox" mode — request production access in the AWS console before going live.

resource "aws_ses_domain_identity" "main" {
  domain = var.ses_from_domain
}

resource "aws_ses_domain_dkim" "main" {
  domain = aws_ses_domain_identity.main.domain
}

# Add DKIM records to Route53 automatically
resource "aws_route53_record" "ses_dkim" {
  count   = 3
  zone_id = var.route53_zone_id
  name    = "${aws_ses_domain_dkim.main.dkim_tokens[count.index]}._domainkey"
  type    = "CNAME"
  ttl     = 600
  records = ["${aws_ses_domain_dkim.main.dkim_tokens[count.index]}.dkim.amazonses.com"]
}

# SPF record so receiving servers trust mail from SES
resource "aws_route53_record" "ses_spf" {
  zone_id = var.route53_zone_id
  name    = var.ses_from_domain
  type    = "TXT"
  ttl     = 600
  records = ["v=spf1 include:amazonses.com -all"]
}

# DMARC policy
resource "aws_route53_record" "ses_dmarc" {
  zone_id = var.route53_zone_id
  name    = "_dmarc.${var.ses_from_domain}"
  type    = "TXT"
  ttl     = 600
  records = ["v=DMARC1; p=quarantine; rua=mailto:dmarc@${var.ses_from_domain}"]
}

# Verify the from-address itself (needed while in SES sandbox)
resource "aws_ses_email_identity" "from" {
  email = var.ses_from_email
}
