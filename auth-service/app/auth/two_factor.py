import os
import random
import string
import bcrypt
from datetime import datetime, timezone, timedelta
import boto3
from botocore.exceptions import ClientError
from database.connection import get_db
from database.models import TwoFactorMethod, TwoFactorCode

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
SES_FROM_EMAIL = os.environ.get("SES_FROM_EMAIL", "")


def _generate_code() -> str:
    return "".join(random.choices(string.digits, k=6))


def _hash_code(code: str) -> str:
    return bcrypt.hashpw(code.encode(), bcrypt.gensalt(rounds=10)).decode()


def _verify_code_hash(code: str, code_hash: str) -> bool:
    return bcrypt.checkpw(code.encode(), code_hash.encode())


def _send_email_code(to_email: str, code: str) -> tuple[bool, str]:
    try:
        ses = boto3.client("ses", region_name=AWS_REGION)
        ses.send_email(
            Source=SES_FROM_EMAIL,
            Destination={"ToAddresses": [to_email]},
            Message={
                "Subject": {"Data": "Your verification code"},
                "Body": {
                    "Text": {
                        "Data": (
                            f"Your verification code is: {code}\n"
                            "This code expires in 10 minutes.\n\n"
                            "If you did not request this, please ignore this email."
                        )
                    },
                    "Html": {
                        "Data": (
                            f"<p>Your verification code is: <strong style='font-size:24px;letter-spacing:4px'>{code}</strong></p>"
                            "<p>This code expires in <strong>10 minutes</strong>.</p>"
                            "<p style='color:#888;font-size:12px'>If you did not request this code, you can safely ignore this email.</p>"
                        )
                    },
                },
            },
        )
        return True, ""
    except ClientError as e:
        return False, e.response["Error"]["Message"]


def _send_sms_code(phone: str, code: str) -> tuple[bool, str]:
    try:
        sns = boto3.client("sns", region_name=AWS_REGION)
        sns.publish(
            PhoneNumber=phone,
            Message=f"Your verification code is: {code}. Expires in 10 minutes.",
            MessageAttributes={
                "AWS.SNS.SMS.SMSType": {
                    "DataType": "String",
                    "StringValue": "Transactional",
                }
            },
        )
        return True, ""
    except ClientError as e:
        return False, e.response["Error"]["Message"]


def _store_and_send_code(
    user_id: str, method_type: str, contact: str
) -> tuple[bool, str]:
    code = _generate_code()
    code_hash = _hash_code(code)
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=10)

    with get_db() as db:
        # Invalidate any previous pending codes for this user
        db.query(TwoFactorCode).filter(
            TwoFactorCode.user_id == user_id,
            TwoFactorCode.used == False,
        ).update({"used": True})

        db.add(
            TwoFactorCode(
                user_id=user_id,
                code_hash=code_hash,
                method_type=method_type,
                expires_at=expires_at,
            )
        )

    if method_type == "email":
        ok, err = _send_email_code(contact, code)
    else:
        ok, err = _send_sms_code(contact, code)

    if not ok:
        return False, f"Failed to send code: {err}"
    return True, "Code sent successfully"


def verify_2fa_code(user_id: str, code: str) -> tuple[bool, str]:
    with get_db() as db:
        pending = (
            db.query(TwoFactorCode)
            .filter(
                TwoFactorCode.user_id == user_id,
                TwoFactorCode.used == False,
                TwoFactorCode.expires_at > datetime.now(timezone.utc),
            )
            .order_by(TwoFactorCode.created_at.desc())
            .first()
        )

        if not pending:
            return False, "No valid code found. Please request a new code."

        if not _verify_code_hash(code.strip(), pending.code_hash):
            return False, "Incorrect code. Please try again."

        pending.used = True
        return True, "Code verified"


def send_login_challenge(user_id: str, method: dict) -> tuple[bool, str]:
    return _store_and_send_code(user_id, method["type"], method["contact"])


def get_user_2fa_methods(user_id: str) -> list[dict]:
    with get_db() as db:
        methods = (
            db.query(TwoFactorMethod)
            .filter(TwoFactorMethod.user_id == user_id)
            .all()
        )
        return [
            {
                "id": str(m.id),
                "type": m.method_type,
                "contact": m.contact,
                "verified": m.is_verified,
                "enabled": m.is_enabled,
            }
            for m in methods
        ]


def initiate_2fa_setup(
    user_id: str, method_type: str, contact: str
) -> tuple[bool, str]:
    with get_db() as db:
        existing = (
            db.query(TwoFactorMethod)
            .filter(
                TwoFactorMethod.user_id == user_id,
                TwoFactorMethod.method_type == method_type,
            )
            .first()
        )
        if existing:
            existing.contact = contact
            existing.is_verified = False
            existing.is_enabled = False
        else:
            db.add(
                TwoFactorMethod(
                    user_id=user_id,
                    method_type=method_type,
                    contact=contact,
                    is_verified=False,
                    is_enabled=False,
                )
            )

    return _store_and_send_code(user_id, method_type, contact)


def confirm_2fa_setup(
    user_id: str, method_type: str, code: str
) -> tuple[bool, str]:
    ok, msg = verify_2fa_code(user_id, code)
    if not ok:
        return False, msg

    with get_db() as db:
        method = (
            db.query(TwoFactorMethod)
            .filter(
                TwoFactorMethod.user_id == user_id,
                TwoFactorMethod.method_type == method_type,
            )
            .first()
        )
        if not method:
            return False, "Method not found"
        method.is_verified = True
        method.is_enabled = True

    return True, f"{method_type.upper()} 2FA successfully enabled"


def disable_2fa_method(user_id: str, method_type: str) -> tuple[bool, str]:
    with get_db() as db:
        method = (
            db.query(TwoFactorMethod)
            .filter(
                TwoFactorMethod.user_id == user_id,
                TwoFactorMethod.method_type == method_type,
            )
            .first()
        )
        if not method:
            return False, "Method not found"

        # Check that this isn't the last active method
        active_count = (
            db.query(TwoFactorMethod)
            .filter(
                TwoFactorMethod.user_id == user_id,
                TwoFactorMethod.is_enabled == True,
                TwoFactorMethod.is_verified == True,
                TwoFactorMethod.method_type != method_type,
            )
            .count()
        )
        if active_count == 0:
            return (
                False,
                "Cannot disable your only active 2FA method. Add another method first.",
            )

        method.is_enabled = False
        return True, f"{method_type.upper()} 2FA disabled"
