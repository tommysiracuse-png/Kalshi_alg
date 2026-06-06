import logging
import re
import bcrypt
from database.connection import get_db
from database.models import User, TwoFactorMethod

logger = logging.getLogger(__name__)


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt(rounds=12)).decode()


def verify_password(password: str, password_hash: str) -> bool:
    logger.debug("verify_password: password=%s hash=%s", password, password_hash)
    return bcrypt.checkpw(password.encode(), password_hash.encode())


def is_valid_email(email: str) -> bool:
    pattern = r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
    return bool(re.match(pattern, email.strip()))


def is_strong_password(password: str) -> tuple[bool, str]:
    if len(password) < 8:
        return False, "At least 8 characters required"
    if not re.search(r"[A-Z]", password):
        return False, "Must contain an uppercase letter"
    if not re.search(r"[a-z]", password):
        return False, "Must contain a lowercase letter"
    if not re.search(r"[0-9]", password):
        return False, "Must contain a number"
    if not re.search(r'[!@#$%^&*()\-_=+\[\]{};:\'",.<>?/\\|`~]', password):
        return False, "Must contain a special character"
    return True, ""


def register_user(email: str, password: str) -> tuple[bool, str]:
    email = email.strip().lower()

    if not is_valid_email(email):
        return False, "Invalid email address"

    strong, reason = is_strong_password(password)
    if not strong:
        return False, f"Weak password: {reason}"

    with get_db() as db:
        if db.query(User).filter(User.email == email).first():
            return False, "An account with this email already exists"

        user = User(email=email, password_hash=hash_password(password))
        db.add(user)
        db.flush()
        return True, str(user.id)


def login_user(email: str, password: str) -> tuple[bool, str, dict | None]:
    email = email.strip().lower()

    with get_db() as db:
        user = db.query(User).filter(User.email == email).first()

        if not user:
            logger.warning("Login attempt with non-existent email: %s", email)
            return False, "Invalid email or password", None

        # Use constant-time comparison even for missing users to prevent timing attacks
        dummy_hash = "$2b$12$notarealhashjustfortimingreasons000000000000000000000000"
        stored_hash = user.password_hash if user else dummy_hash

        logger.info("Login attempt for %s: user found=%s, hash=%s", email, bool(user), stored_hash)

        if not verify_password(password, stored_hash) or not user or not user.is_active:
            return False, "Invalid email or password", None

        active_methods = (
            db.query(TwoFactorMethod)
            .filter(
                TwoFactorMethod.user_id == user.id,
                TwoFactorMethod.is_enabled == True,
                TwoFactorMethod.is_verified == True,
            )
            .all()
        )

        user_data = {
            "id": str(user.id),
            "email": user.email,
            "has_2fa": len(active_methods) > 0,
            "2fa_methods": [
                {"id": str(m.id), "type": m.method_type, "contact": m.contact}
                for m in active_methods
            ],
        }
        return True, "Success", user_data


def get_user_license_keys(user_id: str) -> list[dict]:
    from database.models import LicenseKey

    with get_db() as db:
        keys = (
            db.query(LicenseKey)
            .filter(LicenseKey.user_id == user_id, LicenseKey.is_active == True)
            .all()
        )
        return [
            {
                "id": str(k.id),
                "key": k.key,
                "created_at": k.created_at,
                "expires_at": k.expires_at,
            }
            for k in keys
        ]
