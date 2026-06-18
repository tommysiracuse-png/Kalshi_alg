"""JWT issuance/verification and the FastAPI auth dependency.

Two token kinds are issued:
  * ``access``  — returned after a fully authenticated login; the separate
                  application sends it as ``Authorization: Bearer <token>``.
  * ``challenge`` — short-lived, returned when a login still needs a 2FA code.
                    It only authorizes the ``/auth/2fa/verify`` exchange.

The signing secret comes from ``JWT_SECRET`` (injected from Secrets Manager in
production). Tokens are signed with HS256.
"""

import os
import logging
from datetime import datetime, timezone, timedelta

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

logger = logging.getLogger(__name__)

JWT_SECRET = os.environ.get("JWT_SECRET", "")
JWT_ALGORITHM = "HS256"
ACCESS_TOKEN_TTL_SECONDS = int(os.environ.get("ACCESS_TOKEN_TTL_SECONDS", 60 * 60 * 12))  # 12h
CHALLENGE_TOKEN_TTL_SECONDS = int(os.environ.get("CHALLENGE_TOKEN_TTL_SECONDS", 60 * 10))  # 10m

if not JWT_SECRET:
    # Fail fast rather than silently signing with an empty key.
    logger.warning("JWT_SECRET is not set — token issuance will be rejected until configured.")

_bearer = HTTPBearer(auto_error=True)


def _encode(payload: dict, ttl_seconds: int) -> str:
    if not JWT_SECRET:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Server token signing is not configured.",
        )
    now = datetime.now(timezone.utc)
    body = {
        **payload,
        "iat": now,
        "exp": now + timedelta(seconds=ttl_seconds),
    }
    return jwt.encode(body, JWT_SECRET, algorithm=JWT_ALGORITHM)


def create_access_token(user_id: str, email: str) -> str:
    return _encode({"sub": user_id, "email": email, "scope": "access"}, ACCESS_TOKEN_TTL_SECONDS)


def create_challenge_token(user_id: str, email: str) -> str:
    return _encode({"sub": user_id, "email": email, "scope": "challenge"}, CHALLENGE_TOKEN_TTL_SECONDS)


def decode_token(token: str, expected_scope: str) -> dict:
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if payload.get("scope") != expected_scope:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token is not valid for this operation.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return payload


def get_current_user(
    creds: HTTPAuthorizationCredentials = Depends(_bearer),
) -> dict:
    """FastAPI dependency — resolves the bearer access token to a user identity."""
    payload = decode_token(creds.credentials, expected_scope="access")
    return {"id": payload["sub"], "email": payload.get("email")}
