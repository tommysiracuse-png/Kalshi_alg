"""FastAPI REST API for the authentication service.

Exposes account creation, login (with optional email/SMS 2FA), 2FA management
and license-key retrieval over HTTP/JSON so a separate application can consume
it. All business logic is reused unchanged from the existing ``auth`` package.

Run locally:  uvicorn api.app:app --host 0.0.0.0 --port 8000
"""

import os
import sys
import logging

# Make the sibling packages (auth, database, utils) importable, mirroring main.py.
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stderr,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from database.connection import init_db, get_db
from auth.authentication import (
    login_user,
    register_user,
    get_user_license_keys,
)
from auth.two_factor import (
    send_login_challenge,
    verify_2fa_code,
    get_user_2fa_methods,
    initiate_2fa_setup,
    confirm_2fa_setup,
    disable_2fa_method,
)
from utils.validators import is_valid_phone, mask_contact

from api.schemas import (
    RegisterRequest,
    RegisterResponse,
    LoginRequest,
    TokenResponse,
    ChallengeResponse,
    TwoFactorVerifyRequest,
    TwoFactorSetupRequest,
    TwoFactorConfirmRequest,
    TwoFactorMethodResponse,
    LicenseKeyResponse,
    UserResponse,
    MessageResponse,
    HealthResponse,
)
from api.security import (
    create_access_token,
    create_challenge_token,
    decode_token,
    get_current_user,
    ACCESS_TOKEN_TTL_SECONDS,
)
from api.monitoring import MonitoringMiddleware, metrics_endpoint

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Auth Service API",
    description="REST authentication API: account creation, login, 2FA, and license keys.",
    version="1.0.0",
)

# CORS — the consuming application origins. Configurable via env (comma-separated).
_origins = [o.strip() for o in os.environ.get("CORS_ALLOW_ORIGINS", "*").split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(MonitoringMiddleware)


@app.on_event("startup")
def _startup() -> None:
    init_db()
    logger.info("Auth Service API started.")


# ── Local-dev browser test console ────────────────────────────────────────────
# Served only when ENABLE_TEST_UI is set (docker-compose sets it for local dev).
# It is never enabled in the AWS deployment, so it cannot be reached in prod.
if os.environ.get("ENABLE_TEST_UI", "").lower() in ("1", "true", "yes"):
    from fastapi.responses import FileResponse

    _test_ui_path = os.path.join(os.path.dirname(__file__), "static", "test.html")

    @app.get("/test", include_in_schema=False)
    def test_console():
        return FileResponse(_test_ui_path)

    logger.warning("Test UI enabled at /test — do NOT enable this in production.")


# ── Health & metrics ──────────────────────────────────────────────────────────
@app.get("/health", response_model=HealthResponse, tags=["monitoring"])
def health() -> HealthResponse:
    """Liveness + database connectivity check (used by the ALB health check)."""
    db_status = "ok"
    try:
        with get_db() as db:
            db.execute(text("SELECT 1"))
    except Exception as exc:  # noqa: BLE001 — health must never raise
        logger.error("Health check DB failure: %s", exc)
        db_status = "unavailable"
    return HealthResponse(status="ok", database=db_status)


@app.get("/metrics", tags=["monitoring"])
def metrics():
    """Prometheus metrics exposition."""
    return metrics_endpoint()


# ── Authentication ────────────────────────────────────────────────────────────
@app.post(
    "/auth/register",
    response_model=RegisterResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["auth"],
)
def register(body: RegisterRequest) -> RegisterResponse:
    ok, result = register_user(body.email, body.password)
    if not ok:
        # result is a human-readable reason (e.g. weak password / already exists)
        code = (
            status.HTTP_409_CONFLICT
            if "already exists" in result
            else status.HTTP_400_BAD_REQUEST
        )
        raise HTTPException(status_code=code, detail=result)
    return RegisterResponse(id=result, email=body.email.strip().lower())


@app.post(
    "/auth/login",
    response_model=TokenResponse | ChallengeResponse,
    tags=["auth"],
)
def login(body: LoginRequest):
    ok, msg, user = login_user(body.email, body.password)
    if not ok or not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=msg)

    if not user["has_2fa"]:
        # No 2FA configured — issue an access token directly.
        token = create_access_token(user["id"], user["email"])
        return TokenResponse(access_token=token, expires_in=ACCESS_TOKEN_TTL_SECONDS)

    # 2FA required: send a code to the first active method and return a challenge.
    method = user["2fa_methods"][0]
    sent_ok, sent_msg = send_login_challenge(user["id"], method)
    if not sent_ok:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Could not send 2FA code: {sent_msg}",
        )
    return ChallengeResponse(
        challenge_token=create_challenge_token(user["id"], user["email"]),
        method_type=method["type"],
        contact_masked=mask_contact(method["contact"], method["type"]),
    )


@app.post("/auth/2fa/verify", response_model=TokenResponse, tags=["auth"])
def verify_login_2fa(body: TwoFactorVerifyRequest) -> TokenResponse:
    payload = decode_token(body.challenge_token, expected_scope="challenge")
    user_id, email = payload["sub"], payload.get("email")

    ok, msg = verify_2fa_code(user_id, body.code)
    if not ok:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=msg)

    token = create_access_token(user_id, email)
    return TokenResponse(access_token=token, expires_in=ACCESS_TOKEN_TTL_SECONDS)


# ── Authenticated: identity & license keys ────────────────────────────────────
@app.get("/auth/me", response_model=UserResponse, tags=["auth"])
def me(user: dict = Depends(get_current_user)) -> UserResponse:
    methods = get_user_2fa_methods(user["id"])
    has_2fa = any(m["verified"] and m["enabled"] for m in methods)
    return UserResponse(id=user["id"], email=user["email"], has_2fa=has_2fa)


@app.get("/auth/license-keys", response_model=list[LicenseKeyResponse], tags=["license"])
def license_keys(user: dict = Depends(get_current_user)) -> list[LicenseKeyResponse]:
    # Mirror the UI rule: a verified+enabled 2FA method is required to view keys.
    methods = get_user_2fa_methods(user["id"])
    if not any(m["verified"] and m["enabled"] for m in methods):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="A verified 2FA method is required to access license keys.",
        )
    keys = get_user_license_keys(user["id"])
    return [
        LicenseKeyResponse(
            id=k["id"],
            key=k["key"],
            expires_at=k["expires_at"].isoformat() if k["expires_at"] else None,
        )
        for k in keys
    ]


# ── Authenticated: 2FA management ─────────────────────────────────────────────
@app.get("/auth/2fa/methods", response_model=list[TwoFactorMethodResponse], tags=["2fa"])
def list_2fa_methods(user: dict = Depends(get_current_user)) -> list[TwoFactorMethodResponse]:
    methods = get_user_2fa_methods(user["id"])
    return [
        TwoFactorMethodResponse(
            type=m["type"],
            contact_masked=mask_contact(m["contact"], m["type"]),
            verified=m["verified"],
            enabled=m["enabled"],
        )
        for m in methods
    ]


@app.post("/auth/2fa/setup", response_model=MessageResponse, tags=["2fa"])
def setup_2fa(
    body: TwoFactorSetupRequest, user: dict = Depends(get_current_user)
) -> MessageResponse:
    contact = body.contact.strip()
    if body.method_type == "sms":
        valid, normalized = is_valid_phone(contact)
        if not valid:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=normalized)
        contact = normalized
    else:
        contact = contact.lower()

    ok, msg = initiate_2fa_setup(user["id"], body.method_type, contact)
    if not ok:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=msg)
    return MessageResponse(message=msg)


@app.post("/auth/2fa/confirm", response_model=MessageResponse, tags=["2fa"])
def confirm_2fa(
    body: TwoFactorConfirmRequest, user: dict = Depends(get_current_user)
) -> MessageResponse:
    ok, msg = confirm_2fa_setup(user["id"], body.method_type, body.code)
    if not ok:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=msg)
    return MessageResponse(message=msg)


@app.delete("/auth/2fa/{method_type}", response_model=MessageResponse, tags=["2fa"])
def disable_2fa(method_type: str, user: dict = Depends(get_current_user)) -> MessageResponse:
    if method_type not in ("email", "sms"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid method type.")
    ok, msg = disable_2fa_method(user["id"], method_type)
    if not ok:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=msg)
    return MessageResponse(message=msg)
