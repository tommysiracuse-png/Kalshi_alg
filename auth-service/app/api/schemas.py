"""Pydantic request/response models for the REST API."""

from pydantic import BaseModel, Field


# ── Requests ──────────────────────────────────────────────────────────────────
class RegisterRequest(BaseModel):
    email: str = Field(..., examples=["you@example.com"])
    password: str = Field(..., examples=["S3cur3!pass"])


class LoginRequest(BaseModel):
    email: str = Field(..., examples=["you@example.com"])
    password: str = Field(..., examples=["S3cur3!pass"])


class TwoFactorVerifyRequest(BaseModel):
    challenge_token: str = Field(..., description="Token returned by /auth/login when 2FA is required")
    code: str = Field(..., min_length=4, max_length=8, examples=["123456"])


class TwoFactorSetupRequest(BaseModel):
    method_type: str = Field(..., pattern="^(email|sms)$", examples=["email"])
    contact: str = Field(..., description="Email address or US phone number")


class TwoFactorConfirmRequest(BaseModel):
    method_type: str = Field(..., pattern="^(email|sms)$")
    code: str = Field(..., min_length=4, max_length=8)


# ── Responses ─────────────────────────────────────────────────────────────────
class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int = Field(..., description="Access token lifetime in seconds")


class ChallengeResponse(BaseModel):
    challenge_required: bool = True
    challenge_token: str = Field(..., description="Short-lived token to submit with the 2FA code")
    method_type: str
    contact_masked: str


class RegisterResponse(BaseModel):
    id: str
    email: str


class UserResponse(BaseModel):
    id: str
    email: str
    has_2fa: bool


class MessageResponse(BaseModel):
    message: str


class TwoFactorMethodResponse(BaseModel):
    type: str
    contact_masked: str
    verified: bool
    enabled: bool


class LicenseKeyResponse(BaseModel):
    id: str
    key: str
    expires_at: str | None = None


class HealthResponse(BaseModel):
    status: str
    database: str
