import uuid
from sqlalchemy import Column, String, Boolean, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func

Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String(255), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    two_factor_methods = relationship(
        "TwoFactorMethod", back_populates="user", cascade="all, delete-orphan"
    )
    two_factor_codes = relationship(
        "TwoFactorCode", back_populates="user", cascade="all, delete-orphan"
    )
    license_keys = relationship(
        "LicenseKey", back_populates="user", cascade="all, delete-orphan"
    )


class TwoFactorMethod(Base):
    __tablename__ = "two_factor_methods"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    method_type = Column(String(10), nullable=False)  # 'email' or 'sms'
    contact = Column(String(255), nullable=False)
    is_verified = Column(Boolean, default=False, nullable=False)
    is_enabled = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    user = relationship("User", back_populates="two_factor_methods")


class TwoFactorCode(Base):
    __tablename__ = "two_factor_codes"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    code_hash = Column(String(255), nullable=False)
    method_type = Column(String(10), nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    used = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    user = relationship("User", back_populates="two_factor_codes")


class LicenseKey(Base):
    __tablename__ = "license_keys"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    key = Column(String(255), unique=True, nullable=False, index=True)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    expires_at = Column(DateTime(timezone=True), nullable=True)

    user = relationship("User", back_populates="license_keys")
