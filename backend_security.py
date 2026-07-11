"""Authentication primitives for the local ICON MOBILE server.

The module deliberately uses only the Python standard library.  Sessions are
signed, expire on the server, and are carried in an HTTP-only host cookie.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional


COOKIE_NAME = "icon_session"


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _b64decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("ascii"))


def load_or_create_secret(base_dir: Path) -> bytes:
    configured = os.environ.get("SESSION_SECRET", "").strip()
    if configured:
        if len(configured.encode("utf-8")) < 32:
            raise RuntimeError("SESSION_SECRET must contain at least 32 bytes")
        return configured.encode("utf-8")

    path = base_dir / ".erp_session_secret"
    try:
        value = path.read_bytes()
        if len(value) >= 32:
            return value
    except FileNotFoundError:
        pass

    value = secrets.token_bytes(48)
    try:
        descriptor = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        value = path.read_bytes()
    else:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
    if len(value) < 32:
        raise RuntimeError("Stored session secret is invalid")
    return value


@dataclass(frozen=True)
class SessionIdentity:
    role: str
    issued_at: int
    expires_at: int
    session_id: str


class SessionSigner:
    def __init__(self, secret: bytes, lifetime_seconds: int) -> None:
        self._secret = secret
        self.lifetime_seconds = max(300, int(lifetime_seconds))

    def issue(self, role: str) -> tuple[str, SessionIdentity]:
        now = int(time.time())
        identity = SessionIdentity(
            role=role,
            issued_at=now,
            expires_at=now + self.lifetime_seconds,
            session_id=secrets.token_urlsafe(18),
        )
        payload = {
            "v": 1,
            "role": identity.role,
            "iat": identity.issued_at,
            "exp": identity.expires_at,
            "sid": identity.session_id,
        }
        encoded = _b64encode(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))
        signature = _b64encode(hmac.new(self._secret, encoded.encode("ascii"), hashlib.sha256).digest())
        return f"{encoded}.{signature}", identity

    def verify(self, token: Optional[str]) -> Optional[SessionIdentity]:
        if not token or len(token) > 4096 or "." not in token:
            return None
        encoded, signature = token.rsplit(".", 1)
        expected = _b64encode(hmac.new(self._secret, encoded.encode("ascii"), hashlib.sha256).digest())
        if not hmac.compare_digest(signature, expected):
            return None
        try:
            payload: Mapping[str, Any] = json.loads(_b64decode(encoded))
            role = str(payload["role"])
            issued_at = int(payload["iat"])
            expires_at = int(payload["exp"])
            session_id = str(payload["sid"])
        except (ValueError, TypeError, KeyError, json.JSONDecodeError):
            return None
        now = int(time.time())
        if role not in {"pos", "admin", "wholesale"}:
            return None
        if issued_at > now + 60 or expires_at <= now or expires_at - issued_at > self.lifetime_seconds + 60:
            return None
        if not session_id:
            return None
        return SessionIdentity(role, issued_at, expires_at, session_id)


def password_matches(candidate: object, configured: str) -> bool:
    left = str(candidate or "").encode("utf-8")
    right = configured.encode("utf-8")
    return hmac.compare_digest(left, right)
