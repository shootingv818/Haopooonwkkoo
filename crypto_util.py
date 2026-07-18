"""
Small symmetric-encryption helper for secrets stored at rest (worker SSH
passwords and API tokens).

Uses Fernet (AES-128-CBC + HMAC) from the `cryptography` package. The key
comes from config.WORKER_SECRET (set in .env). If no key is configured we fail
loudly instead of silently storing plaintext, so secrets are never leaked by
accident.

Generate a key once:
    python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
and put it in .env as WORKER_SECRET=...
"""
from __future__ import annotations

import base64
import hashlib

import config


class SecretError(RuntimeError):
    pass


def _fernet():
    try:
        from cryptography.fernet import Fernet
    except ImportError as e:  # pragma: no cover
        raise SecretError(
            "بسته‌ی cryptography نصب نیست. `pip install cryptography` را اجرا کن."
        ) from e

    key = config.WORKER_SECRET
    if not key:
        raise SecretError(
            "WORKER_SECRET در .env تنظیم نشده. یک کلید بساز:\n"
            'python -c "from cryptography.fernet import Fernet; '
            'print(Fernet.generate_key().decode())"'
        )

    # Accept either a proper 32-byte urlsafe-base64 Fernet key, or any
    # passphrase (which we deterministically stretch into a valid key).
    raw = key.encode()
    try:
        if len(base64.urlsafe_b64decode(raw)) == 32:
            return Fernet(raw)
    except Exception:
        pass
    derived = base64.urlsafe_b64encode(hashlib.sha256(raw).digest())
    return Fernet(derived)


def encrypt(plaintext: str) -> str:
    """Encrypt a string -> urlsafe token string (safe to store in SQLite)."""
    if plaintext is None:
        plaintext = ""
    return _fernet().encrypt(plaintext.encode()).decode()


def decrypt(token: str) -> str:
    """Decrypt a token produced by encrypt(). Returns '' for empty input."""
    if not token:
        return ""
    return _fernet().decrypt(token.encode()).decode()


def is_configured() -> bool:
    """True if a usable WORKER_SECRET is present (no exception on build)."""
    try:
        _fernet()
        return True
    except Exception:
        return False
