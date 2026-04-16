"""Secret redaction for event bodies, payloads, facts, and code_refs.

Applied server-side at ingestion — defense in depth. The daemon is
untrusted; we must not persist or broadcast leaked tokens.

Redaction is conservative: false positives (redacting non-secrets)
are preferred over false negatives (leaking real ones).
"""
from __future__ import annotations

import re
from typing import Any

REDACTED = "[REDACTED]"

# Patterns scanned in free text.
# Order matters — more specific patterns first.
_TEXT_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    # Ethereum private key: 0x + exactly 64 hex chars, word-bounded
    ("eth_privkey", re.compile(r"\b0x[a-fA-F0-9]{64}\b")),
    # JWT: three base64url segments separated by dots
    ("jwt", re.compile(r"\beyJ[a-zA-Z0-9_-]{10,}\.[a-zA-Z0-9_-]{10,}\.[a-zA-Z0-9_-]{10,}\b")),
    # GitHub tokens: ghp_, gho_, ghu_, ghs_, ghr_ + 36 chars
    ("github_token", re.compile(r"\bgh[pousr]_[A-Za-z0-9]{36,}\b")),
    # AWS access key ID
    ("aws_access_key", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    # OpenAI-style keys (sk-...)
    ("openai_key", re.compile(r"\bsk-[a-zA-Z0-9_-]{20,}\b")),
    # Bearer tokens — redact the token portion after "Bearer "
    ("bearer", re.compile(r"(?i)(\bBearer\s+)([A-Za-z0-9_.\-~+/]{20,}={0,2})")),
    # password=xxx or password: xxx (common formats)
    ("password_assign", re.compile(r"(?i)(password\s*[:=]\s*)(\S+)")),
    # <KEY>_KEY=xxx (env-var style)
    ("env_secret", re.compile(r"(?i)\b([A-Z][A-Z0-9_]*(?:_KEY|_SECRET|_TOKEN|_PASSWORD))\s*=\s*(\S+)")),
    # AWS secret access key style: 40 char base64-ish (broad)
    ("aws_secret", re.compile(r"(?i)(secret\s*[:=]\s*)([A-Za-z0-9/+=]{40})")),
]

# JSON dict key substrings that imply the value is sensitive.
_SENSITIVE_KEY_SUBSTRINGS = (
    "password",
    "secret",
    "token",
    "api_key",
    "apikey",
    "private_key",
    "privatekey",
    "priv_key",
    "privkey",
    "auth",        # broad but safe — "authorization", "auth_header", etc.
    "credential",
    "passphrase",
)

# Keys that sound sensitive but aren't — don't redact these.
_KEY_ALLOWLIST = (
    "auth_method",      # e.g., "passkey" / "siwe" — not a secret
    "auth_type",
    "authentication_type",
    "token_type",       # e.g., "bearer"
    "public_key",
    "publickey",
)


def _key_is_sensitive(key: str) -> bool:
    low = key.lower()
    if any(a in low for a in _KEY_ALLOWLIST):
        return False
    return any(s in low for s in _SENSITIVE_KEY_SUBSTRINGS)


def redact_text(value: str) -> str:
    """Scan a string for known secret patterns and replace with [REDACTED]."""
    if not value or not isinstance(value, str):
        return value

    out = value
    for name, pattern in _TEXT_PATTERNS:
        if name in ("bearer", "password_assign", "env_secret", "aws_secret"):
            # Patterns with a label group we want to preserve
            out = pattern.sub(lambda m: m.group(1) + REDACTED, out)
        else:
            out = pattern.sub(REDACTED, out)
    return out


def redact_json(obj: Any) -> Any:
    """Recursively redact sensitive values in a dict/list structure.

    Returns a new structure; input is not mutated.

    - If a key name is sensitive, replace its value with [REDACTED]
      (regardless of the value's content).
    - Otherwise, scan string values for secret patterns via redact_text.
    - Lists and nested dicts are walked recursively.
    """
    if isinstance(obj, dict):
        result: dict[str, Any] = {}
        for k, v in obj.items():
            if isinstance(k, str) and _key_is_sensitive(k):
                result[k] = REDACTED
            else:
                result[k] = redact_json(v)
        return result
    if isinstance(obj, list):
        return [redact_json(item) for item in obj]
    if isinstance(obj, str):
        return redact_text(obj)
    # numbers, bool, None — pass through
    return obj
