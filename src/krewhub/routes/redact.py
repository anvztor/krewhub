"""Secret redaction for hook payloads.

Single chokepoint that runs before any hook payload is persisted.
Patterns inspired by the multica regex set: API keys, tokens,
private keys, passwords, AWS creds, .env-style assignments.
"""

from __future__ import annotations

import re
from typing import Any

REDACTED = "[REDACTED]"

_PATTERNS: list[re.Pattern[str]] = [
    # Bearer / Authorization headers
    re.compile(r"(?i)(authorization\s*[:=]\s*)(bearer\s+)?[A-Za-z0-9._\-]{16,}"),
    # Generic api/secret/token assignments: KEY=value, "key": "value"
    re.compile(
        r"(?i)((?:api[_-]?key|secret|token|password|passwd|pwd|access[_-]?key|"
        r"private[_-]?key|client[_-]?secret)[\"']?\s*[:=]\s*[\"']?)"
        r"([A-Za-z0-9._\-+/=]{8,})"
    ),
    # Anthropic / OpenAI / GitHub / Slack style prefixed tokens
    re.compile(r"\bsk-(?:ant-)?[A-Za-z0-9_\-]{20,}\b"),
    re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}\b"),
    re.compile(r"\bxox[aboprs]-[A-Za-z0-9\-]{10,}\b"),
    # AWS access key id
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    # PEM private keys
    re.compile(
        r"-----BEGIN (?:RSA |EC |DSA |OPENSSH |PGP )?PRIVATE KEY-----"
        r"[\s\S]*?-----END (?:RSA |EC |DSA |OPENSSH |PGP )?PRIVATE KEY-----"
    ),
    # JWT
    re.compile(r"\beyJ[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\b"),
]


def redact_text(value: str) -> str:
    if not value:
        return value
    out = value
    for pat in _PATTERNS:
        out = pat.sub(_replace, out)
    return out


def _replace(match: re.Match[str]) -> str:
    # If a capture group exists for the prefix (e.g. KEY=), keep it.
    if match.lastindex and match.lastindex >= 1:
        prefix = match.group(1) or ""
        return f"{prefix}{REDACTED}"
    return REDACTED


def redact_value(value: Any) -> Any:
    """Recursively redact strings inside dicts/lists."""
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, dict):
        return {k: redact_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [redact_value(v) for v in value]
    return value
