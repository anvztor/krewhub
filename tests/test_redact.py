"""Tests for secret redaction service."""
from __future__ import annotations

import pytest

from krewhub.services.redact import REDACTED, redact_json, redact_text


class TestRedactText:
    def test_plain_text_unchanged(self):
        assert redact_text("hello world") == "hello world"
        assert redact_text("") == ""

    def test_ethereum_private_key_redacted(self):
        # 0x + 64 hex chars = 66 total
        key = "0x" + "a" * 64
        result = redact_text(f"Use key {key} to sign")
        assert key not in result
        assert REDACTED in result
        assert "Use key" in result

    def test_ethereum_address_preserved(self):
        # 0x + 40 hex chars (address) should NOT be redacted
        addr = "0x" + "a" * 40
        result = redact_text(f"Transfer to {addr}")
        assert addr in result

    def test_aws_access_key_redacted(self):
        result = redact_text("AWS_KEY=AKIAIOSFODNN7EXAMPLE rest of line")
        assert "AKIAIOSFODNN7EXAMPLE" not in result
        assert REDACTED in result

    def test_aws_secret_key_redacted(self):
        secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        result = redact_text(f"secret={secret}")
        assert secret not in result

    def test_github_token_redacted(self):
        # ghp_, gho_, ghu_, ghs_, ghr_ prefixes
        for prefix in ("ghp_", "gho_", "ghu_", "ghs_", "ghr_"):
            token = prefix + "a" * 36
            result = redact_text(f"token: {token}")
            assert token not in result, f"{prefix} not redacted"

    def test_bearer_token_redacted(self):
        result = redact_text("Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.abc.def")
        assert "eyJhbGci" not in result
        assert "Bearer" in result  # keyword preserved

    def test_jwt_redacted(self):
        jwt = "eyJhbGciOiJIUzI1NiJ9." + "a" * 40 + "." + "b" * 40
        result = redact_text(f"token is {jwt}")
        assert jwt not in result

    def test_api_key_env_pattern_redacted(self):
        result = redact_text("OPENAI_API_KEY=sk-proj-abcdef1234567890abcdef1234567890abcdef1234")
        assert "sk-proj-abcdef" not in result

    def test_password_equals_pattern_redacted(self):
        result = redact_text("password=MySecretP@ss123!")
        assert "MySecretP@ss123!" not in result

    def test_multiple_secrets_in_one_string(self):
        text = "key1=ghp_" + "a" * 36 + " and key2=0x" + "b" * 64
        result = redact_text(text)
        assert "ghp_" + "a" * 36 not in result
        assert "0x" + "b" * 64 not in result
        # Both should be replaced
        assert result.count(REDACTED) >= 2


class TestRedactJson:
    def test_empty_dict(self):
        assert redact_json({}) == {}

    def test_dict_with_plain_values_unchanged(self):
        obj = {"name": "alice", "count": 42}
        assert redact_json(obj) == obj

    def test_key_named_password_redacted(self):
        obj = {"username": "bob", "password": "secret123"}
        result = redact_json(obj)
        assert result["username"] == "bob"
        assert result["password"] == REDACTED

    def test_key_containing_token_redacted(self):
        obj = {"api_token": "abc", "session_token": "xyz", "auth_token": "123"}
        result = redact_json(obj)
        for k in ["api_token", "session_token", "auth_token"]:
            assert result[k] == REDACTED

    def test_key_containing_secret_redacted(self):
        obj = {"client_secret": "shh", "jwt_secret": "hmm"}
        result = redact_json(obj)
        assert all(v == REDACTED for v in result.values())

    def test_private_key_field_redacted(self):
        obj = {"private_key": "0xABC", "privateKey": "0xDEF", "priv_key": "0x123"}
        result = redact_json(obj)
        assert all(v == REDACTED for v in result.values())

    def test_nested_dict_redacted(self):
        obj = {"config": {"api_key": "leaked"}, "name": "ok"}
        result = redact_json(obj)
        assert result["name"] == "ok"
        assert result["config"]["api_key"] == REDACTED

    def test_list_of_dicts_redacted(self):
        obj = {"headers": [{"name": "Authorization", "value": "Bearer xyz"}]}
        result = redact_json(obj)
        assert result["headers"][0]["name"] == "Authorization"
        # value field should be checked but "value" alone isn't sensitive;
        # however the string "Bearer xyz" inside it should be scanned
        # (current design: we redact by KEY name, not value content for json)

    def test_preserves_non_sensitive_hex(self):
        # Ethereum addresses (40-char hex) should stay
        obj = {"wallet_address": "0x" + "a" * 40}
        result = redact_json(obj)
        assert result["wallet_address"] == "0x" + "a" * 40

    def test_redact_string_values_with_patterns(self):
        # When a value LOOKS like a secret (even if key doesn't hint), redact
        obj = {"note": "my github token is ghp_" + "a" * 36}
        result = redact_json(obj)
        assert "ghp_" + "a" * 36 not in result["note"]

    def test_deeply_nested(self):
        obj = {
            "level1": {
                "level2": {
                    "level3": {
                        "api_key": "deep_secret",
                    }
                }
            }
        }
        result = redact_json(obj)
        assert result["level1"]["level2"]["level3"]["api_key"] == REDACTED

    def test_returns_new_object_not_mutated(self):
        original = {"api_key": "secret"}
        result = redact_json(original)
        assert original["api_key"] == "secret"  # not mutated
        assert result["api_key"] == REDACTED


class TestRedactMarkerIsConsistent:
    def test_marker_value(self):
        assert REDACTED == "[REDACTED]"
