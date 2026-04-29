from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    host: str = "127.0.0.1"
    port: int = 8420
    database_path: str = str(Path.home() / ".krewhub" / "krewhub.db")
    api_key: str = "dev-api-key"

    # krewauth JWKS for JWT verification (ES256)
    jwks_url: str = "http://127.0.0.1:8421/.well-known/jwks.json"

    # Legacy HS256 secret (transitional — remove after migration)
    jwt_secret: str = ""

    retention_days: int = 7
    heartbeat_timeout_seconds: int = 30

    # ERC-8004 on GOAT Testnet3
    erc8004_chain_id: int = 48816
    erc8004_rpc_url: str = "https://rpc.testnet3.goat.network"
    erc8004_identity_registry: str = "0x556089008Fc0a60cD09390Eca93477ca254A5522"
    erc8004_reputation_registry: str = "0xd9140951d8aE6E5F625a02F5908535e16e3af964"

    # krewauth service URL (for agent wallet operations)
    krewauth_base_url: str = "http://127.0.0.1:8421"

    # ERC-4337 AA wallet session key config
    krewhub_session_pubkey: str = ""
    aa_allowed_tokens: str = ""
    aa_session_key_valid_hours: int = 720
    aa_session_key_spend_limit: str = "1000000000000000000000"

    # BFF elimination: cookie auth + proxy
    krew_auth_url: str = "http://127.0.0.1:8421"
    app_origin: str = "http://localhost:3000"
    auth_redirect_uri: str = "http://localhost:3000/auth/callback"
    cookie_secure: bool = False
    cookie_domain: str = ""

    # Track A1: krewauth relay (canonical names)
    krewauth_url: str = "http://localhost:8421"
    krewauth_service_token: str = ""

    model_config = {"env_prefix": "KREWHUB_"}


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
