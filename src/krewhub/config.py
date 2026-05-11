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

    # Track A2: e2b orchestrator (template-based sandbox per task).
    # See infra/e2b/scripts/remote-api-create-base-sandbox.sh for the
    # reference API shape: POST <e2b_api_url>/sandboxes with X-API-Key.
    e2b_api_url: str = "http://10.20.100.214:3000"
    e2b_api_key: str = ""
    e2b_default_template: str = "base"
    # The e2b client-proxy address used to reach envd inside sandbox VMs
    # for `Process.Start` exec calls. Defaults to same host as the
    # orchestrator on port 3002. Set explicitly when deployed elsewhere.
    e2b_client_proxy_url: str = ""
    # Host header pattern used by the proxy to route to a sandbox's
    # envd port. The proxy parses `<port>-<sandbox_id>.<domain>` and
    # forwards to the right firecracker VM; the actual domain doesn't
    # need to resolve in DNS — the proxy uses Host string matching.
    e2b_envd_proxy_domain: str = "api.cookrew.dev"
    sandbox_idle_timeout_seconds: int = 600
    sandbox_max_age_seconds: int = 3600

    # Credential vault (Path B: env-injection MVP). Operator pastes a PAT,
    # we encrypt-at-rest with AES-GCM using a key derived from this
    # passphrase, then inject as env var when SandboxHand dispatches op:exec.
    credentials_encryption_key: str = ""

    # GitHub OAuth App for the "Connect GitHub" credential bootstrap path.
    # Register at https://github.com/settings/developers. Callback URL
    # MUST be `<KREWHUB_PUBLIC_URL>/api/v1/oauth/github/callback`.
    github_oauth_client_id: str = ""
    github_oauth_client_secret: str = ""
    # Externally-reachable krewhub URL used to build the callback URL
    # in the GitHub authorize redirect + the redirect-back-to-web URL.
    public_url: str = "https://hub.cookrew.dev"
    # cookrew-web URL the OAuth callback redirects to after success/failure.
    web_url: str = "https://beta.cookrew.dev"

    # Dev escape hatch: when KREWHUB_KREW_DEV_FAKE_AUTH=1, skip cookie/JWT
    # checks and resolve all callers as `dev-user-1`. Kept post-merge as a
    # documented dev-only switch; OFF in prod.
    krew_dev_fake_auth: bool = False

    model_config = {"env_prefix": "KREWHUB_"}


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
