"""E2B orchestrator + envd HTTP client.

Two surfaces:

1. **Orchestrator** (control plane) — `POST <e2b_api_url>/sandboxes`,
   `DELETE /sandboxes/<id>`. Captured in
   `infra/e2b/scripts/remote-api-create-base-sandbox.sh`.

2. **envd** (data plane) — exec, files. Each sandbox runs envd 0.5.15
   internally. envd is reached through the e2b client-proxy on port
   3002 of the orchestrator host, with a Host-header routing pattern
   `<port>-<sandboxId>.<envd_proxy_domain>` (port 49983 = envd default).
   envd speaks the Connect protocol (https://connectrpc.com); the
   exec RPC is `/process.Process/Start` (server-streaming).
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import struct
from typing import AsyncIterator

import httpx

logger = logging.getLogger(__name__)


# envd's default port inside the sandbox VM.
_ENVD_PORT = 49983


class E2bClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        timeout: float = 5.0,
        proxy_url: str | None = None,
        envd_proxy_domain: str = "api.cookrew.dev",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        # Default proxy to same host as orchestrator with port 3002.
        # The proxy routes by Host header (does not actually resolve
        # the domain).
        if proxy_url is None:
            try:
                from urllib.parse import urlparse
                u = urlparse(self.base_url)
                proxy_url = f"{u.scheme}://{u.hostname}:3002"
            except Exception:
                proxy_url = ""
        self.proxy_url = (proxy_url or "").rstrip("/")
        self.envd_proxy_domain = envd_proxy_domain

    def _headers(self) -> dict[str, str]:
        return {
            "X-API-Key": self.api_key,
            "Content-Type": "application/json",
        }

    async def create_sandbox(self, *, template: str) -> str:
        url = f"{self.base_url}/sandboxes"
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                url,
                headers=self._headers(),
                json={"templateID": template},
            )
            response.raise_for_status()
            body = response.json()
        sandbox_id = body.get("sandboxID") or body.get("id")
        if not sandbox_id:
            raise ValueError(
                f"e2b create_sandbox response missing sandboxID/id: {body!r}"
            )
        return sandbox_id

    async def terminate(self, sandbox_id: str) -> None:
        url = f"{self.base_url}/sandboxes/{sandbox_id}"
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.delete(url, headers=self._headers())
        if response.status_code in (200, 204, 404):
            return
        response.raise_for_status()

    async def exec_command(
        self,
        sandbox_id: str,
        command: str,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout: float = 300.0,
    ) -> AsyncIterator[dict]:
        """Run `command` in the sandbox via envd's `process.Process/Start`.

        Returns an async generator yielding chunks:
          - {"stream": "stdout"|"stderr", "data": "..."}  — output chunk
          - {"exit_code": int}                            — terminal exit
          - {"error": "..."}                              — terminal infra failure

        The caller MUST `async for chunk in (await client.exec_command(...))`.
        """
        if not self.proxy_url:
            async def _err():
                yield {"error": "e2b client-proxy not configured"}
            return _err()

        # Connect-streaming envelope:
        #   - 1 byte flags (0x00 normal)
        #   - 4 bytes big-endian uint32 message length
        #   - N bytes JSON message
        # Final response envelope has flags=0x02 (end-of-stream) with
        # error JSON or {} on success.
        process_config: dict = {
            "cmd": "/bin/sh",
            "args": ["-c", command],
        }
        if cwd is not None:
            process_config["cwd"] = cwd
        if env is not None:
            process_config["envs"] = env

        request_body = json.dumps({"process": process_config}).encode("utf-8")
        envelope = struct.pack(">BI", 0, len(request_body)) + request_body

        url = f"{self.proxy_url}/process.Process/Start"
        host_header = f"{_ENVD_PORT}-{sandbox_id}.{self.envd_proxy_domain}"
        headers = {
            "Host": host_header,
            "Content-Type": "application/connect+json",
            "Connect-Protocol-Version": "1",
        }

        async def _stream() -> AsyncIterator[dict]:
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    async with client.stream(
                        "POST", url, headers=headers, content=envelope,
                    ) as resp:
                        if resp.status_code != 200:
                            text = await resp.aread()
                            yield {
                                "error": (
                                    f"envd Start returned {resp.status_code}: "
                                    f"{text.decode(errors='replace')[:500]}"
                                ),
                            }
                            return
                        async for chunk in _decode_connect_stream(resp.aiter_bytes()):
                            yield chunk
            except (httpx.RequestError, asyncio.TimeoutError) as exc:
                yield {"error": f"envd request failed: {exc}"}

        return _stream()


# ---------------------------------------------------------------------------
# Connect-streaming response decoder
# ---------------------------------------------------------------------------


async def _decode_connect_stream(
    byte_iter: AsyncIterator[bytes],
) -> AsyncIterator[dict]:
    """Parse a Connect server-streaming response and yield exec chunks.

    Each envelope: 5-byte header (flags, big-endian length) + JSON body.
    Body shapes (from envd's process.proto StartResponse):
      {"event":{"start":{"pid":N}}}
      {"event":{"data":{"stdout"|"stderr": "<base64>"}}}
      {"event":{"end":{"exit_code":N, "exited":bool, "status":"..."}}}
      {"event":{"keepalive":{}}}
    Final envelope (flags=0x02): empty `{}` on success or `{"error":...}`.

    Yields our normalized chunk shape:
      {"stream":"stdout","data":"..."} / {"stream":"stderr","data":"..."}
      {"exit_code": int}
      {"error": "..."}
    """
    buf = bytearray()
    saw_exit = False
    async for piece in byte_iter:
        buf.extend(piece)
        while True:
            if len(buf) < 5:
                break
            flags = buf[0]
            (size,) = struct.unpack(">I", bytes(buf[1:5]))
            if len(buf) < 5 + size:
                break
            body = bytes(buf[5:5 + size])
            del buf[:5 + size]
            try:
                msg = json.loads(body.decode("utf-8")) if body else {}
            except json.JSONDecodeError:
                continue
            if flags & 0x02:
                # End-of-stream envelope. Body may carry an error.
                err = (msg or {}).get("error")
                if err:
                    yield {"error": _connect_error_string(err)}
                if not saw_exit:
                    # No `end` event arrived — synthesize an exit_code
                    # so the SandboxHand caller doesn't return
                    # no_exit_code.
                    yield {"exit_code": 0}
                return
            event = (msg or {}).get("event") or {}
            if "data" in event:
                data = event["data"] or {}
                if "stdout" in data:
                    yield {
                        "stream": "stdout",
                        "data": _b64_decode(data["stdout"]),
                    }
                elif "stderr" in data:
                    yield {
                        "stream": "stderr",
                        "data": _b64_decode(data["stderr"]),
                    }
            elif "end" in event:
                end = event["end"] or {}
                code = end.get("exit_code") or 0
                if not isinstance(code, int):
                    try:
                        code = int(code)
                    except (TypeError, ValueError):
                        code = 0
                saw_exit = True
                yield {"exit_code": code}
            elif "start" in event:
                # PID — ignore, we don't surface it upstream.
                continue
            elif "keepalive" in event:
                continue


def _b64_decode(s: str) -> str:
    try:
        return base64.b64decode(s).decode("utf-8", errors="replace")
    except Exception:
        return ""


def _connect_error_string(err: dict | str) -> str:
    if isinstance(err, str):
        return err
    code = err.get("code", "unknown")
    message = err.get("message", "")
    return f"{code}: {message}"
