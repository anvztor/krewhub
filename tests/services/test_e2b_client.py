"""E2bClient HTTP wrapper tests."""
from __future__ import annotations

import httpx
import pytest

from krewhub.services.e2b_client import E2bClient


@pytest.mark.asyncio
async def test_create_sandbox_returns_id_from_sandboxID_field(httpx_mock):
    httpx_mock.add_response(
        method="POST",
        url="http://e2b.local/sandboxes",
        json={"sandboxID": "sbx_123"},
        status_code=201,
    )
    c = E2bClient(base_url="http://e2b.local", api_key="k")
    sandbox_id = await c.create_sandbox(template="base")
    assert sandbox_id == "sbx_123"


@pytest.mark.asyncio
async def test_create_sandbox_falls_back_to_id_field(httpx_mock):
    httpx_mock.add_response(
        method="POST",
        url="http://e2b.local/sandboxes",
        json={"id": "sbx_456"},
        status_code=201,
    )
    c = E2bClient(base_url="http://e2b.local", api_key="k")
    sandbox_id = await c.create_sandbox(template="base")
    assert sandbox_id == "sbx_456"


@pytest.mark.asyncio
async def test_create_sandbox_sends_template_api_key_and_default_timeout(httpx_mock):
    httpx_mock.add_response(
        method="POST",
        url="http://e2b.local/sandboxes",
        json={"sandboxID": "sbx_789"},
        status_code=201,
    )
    c = E2bClient(base_url="http://e2b.local/", api_key="api-key-test")
    await c.create_sandbox(template="base")
    req = httpx_mock.get_request()
    assert req is not None
    assert req.headers["X-API-Key"] == "api-key-test"
    import json as _json
    body = _json.loads(req.content)
    # `timeout` (seconds) carries the sandbox's wall-clock lifetime; the
    # self-hosted e2b orchestrator's default is 15s if omitted, which is
    # too short to outlast a real client cold start (see brain smoke
    # 2026-05-08). Default 300s gives every flow breathing room.
    assert body == {"templateID": "base", "timeout": 300}


@pytest.mark.asyncio
async def test_create_sandbox_honours_custom_timeout_s(httpx_mock):
    httpx_mock.add_response(
        method="POST",
        url="http://e2b.local/sandboxes",
        json={"sandboxID": "sbx_t"},
        status_code=201,
    )
    c = E2bClient(base_url="http://e2b.local/", api_key="k")
    await c.create_sandbox(template="base", timeout_s=900)
    req = httpx_mock.get_request()
    import json as _json
    assert _json.loads(req.content)["timeout"] == 900


@pytest.mark.asyncio
async def test_create_sandbox_rejects_out_of_range_timeout():
    c = E2bClient(base_url="http://e2b.local", api_key="k")
    with pytest.raises(ValueError, match="timeout_s must be in"):
        await c.create_sandbox(template="base", timeout_s=0)
    with pytest.raises(ValueError, match="timeout_s must be in"):
        await c.create_sandbox(template="base", timeout_s=4000)


@pytest.mark.asyncio
async def test_create_sandbox_raises_on_http_error(httpx_mock):
    httpx_mock.add_response(
        method="POST",
        url="http://e2b.local/sandboxes",
        status_code=500,
        text="boom",
    )
    c = E2bClient(base_url="http://e2b.local", api_key="k")
    with pytest.raises(httpx.HTTPStatusError):
        await c.create_sandbox(template="base")


@pytest.mark.asyncio
async def test_terminate_swallows_404(httpx_mock):
    httpx_mock.add_response(
        method="DELETE",
        url="http://e2b.local/sandboxes/sbx_404",
        status_code=404,
    )
    c = E2bClient(base_url="http://e2b.local", api_key="k")
    # Should not raise — already gone is fine.
    await c.terminate("sbx_404")


# ---------------------------------------------------------------------------
# Filesystem RPCs (Phase 2 of docs/superpowers/plans/2026-05-08-sandbox-hand-vocabulary.md)
#
# All four go through the client-proxy on :3002 with the same Host-header
# trick exec_command uses. RPC shapes were verified end-to-end against
# envd 0.5.15 in the Phase 1 recon (infra/e2b/scripts/remote-verify-fs-via-proxy.sh).
# ---------------------------------------------------------------------------


def _client() -> E2bClient:
    """Standard test client. proxy_url derives to http://e2b.local:3002."""
    return E2bClient(
        base_url="http://e2b.local",
        api_key="k",
        envd_proxy_domain="api.cookrew.dev",
    )


# ---- write_file ----------------------------------------------------------


@pytest.mark.asyncio
async def test_write_file_posts_octet_stream_to_files_endpoint(httpx_mock):
    httpx_mock.add_response(method="POST", status_code=200, text="[]")
    c = _client()
    await c.write_file("sbx_xyz", "/tmp/hello.txt", b"hello")
    req = httpx_mock.get_request()
    assert req is not None
    assert req.url.host == "e2b.local"
    assert req.url.port == 3002
    assert req.url.path == "/files"
    # path is URL-encoded by httpx; decode the params dict to verify
    assert req.url.params["path"] == "/tmp/hello.txt"
    assert req.headers["Host"] == "49983-sbx_xyz.api.cookrew.dev"
    assert req.headers["Content-Type"] == "application/octet-stream"
    assert req.content == b"hello"


@pytest.mark.asyncio
async def test_write_file_raises_above_size_cap_without_request(httpx_mock):
    c = _client()
    with pytest.raises(ValueError, match="path_too_large"):
        await c.write_file("sbx_xyz", "/tmp/big", b"x" * (1024 * 1024 + 1))
    # Verify we short-circuited — no HTTP call was made.
    assert httpx_mock.get_request() is None


@pytest.mark.asyncio
async def test_write_file_raises_on_http_error(httpx_mock):
    httpx_mock.add_response(method="POST", status_code=500, text="boom")
    c = _client()
    with pytest.raises(httpx.HTTPStatusError):
        await c.write_file("sbx_xyz", "/tmp/x", b"x")


@pytest.mark.asyncio
async def test_write_file_raises_when_proxy_unconfigured():
    c = E2bClient(base_url="http://e2b.local", api_key="k", proxy_url="")
    with pytest.raises(RuntimeError, match="proxy_not_configured"):
        await c.write_file("sbx_xyz", "/tmp/x", b"x")


# ---- read_file -----------------------------------------------------------


@pytest.mark.asyncio
async def test_read_file_gets_files_endpoint_returns_bytes(httpx_mock):
    httpx_mock.add_response(method="GET", status_code=200, content=b"\x00\x01\x02hi")
    c = _client()
    out = await c.read_file("sbx_xyz", "/tmp/blob.bin")
    assert out == b"\x00\x01\x02hi"
    req = httpx_mock.get_request()
    assert req.url.path == "/files"
    assert req.url.params["path"] == "/tmp/blob.bin"
    assert req.headers["Host"] == "49983-sbx_xyz.api.cookrew.dev"
    # No Content-Type on a GET with no body
    assert "Content-Type" not in req.headers or req.headers.get("Content-Length", "0") == "0"


@pytest.mark.asyncio
async def test_read_file_raises_path_not_found_on_404(httpx_mock):
    httpx_mock.add_response(
        method="GET", status_code=404,
        json={"code": 404, "message": "path '/tmp/nope' does not exist"},
    )
    c = _client()
    with pytest.raises(FileNotFoundError):
        await c.read_file("sbx_xyz", "/tmp/nope")


@pytest.mark.asyncio
async def test_read_file_raises_on_other_http_errors(httpx_mock):
    httpx_mock.add_response(method="GET", status_code=500, text="boom")
    c = _client()
    with pytest.raises(httpx.HTTPStatusError):
        await c.read_file("sbx_xyz", "/tmp/x")


# ---- list_dir -------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_dir_posts_unary_json_returns_entries(httpx_mock):
    sample = {
        "entries": [
            {"name": "a.txt", "type": "FILE_TYPE_FILE", "path": "/tmp/a.txt", "size": "5"},
            {"name": "sub",  "type": "FILE_TYPE_DIRECTORY", "path": "/tmp/sub", "size": "4096"},
        ]
    }
    httpx_mock.add_response(method="POST", status_code=200, json=sample)
    c = _client()
    out = await c.list_dir("sbx_xyz", "/tmp", depth=1)
    assert out == sample["entries"]
    req = httpx_mock.get_request()
    assert req.url.path == "/filesystem.Filesystem/ListDir"
    assert req.headers["Host"] == "49983-sbx_xyz.api.cookrew.dev"
    # Connect-unary uses application/json (no envelope frame).
    assert req.headers["Content-Type"] == "application/json"
    import json as _json
    body = _json.loads(req.content)
    assert body == {"path": "/tmp", "depth": 1}


@pytest.mark.asyncio
async def test_list_dir_default_depth_is_1(httpx_mock):
    httpx_mock.add_response(method="POST", status_code=200, json={"entries": []})
    c = _client()
    await c.list_dir("sbx_xyz", "/tmp")
    req = httpx_mock.get_request()
    import json as _json
    body = _json.loads(req.content)
    assert body["depth"] == 1


@pytest.mark.asyncio
async def test_list_dir_raises_path_not_found_on_404(httpx_mock):
    httpx_mock.add_response(
        method="POST", status_code=404,
        json={"code": 404, "message": "path '/nope' does not exist"},
    )
    c = _client()
    with pytest.raises(FileNotFoundError):
        await c.list_dir("sbx_xyz", "/nope")


# ---- stat -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_stat_posts_unary_json_returns_entry(httpx_mock):
    entry = {
        "name": "tmp", "type": "FILE_TYPE_DIRECTORY", "path": "/tmp",
        "size": "4096", "mode": 511,
    }
    httpx_mock.add_response(method="POST", status_code=200, json={"entry": entry})
    c = _client()
    out = await c.stat("sbx_xyz", "/tmp")
    assert out == entry
    req = httpx_mock.get_request()
    assert req.url.path == "/filesystem.Filesystem/Stat"
    assert req.headers["Content-Type"] == "application/json"
    import json as _json
    assert _json.loads(req.content) == {"path": "/tmp"}


@pytest.mark.asyncio
async def test_stat_raises_path_not_found_on_404(httpx_mock):
    httpx_mock.add_response(
        method="POST", status_code=404,
        json={"code": 404, "message": "path does not exist"},
    )
    c = _client()
    with pytest.raises(FileNotFoundError):
        await c.stat("sbx_xyz", "/nope")


# ---- kill_process (best-effort) ------------------------------------------


@pytest.mark.asyncio
async def test_kill_process_posts_send_signal(httpx_mock):
    httpx_mock.add_response(method="POST", status_code=200, json={})
    c = _client()
    await c.kill_process("sbx_xyz")
    req = httpx_mock.get_request()
    assert req is not None
    assert req.url.path == "/process.Process/SendSignal"
    assert req.headers["Host"] == "49983-sbx_xyz.api.cookrew.dev"
    assert req.headers["Content-Type"] == "application/json"


@pytest.mark.asyncio
async def test_kill_process_swallows_errors(httpx_mock):
    httpx_mock.add_response(method="POST", status_code=500, text="boom")
    c = _client()
    # Best-effort: must not raise even when envd refuses.
    await c.kill_process("sbx_xyz")


@pytest.mark.asyncio
async def test_kill_process_swallows_when_proxy_unconfigured():
    c = E2bClient(base_url="http://e2b.local", api_key="k", proxy_url="")
    # Best-effort: must not raise even with no proxy.
    await c.kill_process("sbx_xyz")
