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
async def test_create_sandbox_sends_template_and_api_key(httpx_mock):
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
    assert body == {"templateID": "base"}


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
