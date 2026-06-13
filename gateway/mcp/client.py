"""
Async MCP SSE client — talks to the R MCP Server (posit-dev/mcp).

MCP over SSE transport:
  1. GET /sse            → persistent SSE stream; server sends `endpoint` event
                           with the POST URL (e.g. /messages?sessionId=xxx)
  2. POST <endpoint_url> → JSON-RPC 2.0 request body
  3. Response arrives as `message` event on the SSE stream

We wrap this in a simple async context manager that keeps one SSE connection
open per gateway instance and multiplexes tool calls over it.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any
from contextlib import asynccontextmanager

import httpx

log = logging.getLogger("sdtm.mcp_client")


class MCPError(Exception):
    def __init__(self, code: int, message: str, data: Any = None):
        super().__init__(message)
        self.code = code
        self.data = data


class MCPClient:
    """Single-connection async MCP SSE client."""

    def __init__(self, base_url: str = "http://localhost:8001"):
        self.base_url = base_url.rstrip("/")
        self._http = httpx.AsyncClient(base_url=self.base_url, timeout=60.0)
        self._post_url: str | None = None          # set after SSE handshake
        self._pending: dict[int, asyncio.Future] = {}
        self._req_id = 0
        self._sse_task: asyncio.Task | None = None

    async def start(self) -> None:
        """Connect to SSE endpoint and start background reader."""
        ready = asyncio.Event()
        self._sse_task = asyncio.create_task(self._sse_reader(ready))
        await asyncio.wait_for(ready.wait(), timeout=10.0)
        log.info("MCP SSE connected; posting to %s", self._post_url)

    async def stop(self) -> None:
        if self._sse_task:
            self._sse_task.cancel()
            try:
                await self._sse_task
            except asyncio.CancelledError:
                pass
        await self._http.aclose()

    async def _sse_reader(self, ready: asyncio.Event) -> None:
        """Read SSE stream forever, resolving pending futures on `message` events."""
        async with self._http.stream("GET", "/sse") as resp:
            resp.raise_for_status()
            event_type = ""
            async for line in resp.aiter_lines():
                if line.startswith("event:"):
                    event_type = line.split(":", 1)[1].strip()
                elif line.startswith("data:"):
                    data_raw = line.split(":", 1)[1].strip()
                    if event_type == "endpoint":
                        self._post_url = data_raw
                        ready.set()
                    elif event_type == "message":
                        try:
                            payload = json.loads(data_raw)
                            req_id = payload.get("id")
                            fut = self._pending.pop(req_id, None)
                            if fut and not fut.done():
                                if "error" in payload:
                                    e = payload["error"]
                                    fut.set_exception(MCPError(e.get("code", -1), e.get("message", "")))
                                else:
                                    fut.set_result(payload.get("result"))
                        except Exception as exc:
                            log.warning("Unparseable MCP message: %s — %s", data_raw, exc)
                    event_type = ""

    async def call_tool(self, name: str, arguments: dict[str, Any] | None = None) -> Any:
        """Send a tools/call JSON-RPC request and await the result."""
        if not self._post_url:
            raise RuntimeError("MCP client not started")

        self._req_id += 1
        req_id = self._req_id
        body = {
            "jsonrpc": "2.0",
            "id": req_id,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments or {}},
        }
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending[req_id] = fut

        resp = await self._http.post(self._post_url, json=body)
        resp.raise_for_status()

        try:
            return await asyncio.wait_for(fut, timeout=55.0)
        except asyncio.TimeoutError:
            self._pending.pop(req_id, None)
            raise MCPError(-32001, f"Tool '{name}' timed out")

    # ── Convenience health check (plain HTTP, no SSE needed) ─────────────────
    async def ping(self) -> bool:
        try:
            resp = await self._http.get("/health", timeout=5.0)
            return resp.status_code == 200
        except Exception:
            return False


# Module-level singleton; created in FastAPI lifespan
_client: MCPClient | None = None


def get_client() -> MCPClient:
    if _client is None:
        raise RuntimeError("MCP client not initialised")
    return _client


@asynccontextmanager
async def lifespan_mcp(r_mcp_url: str):
    global _client
    _client = MCPClient(r_mcp_url)
    await _client.start()
    try:
        yield _client
    finally:
        await _client.stop()
        _client = None
