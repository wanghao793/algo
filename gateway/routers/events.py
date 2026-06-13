"""
SSE push endpoint for the Vue 3 frontend.
Each session gets its own asyncio.Queue; domain status changes are pushed here.
"""

from __future__ import annotations

import asyncio
import json
import logging
from fastapi import APIRouter
from fastapi.responses import StreamingResponse

log = logging.getLogger("sdtm.events")
router = APIRouter(prefix="/api/v2/events", tags=["events"])

# session_id → set of asyncio.Queue (one per connected browser tab)
_queues: dict[str, set[asyncio.Queue]] = {}


def get_queues(session_id: str) -> set[asyncio.Queue]:
    return _queues.setdefault(session_id, set())


async def push_event(session_id: str, event_type: str, data: dict) -> None:
    """Called by other routers to broadcast a domain status change."""
    payload = json.dumps({"type": event_type, **data})
    dead: set[asyncio.Queue] = set()
    for q in get_queues(session_id):
        try:
            q.put_nowait(payload)
        except asyncio.QueueFull:
            dead.add(q)
    _queues[session_id] -= dead


@router.get("/{session_id}")
async def sse_stream(session_id: str):
    q: asyncio.Queue = asyncio.Queue(maxsize=100)
    get_queues(session_id).add(q)

    async def generator():
        try:
            # Send a connected handshake immediately
            yield f"event: connected\ndata: {json.dumps({'session_id': session_id})}\n\n"
            while True:
                try:
                    payload = await asyncio.wait_for(q.get(), timeout=15.0)
                    yield f"event: domain_update\ndata: {payload}\n\n"
                except asyncio.TimeoutError:
                    yield ": heartbeat\n\n"   # keep-alive
        except asyncio.CancelledError:
            pass
        finally:
            get_queues(session_id).discard(q)

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
