"""
SDTM Platform — FastAPI gateway
Run: uv run uvicorn main:app --reload --port 8000
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from mcp.client import lifespan_mcp
from middleware.audit_inject import AuditMiddleware
from routers import health, dag, events

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

R_MCP_URL = os.getenv("R_MCP_URL", "http://localhost:8001")


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with lifespan_mcp(R_MCP_URL):
        yield


app = FastAPI(
    title="SDTM Platform Gateway",
    version="2.0.0",
    description="FastAPI gateway for SDTM full-domain automation platform",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(AuditMiddleware)

app.include_router(health.router)
app.include_router(dag.router)
app.include_router(events.router)
