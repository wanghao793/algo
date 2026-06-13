from fastapi import APIRouter, HTTPException
from ..mcp.client import get_client, MCPError
from ..models.responses import ApiResponse

router = APIRouter(prefix="/api/v2/dag", tags=["dag"])


@router.get("/status/{session_id}", response_model=ApiResponse)
async def dag_status(session_id: str):
    try:
        data = await get_client().call_tool("get_dag_status", {"session_id": session_id})
        return ApiResponse.ok(data, session_id=session_id)
    except MCPError as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@router.get("/available/{session_id}", response_model=ApiResponse)
async def dag_available(session_id: str):
    try:
        data = await get_client().call_tool("get_dag_available", {"session_id": session_id})
        return ApiResponse.ok(data, session_id=session_id)
    except MCPError as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@router.post("/mark-complete", response_model=ApiResponse)
async def mark_complete(session_id: str, domain: str):
    try:
        data = await get_client().call_tool(
            "set_domain_status",
            {"session_id": session_id, "domain": domain, "status": "complete"}
        )
        return ApiResponse.ok(data, session_id=session_id, domain=domain)
    except MCPError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


@router.post("/reset/{domain}", response_model=ApiResponse)
async def reset_domain(domain: str, session_id: str):
    try:
        data = await get_client().call_tool(
            "set_domain_status",
            {"session_id": session_id, "domain": domain, "status": "pending"}
        )
        return ApiResponse.ok(data, session_id=session_id, domain=domain)
    except MCPError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
