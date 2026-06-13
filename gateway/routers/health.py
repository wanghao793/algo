from fastapi import APIRouter
from ..mcp.client import get_client, MCPError
from ..models.responses import ApiResponse

router = APIRouter(prefix="/api/v2", tags=["system"])


@router.get("/health", response_model=ApiResponse)
async def health():
    client = get_client()
    try:
        r_status = await client.call_tool("health_check")
        return ApiResponse.ok({
            "gateway": "ok",
            "r_mcp_server": r_status,
        })
    except MCPError as exc:
        return ApiResponse.fail("R_MCP_ERROR", str(exc), http_status=502)
    except Exception as exc:
        return ApiResponse.fail("GATEWAY_ERROR", str(exc), http_status=500)
