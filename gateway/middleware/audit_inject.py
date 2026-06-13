"""
Audit injection middleware — stub for 21 CFR Part 11 compliance.
In v2.0 this will intercept MAPPING_APPROVED, MAPPING_MODIFIED, etc.
"""

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
import logging

log = logging.getLogger("sdtm.audit")

AUDITED_PATHS = {"/api/v2/mappings"}


class AuditMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        if any(request.url.path.startswith(p) for p in AUDITED_PATHS):
            # TODO: extract session_id, domain, user_id from request
            # and call write_audit_event MCP tool
            log.debug("AUDIT stub: %s %s → %d", request.method, request.url.path, response.status_code)
        return response
