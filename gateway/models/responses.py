from __future__ import annotations
from typing import Any
from pydantic import BaseModel
from datetime import datetime, timezone


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Meta(BaseModel):
    timestamp_utc: str = ""
    api_version: str = "2.0"
    session_id: str | None = None
    domain: str | None = None

    model_config = {"populate_by_name": True}


class ApiError(BaseModel):
    code: str
    message: str
    detail: Any = None
    http_status: int = 500


class ApiResponse(BaseModel):
    success: bool
    data: Any = None
    meta: Meta = Meta()
    error: ApiError | None = None

    @classmethod
    def ok(cls, data: Any, **meta_kwargs: Any) -> "ApiResponse":
        return cls(success=True, data=data, meta=Meta(timestamp_utc=_now_utc(), **meta_kwargs))

    @classmethod
    def fail(cls, code: str, message: str, http_status: int = 500, detail: Any = None) -> "ApiResponse":
        return cls(
            success=False,
            meta=Meta(timestamp_utc=_now_utc()),
            error=ApiError(code=code, message=message, http_status=http_status, detail=detail),
        )


class DomainStatus(BaseModel):
    domain: str
    full_name: str
    class_: str
    layer: int
    depends_on: list[str]
    status: str

    model_config = {"populate_by_name": True}
