"""
Cross-cutting API security helpers.

  * APIKeyMiddleware  — rejects requests lacking a valid ``x-api-key`` header.
  * read_upload_capped — streams an UploadFile into memory with a hard size cap
    so a large upload cannot exhaust RAM (DoS).

Both are opt-in via environment variables so local development stays friction
free while production can lock things down:

  API_KEY                        enable auth when set (any non-empty value)
  ALLOWED_ORIGINS                comma-separated CORS allowlist
  MAX_UPLOAD_BYTES               reject uploads larger than this (default 100 MB)
  DOWNLOAD_CONTAINER_ALLOWLIST   comma-separated containers the download
                                 endpoint may read (empty = no restriction)
"""

import os

from fastapi import HTTPException, UploadFile
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", str(100 * 1024 * 1024)))
_CHUNK = 1024 * 1024  # 1 MB

# Endpoints reachable without an API key (health check + interactive docs).
_PUBLIC_PREFIXES = ("/api/health", "/docs", "/redoc", "/openapi.json")


async def read_upload_capped(upload: UploadFile, max_bytes: int = MAX_UPLOAD_BYTES) -> bytes:
    """Read an UploadFile in chunks, aborting with HTTP 413 past ``max_bytes``.

    Reading in chunks means an oversized upload is rejected before its full
    payload is ever held in memory.
    """
    buf = bytearray()
    while True:
        chunk = await upload.read(_CHUNK)
        if not chunk:
            break
        buf.extend(chunk)
        if len(buf) > max_bytes:
            raise HTTPException(
                status_code=413,
                detail=f"Upload exceeds the {max_bytes}-byte limit",
            )
    return bytes(buf)


class APIKeyMiddleware(BaseHTTPMiddleware):
    """Require a matching ``x-api-key`` header on every non-public request.

    Only wraps HTTP requests — WebSocket connections bypass Starlette HTTP
    middleware, so ``/ws/live`` stays open (read-only monitor events).
    Preflight ``OPTIONS`` requests are allowed through so CORS still works.
    """

    def __init__(self, app, api_key: str):
        super().__init__(app)
        self._api_key = api_key

    async def dispatch(self, request, call_next):
        if request.method == "OPTIONS" or request.url.path.startswith(_PUBLIC_PREFIXES):
            return await call_next(request)
        if request.headers.get("x-api-key") != self._api_key:
            return JSONResponse(
                {"detail": "Invalid or missing API key"}, status_code=401
            )
        return await call_next(request)
