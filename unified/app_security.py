"""
Cross-cutting API security helpers.

  * APIKeyMiddleware  — rejects requests lacking a valid ``x-api-key`` header.
  * read_upload_capped — streams an UploadFile into memory with a hard size cap
    so a large upload cannot exhaust RAM (DoS).

Configured via environment variables:

  API_KEY                        require a matching x-api-key header when set.
                                 When unset, the API only answers loopback
                                 clients (127.0.0.1 / ::1) — local dev keeps
                                 working, but nothing is exposed on the network.
  ALLOWED_ORIGINS                comma-separated CORS allowlist
  MAX_UPLOAD_BYTES               reject uploads larger than this (default 100 MB)
  DOWNLOAD_CONTAINER_ALLOWLIST   comma-separated containers the download
                                 endpoint may read (empty = no restriction)
"""

import hmac
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
        if len(buf) + len(chunk) > max_bytes:
            raise HTTPException(
                status_code=413,
                detail=f"Upload exceeds the {max_bytes}-byte limit",
            )
        buf.extend(chunk)
    return bytes(buf)


_LOOPBACK_HOSTS = {"127.0.0.1", "::1", "localhost"}


def is_loopback(client) -> bool:
    return bool(client) and client.host in _LOOPBACK_HOSTS


def api_key_matches(provided, expected: str) -> bool:
    """Constant-time compare so the key cannot be recovered via response timing."""
    if not provided or not expected:
        return False
    return hmac.compare_digest(provided.encode(), expected.encode())


class APIKeyMiddleware(BaseHTTPMiddleware):
    """Require a matching ``x-api-key`` header on every non-public request.

    With no API key configured, only loopback clients are served. Preflight
    ``OPTIONS`` requests are allowed through so CORS still works. WebSockets
    bypass HTTP middleware — ``/ws/live`` checks access itself via
    ``websocket_allowed``.
    """

    def __init__(self, app, api_key: str = ""):
        super().__init__(app)
        self._api_key = api_key

    async def dispatch(self, request, call_next):
        if request.method == "OPTIONS" or request.url.path.startswith(_PUBLIC_PREFIXES):
            return await call_next(request)
        if not self._api_key:
            if not is_loopback(request.client):
                return JSONResponse(
                    {"detail": "API_KEY not configured; only local requests are allowed"},
                    status_code=403,
                )
            return await call_next(request)
        if not api_key_matches(request.headers.get("x-api-key"), self._api_key):
            return JSONResponse(
                {"detail": "Invalid or missing API key"}, status_code=401
            )
        return await call_next(request)


def websocket_allowed(websocket, api_key: str) -> bool:
    """Same policy as APIKeyMiddleware, for WebSockets. Browsers cannot set
    headers on a WebSocket, so the key is accepted as ``?api_key=``."""
    if not api_key:
        return is_loopback(websocket.client)
    provided = websocket.headers.get("x-api-key") or websocket.query_params.get("api_key")
    return api_key_matches(provided, api_key)
