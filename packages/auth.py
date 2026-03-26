from __future__ import annotations

import os
from fastapi import Header, HTTPException


def _expected_token(kind: str = "default") -> str:
    if kind == "mobile":
        return (os.environ.get("MOBILE_FILER_TOKEN") or os.environ.get("INGEST_TOKEN") or "").strip()
    return (os.environ.get("INGEST_TOKEN") or "").strip()


def require_bearer_token(auth_header: str | None, *, kind: str = "default") -> None:
    token = _expected_token(kind)
    if not token:
        raise HTTPException(status_code=500, detail="Auth token not configured")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Authorization: Bearer <token>")
    provided = auth_header.removeprefix("Bearer ").strip()
    if provided != token:
        raise HTTPException(status_code=403, detail="Invalid token")


def auth_header_dependency(kind: str = "default"):
    def _dep(authorization: str | None = Header(default=None)):
        require_bearer_token(authorization, kind=kind)
    return _dep
