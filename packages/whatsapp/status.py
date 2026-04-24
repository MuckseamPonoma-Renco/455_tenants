from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_CAPTURE_ROOT = Path.home() / ".local" / "share" / "tenant-issue-os" / "whatsapp_capture"


def default_status_path() -> Path:
    return DEFAULT_CAPTURE_ROOT / "status.json"


def resolve_status_path(path: str | Path | None = None) -> Path:
    configured = path or os.environ.get("WHATSAPP_CAPTURE_STATUS_PATH")
    raw = Path(configured).expanduser() if configured else default_status_path()
    return raw.resolve()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def read_capture_status(path: str | Path | None = None) -> dict[str, Any]:
    resolved = resolve_status_path(path)
    if not resolved.exists():
        return {}
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def write_capture_status(path: str | Path | None = None, **updates: Any) -> dict[str, Any]:
    resolved = resolve_status_path(path)
    payload = read_capture_status(resolved)
    payload.update({key: value for key, value in updates.items() if value is not None})
    payload["updated_at"] = _now_iso()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return payload
