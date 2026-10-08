from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_AUTOMATION_STATUS_PATH = Path.home() / ".local" / "state" / "tenant-issue-os" / "automation.json"
DEFAULT_PUBLIC_RECORD_SYNC_SECONDS = 21600
DEFAULT_PUBLIC_RECORD_RETRY_SECONDS = 300
DEFAULT_PUBLIC_RECORD_GRACE_SECONDS = 1800


def _nonnegative_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except (ValueError, TypeError):
        return default
    return parsed if parsed >= 0 else default


def public_record_sync_seconds() -> int:
    return _nonnegative_int(
        os.environ.get("AUTOMATION_PUBLIC_RECORD_SYNC_SECONDS", os.environ.get("PUBLIC_RECORD_SYNC_SECONDS")),
        DEFAULT_PUBLIC_RECORD_SYNC_SECONDS,
    )


def public_record_retry_seconds(interval_seconds: int) -> int:
    configured = _nonnegative_int(
        os.environ.get("AUTOMATION_PUBLIC_RECORD_RETRY_SECONDS"), DEFAULT_PUBLIC_RECORD_RETRY_SECONDS
    )
    return min(max(10, interval_seconds), max(10, configured))


def watchdog_max_age_seconds(interval_seconds: Any = None) -> int:
    interval = _nonnegative_int(interval_seconds, public_record_sync_seconds())
    grace = _nonnegative_int(
        os.environ.get("PUBLIC_RECORD_SYNC_GRACE_SECONDS"), DEFAULT_PUBLIC_RECORD_GRACE_SECONDS
    )
    return interval + grace


def watchdog_source_receipts_complete(
    sources: Any, *, now: datetime | None = None, max_age_seconds: int | None = None
) -> bool:
    """Require every configured source, including sources that returned zero rows."""
    from packages.public_records.config import source_configs

    if not isinstance(sources, list) or not sources:
        return False
    required_keys = {source.key for source in source_configs()}
    source_keys = [source.get("source_key") for source in sources if isinstance(source, dict)]
    if not all(isinstance(key, str) for key in source_keys):
        return False
    if len(source_keys) != len(sources) or len(source_keys) != len(required_keys) or set(source_keys) != required_keys:
        return False
    for source in sources:
        if source.get("state") not in {"ready", "empty"} or source.get("source_errors") != 0 or source.get("ok") is False:
            return False
        timestamp = source.get("last_success_at")
        try:
            checked_at = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except (AttributeError, TypeError, ValueError):
            return False
        if checked_at.tzinfo is None:
            return False
        if now is not None:
            maximum_age = watchdog_max_age_seconds() if max_age_seconds is None else max_age_seconds
            if not 0 <= (now - checked_at).total_seconds() <= maximum_age:
                return False
    return True


def watchdog_result_ok(result: Any) -> bool:
    """A completed loop is not a successful sync if any source or Sheet failed."""
    if not isinstance(result, dict) or result.get("ok") is not True:
        return False
    # Missing source-error accounting is not proof of a complete source sync.
    if _nonnegative_int(result.get("source_errors"), -1) != 0:
        return False
    if _nonnegative_int(result.get("sheet_errors"), -1) != 0 or result.get("sheet_readback_ok") is not True:
        return False
    source_health = result.get("source_health")
    return (
        isinstance(source_health, dict)
        and source_health.get("ok") is True
        and watchdog_source_receipts_complete(
            source_health.get("sources"), now=datetime.fromisoformat(_now_iso().replace("Z", "+00:00"))
        )
    )


def update_watchdog_status(
    *,
    state: str,
    interval_seconds: int,
    result: dict[str, Any] | None = None,
    error: str = "",
    path: str | Path | None = None,
) -> dict[str, Any]:
    """Persist subtask completion separately from the continuously updated heartbeat."""
    previous = read_automation_status(path).get("watchdog")
    status = dict(previous) if isinstance(previous, dict) else {}
    timestamp = _now_iso()
    status.update(
        state=state,
        interval_seconds=interval_seconds,
        retry_seconds=public_record_retry_seconds(interval_seconds),
    )
    if state == "working":
        status["last_attempt_at"] = timestamp
    elif state in {"ready", "degraded"}:
        succeeded = watchdog_result_ok(result) and not error
        source_errors = _nonnegative_int((result or {}).get("source_errors"), -1)
        sheet_errors = _nonnegative_int((result or {}).get("sheet_errors"), -1)
        status.update(
            state="ready" if succeeded else "degraded",
            last_completed_at=timestamp,
            source_errors=source_errors if source_errors >= 0 else None,
            sheet_errors=sheet_errors if sheet_errors >= 0 else (0 if succeeded else None),
            sheet_readback_ok=(result or {}).get("sheet_readback_ok") is True,
            has_error=not succeeded,
            last_error=error,
        )
        if isinstance((result or {}).get("source_health"), dict):
            status["source_health"] = result["source_health"]
        if succeeded:
            status["last_success_at"] = timestamp
    write_automation_status(path, watchdog=status)
    return status


def default_status_path() -> Path:
    return DEFAULT_AUTOMATION_STATUS_PATH


def resolve_status_path(path: str | Path | None = None) -> Path:
    configured = path or os.environ.get("AUTOMATION_STATUS_PATH")
    raw = Path(configured).expanduser() if configured else default_status_path()
    return raw.resolve()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def read_automation_status(path: str | Path | None = None) -> dict[str, Any]:
    resolved = resolve_status_path(path)
    if not resolved.exists():
        return {}
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def write_automation_status(path: str | Path | None = None, **updates: Any) -> dict[str, Any]:
    resolved = resolve_status_path(path)
    payload = read_automation_status(resolved)
    payload.update({key: value for key, value in updates.items() if value is not None})
    payload["updated_at"] = _now_iso()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    temporary = resolved.with_name(f".{resolved.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(temporary, resolved)
    return payload
