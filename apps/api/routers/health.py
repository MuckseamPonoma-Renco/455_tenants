import os
import json
import shutil
from pathlib import Path
from typing import Any

from fastapi import APIRouter

from packages.automation_status import read_automation_status
from packages.db import database_is_ready
from packages.llm.openai_client import llm_enabled
from packages.whatsapp.status import read_capture_status

router = APIRouter()
DEFAULT_MIN_FREE_STORAGE_BYTES = 10 * 1024 * 1024 * 1024


def _truthy(name: str, default: str = '0') -> bool:
    return os.environ.get(name, default).strip().lower() in {'1', 'true', 'yes', 'on'}


def _sheets_creds_present() -> bool:
    candidates = [
        os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'),
        '/run/secrets/gcp_sa.json',
        '/run/secrets/gcp_sa_json',
        '/etc/secrets/gcp_sa.json',
        'secrets/gcp_sa.json',
    ]
    return any(path and Path(path).exists() for path in candidates)


def _text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value or None


def _positive_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _storage_health_path() -> Path:
    configured = _text(os.environ.get('HEALTH_STORAGE_PATH'))
    if configured:
        return Path(configured).expanduser()
    return Path.home()


def _minimum_free_storage_bytes() -> int:
    return _positive_int(os.environ.get('HEALTH_MIN_FREE_STORAGE_BYTES')) or DEFAULT_MIN_FREE_STORAGE_BYTES


def _public_storage_status() -> dict[str, Any]:
    try:
        free_bytes = shutil.disk_usage(_storage_health_path()).free
    except OSError:
        return {'state': 'unavailable', 'low_disk': False}

    low_disk = free_bytes < _minimum_free_storage_bytes()
    return {
        'state': 'low_disk' if low_disk else 'ready',
        'low_disk': low_disk,
    }


def _public_capture_status(status: dict[str, Any]) -> dict[str, Any] | None:
    if not status:
        return None
    # /health is reachable through the public tunnel. Keep only operational
    # indicators there, never chat names, local paths, or raw browser errors.
    return {
        'state': _text(status.get('state')) or 'missing',
        'login_required': bool(status.get('login_required')),
        'last_cycle_at': _text(status.get('last_cycle_at')),
        'poll_seconds': _positive_int(status.get('poll_seconds')),
        'updated_at': _text(status.get('updated_at')),
        'has_error': bool(_text(status.get('last_error'))),
    }


def _public_automation_status(status: dict[str, Any]) -> dict[str, Any]:
    return {
        'state': _text(status.get('state')) or 'missing',
        'last_cycle_at': _text(status.get('last_cycle_at')),
        'poll_seconds': _positive_int(status.get('poll_seconds')),
        'updated_at': _text(status.get('updated_at')),
        'has_error': bool(_text(status.get('last_error'))),
    }


def _chat_export_sync_state_path() -> Path:
    configured = _text(os.environ.get('CHAT_EXPORT_SYNC_STATE_PATH'))
    if configured:
        return Path(configured).expanduser()
    return Path.home() / '.local' / 'state' / 'tenant-issue-os' / 'chat-export-sync.json'


def _public_chat_export_sync_status() -> dict[str, Any]:
    path = _chat_export_sync_state_path()
    if not path.exists():
        return {'state': 'missing', 'has_error': False}
    try:
        state = json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return {'state': 'unreadable', 'has_error': True}
    if not isinstance(state, dict):
        return {'state': 'unreadable', 'has_error': True}

    error = _text(state.get('last_error'))
    if error and error.startswith('waiting for complete iCloud export:'):
        status = 'waiting_for_download'
    elif error:
        status = 'error'
    elif state.get('last_processed_fingerprint'):
        status = 'ready'
    elif _text(state.get('last_checked_at')):
        status = 'no_export'
    else:
        status = 'unknown'

    return {
        'state': status,
        'last_checked_at': _text(state.get('last_checked_at')),
        'last_processed_at': _text(state.get('last_processed_at')),
        'has_error': bool(error and status == 'error'),
    }


@router.get('/health')
def health():
    whatsapp_capture = _public_capture_status(read_capture_status())
    database_configured = bool((os.environ.get('DATABASE_URL') or '').strip())
    return {
        'ok': True,
        'process_inline': _truthy('PROCESS_INLINE'),
        'llm_enabled': llm_enabled(),
        'sheets_disabled': _truthy('DISABLE_SHEETS_SYNC'),
        'database_configured': database_configured,
        'database_ready': database_configured and database_is_ready(),
        'redis_configured': bool((os.environ.get('REDIS_URL') or '').strip()),
        'sheets_configured': bool((os.environ.get('GOOGLE_SHEETS_SPREADSHEET_ID') or '').strip()) and _sheets_creds_present(),
        'whatsapp_capture': whatsapp_capture,
        'automation': _public_automation_status(read_automation_status()),
        'chat_export_sync': _public_chat_export_sync_status(),
        'storage': _public_storage_status(),
    }
