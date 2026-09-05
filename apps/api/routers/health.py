import os
import json
import re
import shutil
import datetime as dt
from pathlib import Path
from typing import Any

from fastapi import APIRouter

from packages.automation_status import read_automation_status
from packages.db import database_is_ready
from packages.llm.openai_client import llm_enabled
from packages.whatsapp.status import read_capture_status

router = APIRouter()
DEFAULT_MIN_FREE_STORAGE_BYTES = 10 * 1024 * 1024 * 1024
DEFAULT_CHAT_EXPORT_SYNC_MAX_AGE_SECONDS = 3600
CLOUD_RECEIPT_STAGE_NAMES = (
    'upload',
    'discovery',
    'download',
    'processing',
    'audit',
    'sheet_sync',
    'sheet_readback',
    'acknowledgement',
)


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


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


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


def _cloud_chat_export_sync_state_path() -> Path:
    configured = _text(os.environ.get('CLOUD_CHAT_EXPORT_SYNC_STATE_PATH'))
    if configured:
        return Path(configured).expanduser()
    return Path.home() / '.local' / 'state' / 'tenant-issue-os' / 'cloud-chat-export-sync.json'


def _read_json_object(path: Path) -> tuple[dict[str, Any], str | None]:
    if not path.exists():
        return {}, 'missing'
    try:
        state = json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return {}, 'unreadable'
    if not isinstance(state, dict):
        return {}, 'unreadable'
    return state, None


def _chat_export_sync_is_stale(last_checked_at: str | None) -> bool:
    if not last_checked_at:
        return False
    try:
        checked_at = dt.datetime.fromisoformat(last_checked_at.replace('Z', '+00:00'))
    except ValueError:
        return True
    if checked_at.tzinfo is None:
        checked_at = checked_at.replace(tzinfo=dt.UTC)
    max_age_seconds = _positive_int(os.environ.get('CHAT_EXPORT_SYNC_MAX_AGE_SECONDS')) or DEFAULT_CHAT_EXPORT_SYNC_MAX_AGE_SECONDS
    return (_utcnow() - checked_at).total_seconds() > max_age_seconds


def _public_icloud_chat_export_sync_status(state: dict[str, Any], read_error: str | None) -> dict[str, Any]:
    if read_error:
        return {'state': read_error, 'has_error': read_error == 'unreadable'}
    error = _text(state.get('last_error'))
    last_checked_at = _text(state.get('last_checked_at'))
    stale = _chat_export_sync_is_stale(last_checked_at)
    if stale:
        status = 'stale'
    elif error and error.startswith('waiting for complete iCloud export:'):
        status = 'waiting_for_download'
    elif error and ('model review incomplete:' in error or 'insufficient_quota' in error):
        status = 'blocked_model_review'
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
        'last_checked_at': last_checked_at,
        'last_processed_at': _text(state.get('last_processed_at')),
        'has_error': bool(stale or (error and status in {'error', 'blocked_model_review'})),
    }


def _parse_timestamp(value: Any) -> dt.datetime | None:
    value = _text(value)
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed.astimezone(dt.UTC)


def _icloud_export_timestamp(state: dict[str, Any]) -> dt.datetime | None:
    timestamps: list[dt.datetime] = []
    for key in ('last_pending_fingerprint', 'last_seen_fingerprint', 'last_processed_fingerprint'):
        fingerprint = state.get(key)
        if not isinstance(fingerprint, dict):
            continue
        try:
            mtime_ns = int(fingerprint.get('mtime_ns') or 0)
        except (TypeError, ValueError):
            mtime_ns = 0
        if mtime_ns > 0:
            timestamps.append(dt.datetime.fromtimestamp(mtime_ns / 1_000_000_000, tz=dt.UTC))
    if timestamps:
        return max(timestamps)
    return _parse_timestamp(state.get('last_processed_at'))


def _cloud_key_timestamp(value: Any) -> dt.datetime | None:
    if not isinstance(value, str):
        return None
    match = re.match(r'^pending/(\d{8}T\d{6}(?:\d{3})?Z)-[a-f0-9]{32}-', value, re.IGNORECASE)
    if not match:
        return None
    stamp = match.group(1)
    for pattern in ('%Y%m%dT%H%M%S%fZ', '%Y%m%dT%H%M%SZ'):
        try:
            return dt.datetime.strptime(stamp, pattern).replace(tzinfo=dt.UTC)
        except ValueError:
            continue
    return None


def _cloud_receipt_effective_timestamp(receipt: dict[str, Any]) -> dt.datetime | None:
    acknowledged_at = _parse_timestamp(receipt.get('acknowledged_at'))
    keyed_at = _cloud_key_timestamp(receipt.get('key'))
    uploaded_at = _parse_timestamp(receipt.get('uploaded_at'))
    if (
        uploaded_at
        and (keyed_at is None or uploaded_at >= keyed_at)
        and (acknowledged_at is None or uploaded_at <= acknowledged_at)
    ):
        return uploaded_at
    if keyed_at and (acknowledged_at is None or keyed_at <= acknowledged_at):
        return keyed_at
    return _parse_timestamp(receipt.get('discovered_at'))


def _latest_cloud_receipt(state: dict[str, Any]) -> dict[str, Any] | None:
    receipts = state.get('receipts')
    if not isinstance(receipts, dict):
        return None
    candidates = [receipt for receipt in receipts.values() if isinstance(receipt, dict)]
    if not candidates:
        return None
    minimum = dt.datetime.min.replace(tzinfo=dt.UTC)

    def receipt_sort_key(receipt: dict[str, Any]) -> tuple[dt.datetime, dt.datetime, str]:
        effective_at = _cloud_receipt_effective_timestamp(receipt) or minimum
        acknowledged_at = _parse_timestamp(receipt.get('acknowledged_at')) or minimum
        return effective_at, acknowledged_at, str(receipt.get('key') or '')

    return max(
        candidates,
        key=receipt_sort_key,
    )


def _cloud_receipt_stage_states(receipt: dict[str, Any]) -> dict[str, str]:
    stages = receipt.get('stages')
    if not isinstance(stages, dict):
        stages = {}
    result: dict[str, str] = {}
    for name in CLOUD_RECEIPT_STAGE_NAMES:
        value = stages.get(name)
        if isinstance(value, dict):
            result[name] = _text(value.get('state')) or 'not_reported'
        else:
            result[name] = 'not_reported'
    return result


def _cloud_receipt_status(receipt: dict[str, Any], stages: dict[str, str]) -> str:
    if receipt.get('legacy_reconstructed') is True:
        return 'legacy_unverified'
    if not isinstance(receipt.get('stages'), dict):
        return _text(receipt.get('status')) or 'unknown'
    if stages['audit'] == 'blocked_model_review':
        return 'blocked_model_review'
    if any(stages[name] == 'error' for name in ('download', 'processing')):
        return 'processing_error'
    if stages['audit'] == 'error':
        return 'audit_error'
    if stages['sheet_sync'] == 'error':
        return 'sheet_sync_failed'
    if stages['sheet_readback'] == 'error':
        return 'sheet_readback_failed'
    if stages['acknowledgement'] == 'error':
        return 'acknowledgement_error'
    if stages['processing'] in {'pending', 'running'} or stages['audit'] == 'running':
        return 'processing'
    if stages['audit'] != 'complete':
        return 'discovered'
    if stages['sheet_sync'] != 'complete':
        return 'sheet_sync_unverified' if stages['sheet_sync'] == 'not_reported' else 'sheet_sync_pending'
    if stages['sheet_readback'] != 'complete':
        return (
            'sheet_readback_unverified'
            if stages['sheet_readback'] == 'not_reported'
            else 'sheet_readback_pending'
        )
    if stages['acknowledgement'] != 'complete':
        return 'pending_acknowledgement'
    return 'ready'


def _public_cloud_receipt(receipt: dict[str, Any]) -> dict[str, Any]:
    stages = _cloud_receipt_stage_states(receipt)
    status = _cloud_receipt_status(receipt, stages)
    return {
        'source': 'cloud_receiver',
        'uploaded_at': _text(receipt.get('uploaded_at')),
        'discovered_at': _text(receipt.get('discovered_at')),
        'receipt_version': _positive_int(receipt.get('receipt_version')),
        'status': status,
        'stages': stages,
    }


def _cloud_receipt_context(
    state: dict[str, Any],
) -> tuple[dict[str, Any] | None, dict[str, Any], list[str]]:
    receipt = _latest_cloud_receipt(state)
    pending_keys = state.get('pending_export_keys')
    pending_key_values = [str(key) for key in pending_keys] if isinstance(pending_keys, list) else []
    receipts = state.get('receipts') if isinstance(state.get('receipts'), dict) else {}
    pending_statuses: list[str] = []
    for key in pending_key_values:
        pending_receipt = receipts.get(key)
        if isinstance(pending_receipt, dict):
            stages = _cloud_receipt_stage_states(pending_receipt)
            pending_statuses.append(_cloud_receipt_status(pending_receipt, stages))
    context: dict[str, Any] = {
        'pending_cloud_exports': len(set(pending_key_values)),
        'blocked_cloud_exports': sum(status == 'blocked_model_review' for status in pending_statuses),
    }
    if receipt is not None:
        context['latest_export'] = _public_cloud_receipt(receipt)
    return receipt, context, pending_statuses


def _public_chat_export_sync_status() -> dict[str, Any]:
    icloud_state, icloud_read_error = _read_json_object(_chat_export_sync_state_path())
    icloud_status = _public_icloud_chat_export_sync_status(icloud_state, icloud_read_error)
    if not _cloud_export_receiver_configured():
        return icloud_status

    cloud_state, cloud_read_error = _read_json_object(_cloud_chat_export_sync_state_path())
    if cloud_read_error:
        return {
            'state': f'cloud_state_{cloud_read_error}',
            'last_checked_at': None,
            'last_processed_at': icloud_status.get('last_processed_at'),
            'has_error': True,
        }

    cloud_checked_at = _text(cloud_state.get('last_checked_at'))
    if not cloud_checked_at:
        return {
            'state': 'cloud_state_unknown',
            'last_checked_at': None,
            'last_processed_at': icloud_status.get('last_processed_at'),
            'has_error': True,
        }
    receipt, receipt_context, pending_statuses = _cloud_receipt_context(cloud_state)
    if _chat_export_sync_is_stale(cloud_checked_at):
        return {
            'state': 'stale',
            'last_checked_at': cloud_checked_at,
            'last_processed_at': icloud_status.get('last_processed_at'),
            'has_error': True,
            **receipt_context,
        }
    hard_failure_states = (
        'acknowledgement_error',
        'audit_error',
        'processing_error',
        'sheet_sync_failed',
        'sheet_readback_failed',
    )
    cloud_error = _text(cloud_state.get('last_error'))
    if cloud_error:
        latest_status = (
            str(receipt_context['latest_export']['status'])
            if isinstance(receipt_context.get('latest_export'), dict)
            else ''
        )
        pending_failure = next(
            (candidate for candidate in hard_failure_states if candidate in pending_statuses),
            None,
        )
        return {
            'state': pending_failure or (latest_status if latest_status in hard_failure_states else 'cloud_error'),
            'last_checked_at': cloud_checked_at,
            'last_processed_at': icloud_status.get('last_processed_at'),
            'has_error': True,
            **receipt_context,
        }

    if receipt is None:
        if receipt_context['pending_cloud_exports']:
            return {
                'state': 'pending_cloud_exports',
                'last_checked_at': cloud_checked_at,
                'last_processed_at': icloud_status.get('last_processed_at'),
                'has_error': False,
                **receipt_context,
            }
        return {**icloud_status, 'last_checked_at': cloud_checked_at or icloud_status.get('last_checked_at')}

    cloud_export_at = _cloud_receipt_effective_timestamp(receipt)
    icloud_export_at = _icloud_export_timestamp(icloud_state)
    if (
        cloud_export_at
        and icloud_export_at
        and cloud_export_at < icloud_export_at
        and not receipt_context['pending_cloud_exports']
    ):
        return {**icloud_status, 'last_checked_at': cloud_checked_at or icloud_status.get('last_checked_at')}

    latest_export = receipt_context['latest_export']
    receipt_status = str(latest_export['status'])
    pending_failures = (*hard_failure_states, 'blocked_model_review')
    status = next((candidate for candidate in pending_failures if candidate in pending_statuses), receipt_status)
    if status == 'ready' and receipt_context['pending_cloud_exports']:
        status = 'pending_cloud_exports'
    processed_stage = receipt.get('stages', {}).get('processing', {}) if isinstance(receipt.get('stages'), dict) else {}
    has_error = status in {
        'acknowledgement_error',
        'audit_error',
        'blocked_model_review',
        'cloud_error',
        'legacy_unverified',
        'processing_error',
        'sheet_readback_failed',
        'sheet_readback_unverified',
        'sheet_sync_failed',
        'sheet_sync_unverified',
    }
    return {
        'state': status,
        'last_checked_at': cloud_checked_at,
        'last_processed_at': _text(processed_stage.get('at')),
        'has_error': has_error,
        **receipt_context,
    }


def _cloud_export_receiver_configured() -> bool:
    return bool(_text(os.environ.get('CLOUD_EXPORT_RECEIVER_URL')) and _text(os.environ.get('CLOUD_EXPORT_RECEIVER_PULL_TOKEN')))


def _public_cloud_export_receiver_status() -> dict[str, Any]:
    configured = _cloud_export_receiver_configured()
    return {
        'state': 'configured' if configured else 'not_configured',
        'configured': configured,
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
        'cloud_export_receiver': _public_cloud_export_receiver_status(),
        'storage': _public_storage_status(),
    }
