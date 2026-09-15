"""Sanitized 311 capability health, separate from the process heartbeat."""
import os
from datetime import datetime, timezone
from pathlib import Path
from packages.automation_status import read_automation_status, write_automation_status


def status_path():
    return Path(os.environ.get('NYC311_STATUS_STATE_PATH') or
                str(Path.home() / '.local/state/tenant-issue-os/nyc311-status.json'))


def record_status_sync(summary):
    # Counts and outcome only: never raw portal text, complaint text or tokens.
    fields = {'ok', 'updated', 'total', 'attempted', 'checked', 'not_found',
              'unverified', 'deferred', 'errors', 'coverage_complete'}
    return write_automation_status(status_path(), **{k: v for k, v in summary.items() if k in fields})


def public_status():
    receipt = read_automation_status(status_path())
    if not receipt:
        return {'state': 'unverified', 'has_error': True}
    try:
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(receipt['updated_at'].replace('Z', '+00:00'))).total_seconds()
    except (KeyError, TypeError, ValueError):
        age = float('inf')
    stale = age > 7200 or age < -300
    failed = receipt.get('ok') is not True
    return {
        'state': 'stale' if stale else 'degraded' if failed else 'ready',
        'has_error': stale or failed,
        'last_completed_at': receipt.get('updated_at'),
        **{key: receipt.get(key, 0) for key in ('updated', 'total', 'not_found', 'unverified', 'deferred', 'errors')},
        'coverage_complete': receipt.get('coverage_complete') is True,
    }
