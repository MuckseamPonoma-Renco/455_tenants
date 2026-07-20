#!/usr/bin/env python3
"""Validate the non-sensitive operational fields published by /health."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import urllib.error
import urllib.request
from typing import Any


def _parse_timestamp(value: Any) -> dt.datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(dt.UTC)


def _age_seconds(value: Any, now: dt.datetime) -> int | None:
    parsed = _parse_timestamp(value)
    if parsed is None:
        return None
    return max(0, int((now - parsed).total_seconds()))


def validate_health(
    payload: Any,
    *,
    now: dt.datetime,
    max_capture_age_seconds: int,
    max_automation_age_seconds: int,
    max_import_age_seconds: int,
    require_cloud_export_receiver: bool = False,
) -> tuple[list[str], dict[str, Any]]:
    failures: list[str] = []
    details: dict[str, Any] = {}
    if not isinstance(payload, dict):
        return ['health response was not a JSON object'], details

    if payload.get('ok') is not True:
        failures.append('health endpoint did not report ok=true')
    if payload.get('database_configured') is not True:
        failures.append('database is not configured')
    if payload.get('database_ready') is not True:
        failures.append('database is not reachable')
    if payload.get('sheets_disabled') is True or payload.get('sheets_configured') is not True:
        failures.append('Google Sheets sync is not configured and enabled')

    storage = payload.get('storage')
    if not isinstance(storage, dict):
        failures.append('host storage health is missing')
    else:
        storage_state = storage.get('state')
        details['storage_state'] = storage_state
        if storage_state != 'ready' or storage.get('low_disk') is not False:
            failures.append(f"host storage is {storage_state or 'unknown'}")

    automation = payload.get('automation')
    if not isinstance(automation, dict):
        failures.append('automation health is missing')
    else:
        automation_state = automation.get('state')
        if automation_state not in {'ready', 'starting', 'working'}:
            failures.append(f"automation is {automation_state or 'unknown'}")
        if automation.get('has_error') is True:
            failures.append('automation has an error')
        automation_age = _age_seconds(automation.get('last_cycle_at'), now)
        details['automation_age_seconds'] = automation_age
        if automation_age is None:
            failures.append('automation has no valid last_cycle_at timestamp')
        elif automation_age > max_automation_age_seconds:
            failures.append(f'automation is stale ({automation_age}s old)')

    capture = payload.get('whatsapp_capture')
    if not isinstance(capture, dict):
        failures.append('WhatsApp capture health is missing')
    else:
        if capture.get('state') != 'ready' or capture.get('login_required') is not False:
            failures.append('WhatsApp capture is not ready')
        if capture.get('has_error') is True:
            failures.append('WhatsApp capture has an error')
        capture_age = _age_seconds(capture.get('last_cycle_at'), now)
        details['whatsapp_capture_age_seconds'] = capture_age
        if capture_age is None:
            failures.append('WhatsApp capture has no valid last_cycle_at timestamp')
        elif capture_age > max_capture_age_seconds:
            failures.append(f'WhatsApp capture is stale ({capture_age}s old)')

    sync = payload.get('chat_export_sync')
    if not isinstance(sync, dict):
        failures.append('chat export sync health is missing')
    else:
        if sync.get('state') not in {'ready', 'no_export', 'waiting_for_download'}:
            failures.append(f"chat export sync is {sync.get('state') or 'unknown'}")
        if sync.get('has_error') is True:
            failures.append('chat export sync has an error')
        import_age = _age_seconds(sync.get('last_checked_at'), now)
        details['chat_export_sync_age_seconds'] = import_age
        if import_age is None:
            failures.append('chat export sync has no valid last_checked_at timestamp')
        elif import_age > max_import_age_seconds:
            failures.append(f'chat export sync is stale ({import_age}s old)')

    cloud_receiver = payload.get('cloud_export_receiver')
    if isinstance(cloud_receiver, dict):
        receiver_state = cloud_receiver.get('state')
        receiver_configured = cloud_receiver.get('configured') is True
        details['cloud_export_receiver_state'] = receiver_state
        details['cloud_export_receiver_configured'] = receiver_configured
        if require_cloud_export_receiver and not receiver_configured:
            failures.append(f"cloud export receiver is {receiver_state or 'not_configured'}")
    elif require_cloud_export_receiver:
        failures.append('cloud export receiver health is missing')

    return failures, details


def fetch_health(url: str, timeout_seconds: int) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={'Accept': 'application/json', 'User-Agent': 'tenant-issue-os-healthcheck/1'})
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        if response.status != 200:
            raise RuntimeError(f'health endpoint returned HTTP {response.status}')
        payload = json.load(response)
    if not isinstance(payload, dict):
        raise RuntimeError('health endpoint returned a non-object JSON value')
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', required=True, help='Public /health URL to validate.')
    parser.add_argument('--timeout-seconds', type=int, default=20)
    parser.add_argument('--max-capture-age-seconds', type=int, default=600)
    parser.add_argument('--max-automation-age-seconds', type=int, default=1200)
    parser.add_argument('--max-import-age-seconds', type=int, default=3600)
    parser.add_argument('--require-cloud-export-receiver', action='store_true')
    args = parser.parse_args()

    try:
        payload = fetch_health(args.url, max(1, args.timeout_seconds))
        failures, details = validate_health(
            payload,
            now=dt.datetime.now(dt.UTC),
            max_capture_age_seconds=max(1, args.max_capture_age_seconds),
            max_automation_age_seconds=max(1, args.max_automation_age_seconds),
            max_import_age_seconds=max(1, args.max_import_age_seconds),
            require_cloud_export_receiver=args.require_cloud_export_receiver,
        )
    except (OSError, RuntimeError, urllib.error.URLError, json.JSONDecodeError) as exc:
        print(json.dumps({'ok': False, 'failures': [str(exc)]}, sort_keys=True))
        return 1

    print(json.dumps({'ok': not failures, 'failures': failures, **details}, sort_keys=True))
    return 0 if not failures else 1


if __name__ == '__main__':
    raise SystemExit(main())
