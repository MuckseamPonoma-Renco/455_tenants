"""Move existing headless services into this user's Background launchd domain.

Preview by default. No system domain, administrator credentials, or encryption
settings are changed. FileVault startup unlock remains a separate requirement.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import plistlib
import subprocess
import sys
import time
import datetime as dt
import urllib.request

NAMES = ('keep-awake', 'api', 'tunnel', 'automation', 'whatsapp_capture',
         'chat-export-sync', 'reliability-72h', 'watchdog')


class RollbackIncomplete(RuntimeError):
    pass


def launchctl(*args):
    return subprocess.run(['/bin/launchctl', *args], capture_output=True, text=True, timeout=30)


def wait_removed(target, run, *, timeout=30):
    # bootout acknowledges termination before the last process has exited.
    # Labels are shared across GUI and Background namespaces until teardown.
    deadline = time.monotonic() + timeout
    while run('print', target).returncode == 0:
        if time.monotonic() >= deadline:
            raise RuntimeError('Service teardown did not finish: ' + target)
        time.sleep(0.1)


def preview(home: Path, uid: int, run=launchctl):
    config = home / '.local/state/tenant-issue-os/launchd-domain'
    old = config.read_text().strip() if config.exists() else 'gui'
    if old != 'gui':
        raise ValueError('Migration requires the existing GUI configuration')
    rows = []
    for name in NAMES:
        label = 'tenant-issue-os.' + name
        path = home / 'Library/LaunchAgents' / (label + '.plist')
        if not path.exists():
            continue
        raw = path.read_bytes()
        body = plistlib.loads(raw)
        if body.get('Label') != label:
            raise ValueError('Plist label mismatch')
        if run('print', f'user/{uid}/{label}').returncode == 0:
            raise ValueError('Destination service already exists; refuse duplicate ownership')
        rows.append({'label': label, 'path': str(path), 'before': raw.decode(),
                     'sha256': hashlib.sha256(raw).hexdigest(),
                     'mode': path.stat().st_mode & 0o777,
                     'loaded': run('print', f'gui/{uid}/{label}').returncode == 0})
    if not rows:
        raise ValueError('No existing services found')
    return {'uid': uid, 'home': str(home), 'old_domain_config': config.read_text() if config.exists() else None,
            'source_domain': f'gui/{uid}', 'destination_domain': f'user/{uid}', 'services': rows,
            'limitation': 'Background domain is independent of GUI login; FileVault still needs startup unlock.'}


def migrate(plan, backup: Path, run=launchctl, verify=None):
    config = Path(plan['home']) / '.local/state/tenant-issue-os/launchd-domain'
    if (config.read_text() if config.exists() else None) != plan['old_domain_config']:
        raise ValueError('Domain configuration changed since preview')
    for row in plan['services']:
        if hashlib.sha256(Path(row['path']).read_bytes()).hexdigest() != row['sha256']:
            raise ValueError('Plist changed since preview')
        if run('print', plan['destination_domain'] + '/' + row['label']).returncode == 0:
            raise ValueError('Destination became occupied since preview')
        loaded = run('print', plan['source_domain'] + '/' + row['label']).returncode == 0
        if loaded != row['loaded']:
            raise ValueError('Service ownership changed since preview')
    backup.mkdir(mode=0o700, parents=True, exist_ok=False)
    record = backup / 'rollback.json'
    record.write_text(json.dumps(plan, indent=2)); record.chmod(0o600)
    def checked(*args):
        result = run(*args)
        if result.returncode:
            raise RuntimeError('launchctl ' + args[0] + ' failed')
    stopped = []
    started = []
    # Watchdog first: it must not resurrect GUI processes during transfer.
    stop_order = sorted(plan['services'], key=lambda r: not r['label'].endswith('.watchdog'))
    try:
        for row in stop_order:
            if row['loaded']:
                checked('bootout', plan['source_domain'] + '/' + row['label'])
                stopped.append(row)
                wait_removed(plan['source_domain'] + '/' + row['label'], run)
        for row in plan['services']:
            body = plistlib.loads(row['before'].encode())
            body['LimitLoadToSessionType'] = 'Background'
            path = Path(row['path']); path.write_bytes(plistlib.dumps(body)); path.chmod(row['mode'])
        config.write_text('user\n'); config.chmod(0o600)
        for row in plan['services']:
            if row['loaded']:
                checked('bootstrap', plan['destination_domain'], row['path'])
                started.append(row)
                checked('print', plan['destination_domain'] + '/' + row['label'])
                if run('print', plan['source_domain'] + '/' + row['label']).returncode == 0:
                    raise RuntimeError('GUI owner remained after migration')
        if verify is not None:
            verify()
    except BaseException:
        # Remove new ownership before restoring the exact old definitions.
        rollback_errors = []
        for row in reversed(plan['services']):
            target = plan['destination_domain'] + '/' + row['label']
            if run('print', target).returncode == 0:
                try:
                    checked('bootout', target)
                    wait_removed(target, run)
                except Exception:
                    rollback_errors.append(row['label'])
        for row in plan['services']:
            path = Path(row['path']); path.write_text(row['before']); path.chmod(row['mode'])
        if plan['old_domain_config'] is None:
            config.unlink(missing_ok=True)
        else:
            config.write_text(plan['old_domain_config'])
        for row in reversed(stopped):
            if row['label'] in rollback_errors:
                continue  # Never create a second owner if new owner could not stop.
            target = plan['source_domain'] + '/' + row['label']
            try:
                wait_removed(target, run)
                checked('bootstrap', plan['source_domain'], row['path'])
            except Exception:
                rollback_errors.append(row['label'])
        if rollback_errors:
            raise RollbackIncomplete('Migration failed; maintenance fence retained; rollback needs attention: ' + ', '.join(rollback_errors))
        raise
    return {'ok': True, 'migrated': [r['label'] for r in started], 'rollback': str(record)}


def verify_runtime(root: Path, started: float, *, timeout: float = 180):
    """Require new capture/automation cycles, not cached pre-migration green."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=10) as response:
                status = json.load(response)
            fresh = all(
                status[name].get('has_error') is False
                and dt.datetime.fromisoformat(status[name]['last_cycle_at'].replace('Z', '+00:00')).timestamp() >= started
                for name in ('whatsapp_capture', 'automation'))
            if fresh:
                checked = subprocess.run([str(root / 'scripts/check_mac_services.sh'), '--json'], capture_output=True, text=True, timeout=45)
                if checked.returncode == 0 and json.loads(checked.stdout).get('outcome') == 'healthy':
                    return
        except (KeyError, ValueError, OSError, TimeoutError, subprocess.TimeoutExpired):
            pass
        time.sleep(5)
    raise RuntimeError('Background services failed live health/heartbeat verification')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--backup-dir', type=Path)
    args = parser.parse_args()
    if sys.platform != 'darwin' or os.geteuid() == 0:
        parser.error('Run as the owning non-root macOS user')
    home = Path.home()
    state = home / '.local/state/tenant-issue-os'
    plan = preview(home, os.getuid())
    if not args.apply:
        print(json.dumps(plan, indent=2)); return
    if not args.backup_dir:
        parser.error('--apply requires a new --backup-dir')
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root))
    from packages.local_env import load_local_env_file
    load_local_env_file(root / '.env')
    if os.environ.get('WHATSAPP_CAPTURE_HEADLESS', '').lower() not in {'1', 'true', 'yes'}:
        parser.error('Verify headless WhatsApp capture before migration')
    from packages.db import get_session, FilingJob
    from sqlalchemy import select
    with get_session() as session:
        if session.scalar(select(FilingJob.job_id).where(FilingJob.state.in_(['claimed', 'submitting'])).limit(1)) is not None:
            parser.error('An active or unresolved submitting job must be reconciled before service migration')
    lock = state / 'install.lock'
    lock.mkdir()  # Existing installer/watchdog maintenance fence.
    rollback_incomplete = False
    try:
        started = time.time()
        print(json.dumps(migrate(plan, args.backup_dir, verify=lambda: verify_runtime(root, started)), indent=2))
    except RollbackIncomplete:
        rollback_incomplete = True
        raise
    finally:
        if not rollback_incomplete:
            lock.rmdir()


if __name__ == '__main__':
    main()
