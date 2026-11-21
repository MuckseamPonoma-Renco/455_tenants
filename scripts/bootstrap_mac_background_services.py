"""Load existing Background agents after login; never restart a loaded owner.

~/Library/LaunchAgents is loaded for the GUI session. Background-only plists
there are not enough to recreate user-domain jobs after reboot. This small
Aqua agent bridges that startup event without moving the workers into Aqua.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import plistlib
import subprocess
import sys
import urllib.request

LABEL = 'tenant-issue-os.bootstrap'
NAMES = ('keep-awake', 'api', 'tunnel', 'automation', 'whatsapp_capture',
         'chat-export-sync', 'watchdog')


def launchctl(*args):
    return subprocess.run(['/bin/launchctl', *args], capture_output=True, text=True, timeout=20)


def ensure_background_services(home: Path, uid: int, run=launchctl):
    state = home / '.local/state/tenant-issue-os'
    domain = state / 'launchd-domain'
    if not domain.is_file() or domain.read_text().strip() != 'user':
        return {'ok': True, 'action': 'background_domain_not_selected', 'services': []}
    if (state / 'install.lock').exists():
        return {'ok': True, 'action': 'maintenance_in_progress', 'services': []}
    rows = []
    for name in NAMES:
        label = 'tenant-issue-os.' + name
        path = home / 'Library/LaunchAgents' / (label + '.plist')
        if not path.is_file():
            continue  # Optional services must never be invented or enabled.
        try:
            body = plistlib.loads(path.read_bytes())
        except (OSError, ValueError, plistlib.InvalidFileException):
            rows.append({'label': label, 'ok': False, 'action': 'unreadable_definition'})
            continue
        if body.get('Label') != label or body.get('LimitLoadToSessionType') != 'Background':
            rows.append({'label': label, 'ok': False, 'action': 'invalid_definition'})
            continue
        target = f'user/{uid}/{label}'
        gui_loaded = run('print', f'gui/{uid}/{label}').returncode == 0
        user_loaded = run('print', target).returncode == 0
        if gui_loaded:
            rows.append({'label': label, 'ok': False, 'action': 'gui_owner_conflict'})
            continue
        if user_loaded:
            rows.append({'label': label, 'ok': True, 'action': 'already_loaded'})
            continue
        result = run('bootstrap', f'user/{uid}', str(path))
        loaded = run('print', target).returncode == 0
        rows.append({'label': label, 'ok': loaded, 'action': 'loaded' if loaded else 'load_failed',
                     'returncode': result.returncode})
    return {'ok': bool(rows) and all(r['ok'] for r in rows), 'action': 'checked', 'services': rows}


def loader_definition(root: Path, home: Path):
    logs = home / '.local/var/log/tenant-issue-os'
    return {'Label': LABEL, 'ProgramArguments': [str(root / 'scripts/run_background_bootstrap.sh')],
            'WorkingDirectory': str(root), 'RunAtLoad': True, 'StartInterval': 60,
            'ThrottleInterval': 15, 'LimitLoadToSessionType': 'Aqua',
            'StandardOutPath': str(logs / 'bootstrap.out.log'),
            'StandardErrorPath': str(logs / 'bootstrap.err.log')}


def install_loader(root: Path, home: Path, uid: int, run=launchctl):
    path = home / 'Library/LaunchAgents' / (LABEL + '.plist')
    expected = loader_definition(root, home)
    if path.exists() and plistlib.loads(path.read_bytes()) != expected:
        raise ValueError('Existing loader differs; use the normal backed-up installer')
    path.parent.mkdir(parents=True, exist_ok=True)
    (home / '.local/var/log/tenant-issue-os').mkdir(parents=True, exist_ok=True)
    path.write_bytes(plistlib.dumps(expected)); path.chmod(0o600)
    target = f'gui/{uid}/{LABEL}'
    if run('print', target).returncode != 0:
        result = run('bootstrap', f'gui/{uid}', str(path))
        if result.returncode or run('print', target).returncode:
            raise RuntimeError('Login loader could not be registered')
    return {'ok': True, 'target': target, 'path': str(path)}


def record_result(home: Path, result: dict):
    state = home / '.local/state/tenant-issue-os'
    stamp = datetime.now(timezone.utc)
    receipt = {'checked_at': stamp.isoformat(), **result}
    receipt['boot_uuid'] = subprocess.check_output(['/usr/sbin/sysctl', '-n', 'kern.bootsessionuuid'], text=True, timeout=5).strip()
    receipt['sleep_stats'] = subprocess.check_output(['/usr/bin/pmset', '-g', 'stats'], text=True, timeout=5).strip()
    receipt['load_average'] = os.getloadavg()
    try:
        with urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=3) as response:
            health = json.load(response)
        receipt['application'] = {
            key: {field: (health.get(key) or {}).get(field) for field in fields}
            for key, fields in {
                'whatsapp_capture': ('state', 'last_cycle_at', 'has_error'),
                'automation': ('state', 'last_cycle_at', 'has_error'),
                'source_quality': ('has_error', 'unreviewed_sources', 'sheet_readback_ok'),
                'nyc311_session': ('state', 'has_error'),
                'nyc311_status': ('state', 'fresh', 'total', 'has_error'),
            }.items()
        }
    except (OSError, ValueError, TimeoutError) as exc:
        receipt['application'] = {'probe_error': type(exc).__name__}
    temporary = state / '.bootstrap-status.tmp'
    temporary.write_text(json.dumps(receipt, indent=2) + '\n'); temporary.chmod(0o600)
    temporary.replace(state / 'bootstrap-status.json')
    # Ongoing metadata-only evidence, independent of a finite 72-hour audit.
    history = state / 'bootstrap-history'; history.mkdir(mode=0o700, exist_ok=True)
    with (history / (stamp.strftime('%Y-%m-%d') + '.jsonl')).open('a') as output:
        output.write(json.dumps(receipt) + '\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--install', action='store_true')
    args = parser.parse_args()
    if sys.platform != 'darwin' or os.geteuid() == 0:
        parser.error('Run as the owning non-root macOS user')
    os.umask(0o077)
    root = Path(__file__).resolve().parents[1]; home = Path.home(); uid = os.getuid()
    state = home / '.local/state/tenant-issue-os'; state.mkdir(parents=True, exist_ok=True)
    if args.install:
        print(json.dumps(install_loader(root, home, uid)))
        return
    with (state / 'bootstrap.lock').open('a+') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        result = ensure_background_services(home, uid)
        record_result(home, result)
        if not result['ok']:
            print(json.dumps(result))
            raise SystemExit(1)


if __name__ == '__main__':
    main()
