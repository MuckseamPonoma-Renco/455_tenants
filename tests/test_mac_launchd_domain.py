from pathlib import Path
import subprocess
import plistlib
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('value,expected', [(None, 'gui'), ('gui\n', 'gui'), ('user\n', 'user')])
def test_service_control_uses_selected_own_user_domain(tmp_path, value, expected):
    if value is not None:
        (tmp_path / 'launchd-domain').write_text(value)
    result = subprocess.run(['bash', '-c', 'source "$1"; MAC_SERVICE_STATE_DIR="$2"; MAC_SERVICE_UID=501; mac_service_service_target automation', 'test', str(ROOT / 'scripts/mac_service_helpers.sh'), str(tmp_path)], capture_output=True, text=True)
    assert result.returncode == 0
    assert result.stdout.strip() == expected + '/501/tenant-issue-os.automation'


@pytest.mark.parametrize('value', ['system', 'user/0', 'user; echo unsafe', ''])
def test_invalid_domain_never_targets_system_or_another_user(tmp_path, value):
    (tmp_path / 'launchd-domain').write_text(value)
    result = subprocess.run(['bash', '-c', 'source "$1"; MAC_SERVICE_STATE_DIR="$2"; mac_service_service_target api', 'test', str(ROOT / 'scripts/mac_service_helpers.sh'), str(tmp_path)], capture_output=True, text=True)
    assert result.returncode != 0
    assert not result.stdout


def fixture_services(home, failure=None):
    directory = home / 'Library/LaunchAgents'; directory.mkdir(parents=True)
    (home / '.local/state/tenant-issue-os').mkdir(parents=True)
    labels = ['tenant-issue-os.keep-awake', 'tenant-issue-os.api', 'tenant-issue-os.watchdog']
    for label in labels:
        (directory / (label + '.plist')).write_bytes(plistlib.dumps({'Label': label, 'ProgramArguments': ['/fixture'], 'RunAtLoad': True}))
    loaded = {f'gui/501/{label}' for label in labels}
    calls = []
    failed = False
    def run(*args):
        nonlocal failed
        calls.append(args)
        if args[0] == 'print':
            return SimpleNamespace(returncode=int(args[1] not in loaded))
        if args[0] == 'bootout':
            loaded.remove(args[1])
            return SimpleNamespace(returncode=0)
        if args[0] == 'bootstrap':
            label = plistlib.loads(Path(args[2]).read_bytes())['Label']
            target = args[1] + '/' + label
            if failure and not failed and target == 'user/501/tenant-issue-os.api':
                failed = True
                if failure == 'partial': loaded.add(target)
                return SimpleNamespace(returncode=1)
            assert target not in loaded
            assert not any(x.endswith('/' + label) for x in loaded), 'duplicate owner'
            loaded.add(target)
            return SimpleNamespace(returncode=0)
        raise AssertionError(args)
    return loaded, calls, run


def test_background_migration_preserves_definitions_and_single_owner(tmp_path):
    from scripts.migrate_mac_background_services import preview, migrate
    loaded, calls, run = fixture_services(tmp_path)
    plan = preview(tmp_path, 501, run)
    result = migrate(plan, tmp_path / 'backup', run)
    assert result['ok']
    assert all(x.startswith('user/501/') for x in loaded)
    assert (tmp_path / '.local/state/tenant-issue-os/launchd-domain').read_text() == 'user\n'
    assert next(x for x in calls if x[0] == 'bootout')[1].endswith('.watchdog')
    for row in plan['services']:
        body = plistlib.loads(Path(row['path']).read_bytes())
        assert body.pop('LimitLoadToSessionType') == 'Background'
        assert body == plistlib.loads(row['before'].encode())


@pytest.mark.parametrize('failure', ['normal', 'partial'])
def test_failed_background_migration_restores_old_owners_and_files(tmp_path, failure):
    from scripts.migrate_mac_background_services import preview, migrate
    loaded, calls, run = fixture_services(tmp_path, failure)
    original = loaded.copy()
    plan = preview(tmp_path, 501, run)
    with pytest.raises(RuntimeError):
        migrate(plan, tmp_path / 'backup', run)
    assert loaded == original
    assert not (tmp_path / '.local/state/tenant-issue-os/launchd-domain').exists()
    assert all(Path(r['path']).read_text() == r['before'] for r in plan['services'])


def test_stale_preview_changes_nothing(tmp_path):
    from scripts.migrate_mac_background_services import preview, migrate
    loaded, calls, run = fixture_services(tmp_path)
    plan = preview(tmp_path, 501, run)
    Path(plan['services'][0]['path']).write_text('modified elsewhere')
    with pytest.raises(ValueError):
        migrate(plan, tmp_path / 'backup', run)
    assert not any(c[0] in ('bootout', 'bootstrap') for c in calls)


def test_failed_live_verification_rolls_back_even_after_successful_bootstrap(tmp_path):
    from scripts.migrate_mac_background_services import preview, migrate
    loaded, calls, run = fixture_services(tmp_path)
    original = loaded.copy()
    plan = preview(tmp_path, 501, run)
    def unhealthy():
        raise RuntimeError('Browser did not authenticate in background domain')
    with pytest.raises(RuntimeError, match='Browser'):
        migrate(plan, tmp_path / 'backup', run, verify=unhealthy)
    assert loaded == original
    assert all(Path(r['path']).read_text() == r['before'] for r in plan['services'])


def test_bootout_waits_for_async_teardown_before_importing_same_label(tmp_path, monkeypatch):
    from scripts import migrate_mac_background_services as migration
    loaded, calls, original = fixture_services(tmp_path)
    pending = {}
    def run(*args):
        if args[0] == 'bootout':
            calls.append(args)
            pending[args[1]] = 2
            return SimpleNamespace(returncode=0)
        if args[0] == 'print' and args[1] in pending:
            pending[args[1]] -= 1
            if pending[args[1]] == 0:
                loaded.remove(args[1]); del pending[args[1]]
        return original(*args)
    monkeypatch.setattr(migration.time, 'sleep', lambda _: None)
    result = migration.migrate(migration.preview(tmp_path, 501, run), tmp_path / 'backup', run)
    assert result['ok'] and all(x.startswith('user/') for x in loaded)
