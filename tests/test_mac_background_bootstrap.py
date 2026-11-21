from pathlib import Path
import plistlib
import subprocess
import sys
from types import SimpleNamespace

import pytest

from scripts import bootstrap_mac_background_services as bootstrap


def setup(home, *, missing=False):
    state = home / '.local/state/tenant-issue-os'; state.mkdir(parents=True)
    (state / 'launchd-domain').write_text('user\n')
    folder = home / 'Library/LaunchAgents'; folder.mkdir(parents=True)
    if not missing:
        for name in bootstrap.NAMES:
            label = 'tenant-issue-os.' + name
            (folder / (label + '.plist')).write_bytes(plistlib.dumps({
                'Label': label, 'LimitLoadToSessionType': 'Background', 'ProgramArguments': ['/fixture']}))
    loaded = set(); calls = []; fail = set()
    def run(*args):
        calls.append(args)
        if args[0] == 'print': return SimpleNamespace(returncode=int(args[1] not in loaded))
        assert args[0] == 'bootstrap', 'Loader must not stop or kickstart active services'
        label = plistlib.loads(Path(args[2]).read_bytes())['Label']
        target = args[1] + '/' + label
        assert not any(v.endswith('/'+label) for v in loaded), 'duplicate owner'
        if label in fail: return SimpleNamespace(returncode=5)
        loaded.add(target)
        return SimpleNamespace(returncode=0)
    return state, folder, loaded, calls, fail, run


def test_fresh_login_loads_all_missing_background_jobs_and_next_run_is_noop(tmp_path):
    _, _, loaded, calls, _, run = setup(tmp_path)
    first = bootstrap.ensure_background_services(tmp_path, 501, run)
    assert first['ok'] and len(loaded) == len(bootstrap.NAMES)
    assert all(v.startswith('user/501/') for v in loaded)
    assert all(row['action'] == 'loaded' for row in first['services'])
    count = len([x for x in calls if x[0] == 'bootstrap'])
    second = bootstrap.ensure_background_services(tmp_path, 501, run)
    assert second['ok'] and all(r['action'] == 'already_loaded' for r in second['services'])
    assert len([x for x in calls if x[0] == 'bootstrap']) == count


def test_loader_never_duplicates_gui_owned_worker(tmp_path):
    _, _, loaded, _, _, run = setup(tmp_path)
    loaded.add('gui/501/tenant-issue-os.automation')
    result = bootstrap.ensure_background_services(tmp_path, 501, run)
    assert not result['ok']
    assert 'user/501/tenant-issue-os.automation' not in loaded
    assert next(r for r in result['services'] if r['label'].endswith('.automation'))['action'] == 'gui_owner_conflict'


def test_failed_bootstrap_is_retried_without_restarting_successful_services(tmp_path):
    _, _, loaded, calls, fail, run = setup(tmp_path)
    fail.add('tenant-issue-os.api')
    assert not bootstrap.ensure_background_services(tmp_path, 501, run)['ok']
    calls.clear();fail.clear()
    assert bootstrap.ensure_background_services(tmp_path, 501, run)['ok']
    assert [Path(c[2]).name for c in calls if c[0] == 'bootstrap'] == ['tenant-issue-os.api.plist']


@pytest.mark.parametrize('case', ['maintenance', 'legacy'])
def test_maintenance_and_legacy_domains_make_no_service_changes(tmp_path, case):
    state, _, _, calls, _, run = setup(tmp_path)
    if case == 'maintenance': (state / 'install.lock').mkdir()
    else: (state / 'launchd-domain').write_text('gui\n')
    assert bootstrap.ensure_background_services(tmp_path, 501, run)['ok']
    assert not calls


def test_invalid_definition_does_not_block_independent_service_recovery(tmp_path):
    _, folder, loaded, _, _, run = setup(tmp_path)
    (folder / 'tenant-issue-os.api.plist').write_text('invalid plist')
    result = bootstrap.ensure_background_services(tmp_path, 501, run)
    assert not result['ok'] and 'user/501/tenant-issue-os.watchdog' in loaded
    assert 'user/501/tenant-issue-os.api' not in loaded


def test_no_definitions_is_not_a_healthy_bootstrap(tmp_path):
    _, _, _, _, _, run = setup(tmp_path, missing=True)
    assert not bootstrap.ensure_background_services(tmp_path, 501, run)['ok']


def test_login_loader_is_aqua_runatload_and_periodic(tmp_path):
    _, _, loaded, calls, _, run = setup(tmp_path)
    root = tmp_path / 'runtime'
    result = bootstrap.install_loader(root, tmp_path, 501, run)
    assert result['target'] == 'gui/501/tenant-issue-os.bootstrap'
    body = plistlib.loads(Path(result['path']).read_bytes())
    assert body['LimitLoadToSessionType'] == 'Aqua'
    assert body['RunAtLoad'] and body['StartInterval'] == 60
    assert body == bootstrap.loader_definition(root, tmp_path)
    calls.clear()
    bootstrap.install_loader(root, tmp_path, 501, run)
    assert not any(c[0]=='bootstrap' for c in calls)


@pytest.mark.parametrize('name,session_type', [('bootstrap', 'Aqua'), ('api', 'Background')])
def test_normal_installer_preserves_distinct_loader_and_worker_session_types(tmp_path, name, session_type):
    root = Path(__file__).resolve().parents[1]
    script = (root / 'scripts/install_mac_launch_agents.sh').read_text()
    render = script.split('render_template() {', 1)[1].split('\nbootstrap_with_retry()', 1)[0]
    code = render.split("<<'PY'\n", 1)[1].split('\nPY\n', 1)[0]
    destination = tmp_path / 'rendered.plist'
    subprocess.run([sys.executable, '-', str(root / 'launchd' / ('tenant-issue-os.'+name+'.plist.template')),
                    str(destination), '/fixture/runtime', '/fixture/logs', 'user'], input=code, text=True, check=True)
    assert plistlib.loads(destination.read_bytes())['LimitLoadToSessionType'] == session_type
