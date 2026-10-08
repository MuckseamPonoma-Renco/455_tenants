import json
from pathlib import Path

import pytest

from packages.sheets.public_semantic_overrides import PublicSemanticOverrideError, raw_text_sha256
from scripts.check_runtime_migration import check_runtime_migration


def manifest(path, *, summary="Fictional reviewed summary."):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"schema_version": 1, "overrides": [{
        "message_id": "a" * 64, "raw_text_sha256": raw_text_sha256("Fictional source."),
        "include": True, "issue_label": "Example issue", "category_label": "Example",
        "summary": summary, "show_evidence": False, "reason": "Synthetic test review.",
    }]}))
    return path


@pytest.fixture
def installation(tmp_path):
    runtime = tmp_path / "runtime"
    source = tmp_path / "candidate"
    source.mkdir()
    manifest(runtime / "packages/sheets/public_semantic_overrides.json")
    return runtime, source


def configure(runtime, path=None):
    values = "GOOGLE_PUBLIC_SHEETS_SPREADSHEET_ID=synthetic-public\n"
    if path is not None:
        values += f'TENANT_PUBLIC_SEMANTIC_OVERRIDES_PATH="{path}"\n'
    (runtime / ".env").write_text(values)


def test_existing_corrections_cannot_be_silently_replaced_with_empty_default(installation):
    runtime, source = installation
    configure(runtime)
    with pytest.raises(PublicSemanticOverrideError, match="Preserve existing corrections"):
        check_runtime_migration(runtime, source)


@pytest.mark.parametrize("location", ["external", "runtime_private"])
def test_equivalent_manifest_in_durable_private_location_passes(installation, tmp_path, location):
    runtime, source = installation
    path = runtime / ".local/private/overrides.json" if location == "runtime_private" else tmp_path / "private/overrides.json"
    configure(runtime, manifest(path))
    assert check_runtime_migration(runtime, source) == {"ok": True, "preserved_corrections": 1}


def test_changed_private_corrections_block_installation(installation, tmp_path):
    runtime, source = installation
    configure(runtime, manifest(tmp_path / "private/overrides.json", summary="Changed."))
    with pytest.raises(PublicSemanticOverrideError, match="exactly preserve"):
        check_runtime_migration(runtime, source)


@pytest.mark.parametrize("location", ["candidate", "runtime_source"])
def test_manifest_must_survive_staging_and_checkout_cleanup(installation, location):
    runtime, source = installation
    path = source / ".local/private/overrides.json" if location == "candidate" else runtime / "overrides.json"
    configure(runtime, manifest(path))
    with pytest.raises(PublicSemanticOverrideError, match="source checkout|overwritten"):
        check_runtime_migration(runtime, source)


def test_both_installers_preflight_before_any_source_copy():
    root = Path(__file__).resolve().parents[1]
    for name in ("install_mac_launch_agents.sh", "install_chat_export_sync_launch_agent.sh"):
        script = (root / "scripts" / name).read_text()
        assert script.index('scripts/check_runtime_migration.py') < script.index('rsync -a --delete')
