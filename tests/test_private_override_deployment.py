import json
from pathlib import Path

import pytest

from packages.sheets.public_semantic_overrides import (
    DEFAULT_OVERRIDE_PATH, PublicSemanticOverrideError,
    require_private_overrides_for_publication,
)
from scripts.materialize_private_overrides import encode_private_manifest, materialize_private_manifest
from scripts.run_cloud_recovery_cycle import config_errors


def test_live_publication_requires_explicit_valid_private_manifest(tmp_path):
    values = {"GOOGLE_PUBLIC_SHEETS_SPREADSHEET_ID": "fictional-public", "DISABLE_SHEETS_SYNC": "0"}
    with pytest.raises(PublicSemanticOverrideError, match="required before publishing"):
        require_private_overrides_for_publication(values)
    values["TENANT_PUBLIC_SEMANTIC_OVERRIDES_PATH"] = str(tmp_path / "missing.json")
    with pytest.raises(PublicSemanticOverrideError, match="unable to load"):
        require_private_overrides_for_publication(values)
    values["TENANT_PUBLIC_SEMANTIC_OVERRIDES_PATH"] = str(DEFAULT_OVERRIDE_PATH)
    with pytest.raises(PublicSemanticOverrideError, match="outside the bundled default"):
        require_private_overrides_for_publication(values)
    private = tmp_path / "explicit-empty.json"
    private.write_text('{"schema_version":1,"overrides":[]}')
    values["TENANT_PUBLIC_SEMANTIC_OVERRIDES_PATH"] = str(private)
    require_private_overrides_for_publication(values)


def test_offline_examples_do_not_require_private_deployment_records():
    require_private_overrides_for_publication({})
    require_private_overrides_for_publication({"GOOGLE_PUBLIC_SHEETS_SPREADSHEET_ID": "fixture", "DISABLE_SHEETS_SYNC": "1"})


def test_public_sync_guard_runs_before_spreadsheet_writes(monkeypatch):
    from packages.sheets import sync
    monkeypatch.setenv("DISABLE_SHEETS_SYNC", "0")
    monkeypatch.setenv("GOOGLE_PUBLIC_SHEETS_SPREADSHEET_ID", "fixture-public")
    monkeypatch.delenv("TENANT_PUBLIC_SEMANTIC_OVERRIDES_PATH", raising=False)
    monkeypatch.setattr(sync, "_service", lambda: object())
    monkeypatch.setattr(sync, "_set_spreadsheet_title", lambda *args: pytest.fail("publication before correction preflight"))
    with pytest.raises(PublicSemanticOverrideError):
        sync.sync_public_updates_to_sheets()


def test_cloud_configuration_rejects_missing_private_publication_data():
    errors = config_errors({"GOOGLE_PUBLIC_SHEETS_SPREADSHEET_ID": "fixture-public"})
    assert any("TENANT_PUBLIC_SEMANTIC_OVERRIDES_PATH" in error for error in errors)


def test_private_secret_round_trip_preserves_bytes_and_permissions(tmp_path):
    original = tmp_path / "original.json"
    original.write_text(json.dumps({"schema_version": 1, "overrides": []}, indent=2))
    output = tmp_path / "materialized.json"
    materialize_private_manifest(encode_private_manifest(original), output)
    assert output.read_bytes() == original.read_bytes()
    assert output.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize("encoded", ["", "not base64", "e30="])
def test_invalid_private_secret_does_not_replace_existing_file(tmp_path, encoded):
    output = tmp_path / "preserved.json"
    output.write_text("unchanged")
    with pytest.raises(Exception):
        materialize_private_manifest(encoded, output)
    assert output.read_text() == "unchanged"


def test_cloud_workflow_materializes_private_data_before_running_recovery():
    text = (Path(__file__).resolve().parents[1] / ".github/workflows/cloud-recovery.yml").read_text()
    assert "secrets.CLOUD_RECOVERY_SEMANTIC_OVERRIDES_GZIP_BASE64" in text
    assert text.index("scripts/materialize_private_overrides.py") < text.index("python scripts/run_cloud_recovery_cycle.py")
    assert 'export TENANT_PUBLIC_SEMANTIC_OVERRIDES_PATH="$overrides_file"' in text


def test_cloud_configuration_preview_never_writes_secrets(tmp_path, monkeypatch, capsys):
    from scripts import configure_github_cloud_recovery as configure
    credentials = tmp_path / "credentials.json"
    credentials.write_text('{"type":"service_account"}')
    manifest = tmp_path / "corrections.json"
    manifest.write_text('{"schema_version":1,"overrides":[]}')
    env = tmp_path / ".env"
    env.write_text("\n".join([
        "DATABASE_URL=sqlite:///:memory:", "OPENAI_API_KEY=fictional-key",
        "CLOUD_EXPORT_RECEIVER_URL=https://example.test",
        "CLOUD_EXPORT_RECEIVER_PULL_TOKEN=fictional-token",
        "GOOGLE_SHEETS_SPREADSHEET_ID=fictional-operator",
        f"GOOGLE_APPLICATION_CREDENTIALS={credentials}",
        f"TENANT_PUBLIC_SEMANTIC_OVERRIDES_PATH={manifest}",
    ]))
    monkeypatch.setattr("sys.argv", ["configure", "--env-file", str(env)])
    monkeypatch.setattr(configure, "_run_gh", lambda *args, **kwargs: pytest.fail("Preview attempted external write"))
    assert configure.main() == 0
    output = capsys.readouterr().out
    assert json.loads(output)["action"] == "preview"
    assert "fictional-key" not in output and "fictional-token" not in output


def test_private_corrections_are_provisioned_before_cloud_activation(monkeypatch):
    from scripts import configure_github_cloud_recovery as configure
    calls = []
    monkeypatch.setattr(configure, "_run_gh", lambda args, **kwargs: calls.append(args))
    configure.apply_configuration(repo="example/project", recovery_env="fixture", credentials_json="{}", overrides_secret="fixture")
    names = [args[2] for args in calls]
    assert names.index("CLOUD_RECOVERY_SEMANTIC_OVERRIDES_GZIP_BASE64") < names.index("CLOUD_RECOVERY_ENABLED")
    assert names[-1] == "CLOUD_RECOVERY_ENABLED"
