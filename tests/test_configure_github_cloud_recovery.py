import json

import pytest

from scripts.configure_github_cloud_recovery import (
    build_recovery_env,
    google_credentials,
)


def test_recovery_env_is_allowlisted_and_requires_model_access():
    values = {
        "DATABASE_URL": "postgresql://db",
        "CLOUD_EXPORT_RECEIVER_URL": "https://uploads.example.test",
        "CLOUD_EXPORT_RECEIVER_PULL_TOKEN": "pull-token",
        "GOOGLE_SHEETS_SPREADSHEET_ID": "sheet-id",
        "OPENAI_API_KEY": "model-key",
        "INGEST_TOKEN": "must-not-leave-the-mac",
        "MOBILE_FILER_TOKEN": "must-not-leave-the-mac",
        "NYC311_CONTACT_EMAIL": "must-not-leave-the-mac@example.test",
    }

    rendered = build_recovery_env(values)

    assert "DATABASE_URL=postgresql://db" in rendered
    assert "OPENAI_API_KEY=model-key" in rendered
    assert "INGEST_TOKEN" not in rendered
    assert "MOBILE_FILER_TOKEN" not in rendered
    assert "NYC311_CONTACT_EMAIL" not in rendered

    del values["OPENAI_API_KEY"]
    with pytest.raises(ValueError, match="OPENAI_API_KEY or LLM_API_KEY"):
        build_recovery_env(values)


def test_google_credentials_requires_service_account_json(tmp_path):
    path = tmp_path / "service-account.json"
    path.write_text(json.dumps({"type": "service_account", "client_email": "robot@example.test"}), encoding="utf-8")

    compact = google_credentials({"GOOGLE_APPLICATION_CREDENTIALS": str(path)})
    assert json.loads(compact)["type"] == "service_account"

    path.write_text(json.dumps({"type": "authorized_user"}), encoding="utf-8")
    with pytest.raises(ValueError, match="service-account"):
        google_credentials({"GOOGLE_APPLICATION_CREDENTIALS": str(path)})
