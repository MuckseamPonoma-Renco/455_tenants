import sys

import scripts.run_weekly_chat_export_audit as weekly_audit


def test_weekly_zip_import_requests_all_message_model_review(tmp_path, monkeypatch):
    export_path = tmp_path / "WhatsApp Chat - 455 Tenants.zip"
    calls = []

    def fake_run(command, *, cwd, check):
        calls.append((command, cwd, check))

    monkeypatch.setattr(weekly_audit.subprocess, "run", fake_run)

    weekly_audit.import_export(export_path, llm_mode="all")

    assert calls == [
        (
            [
                sys.executable,
                str(weekly_audit.ROOT / "scripts" / "import_whatsapp_zip.py"),
                "--zip",
                str(export_path),
                "--llm-mode",
                "all",
            ],
            weekly_audit.ROOT,
            True,
        )
    ]


def test_weekly_text_import_requests_all_message_model_review(tmp_path, monkeypatch):
    export_path = tmp_path / "WhatsApp Chat - 455 Tenants.txt"
    calls = []

    def fake_run(command, *, cwd, check):
        calls.append((command, cwd, check))

    monkeypatch.setattr(weekly_audit.subprocess, "run", fake_run)

    weekly_audit.import_export(export_path, llm_mode="all")

    assert calls == [
        (
            [
                sys.executable,
                str(weekly_audit.ROOT / "scripts" / "import_whatsapp_export.py"),
                str(export_path),
                "--llm-mode",
                "all",
            ],
            weekly_audit.ROOT,
            True,
        )
    ]
