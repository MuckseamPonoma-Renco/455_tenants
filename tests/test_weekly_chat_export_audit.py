import json
import sys

from packages.audit import compute_message_id, sender_hash
from packages.db import MessageDecision, RawMessage, get_session
import scripts.run_weekly_chat_export_audit as weekly_audit


def test_weekly_zip_import_requests_all_message_model_review(tmp_path, monkeypatch):
    export_path = tmp_path / "WhatsApp Chat - 455 Tenants.zip"
    calls = []

    def fake_run(command, *, cwd, check, env):
        calls.append((command, cwd, check, env["AUTO_FILE_ENABLED"], env["DISABLE_SHEETS_SYNC"]))

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
            "0",
            "1",
        )
    ]


def test_weekly_text_import_requests_all_message_model_review(tmp_path, monkeypatch):
    export_path = tmp_path / "WhatsApp Chat - 455 Tenants.txt"
    calls = []

    def fake_run(command, *, cwd, check, env):
        calls.append((command, cwd, check, env["AUTO_FILE_ENABLED"], env["DISABLE_SHEETS_SYNC"]))

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
            "0",
            "1",
        )
    ]


def test_post_audit_sheet_sync_forces_enabled_then_restores_environment(monkeypatch):
    calls = []
    monkeypatch.setenv("DISABLE_SHEETS_SYNC", "1")
    monkeypatch.setattr(
        "packages.worker_jobs.sync_all_sheets",
        lambda: calls.append(__import__("os").environ["DISABLE_SHEETS_SYNC"]),
    )

    weekly_audit.sync_sheets_after_success()

    assert calls == ["0"]
    assert __import__("os").environ["DISABLE_SHEETS_SYNC"] == "1"


def test_retry_incomplete_reviews_reprocesses_only_unreviewed_messages(client, tmp_path, monkeypatch):
    export_path = tmp_path / "WhatsApp Chat - 455 Tenants.txt"
    export_path.write_text(
        "[6/5/26, 9:00:00 AM] Karen: First message\n"
        "[6/5/26, 9:01:00 AM] Karen: Second message\n",
        encoding="utf-8",
    )
    messages = weekly_audit.iter_export_messages(export_path)
    message_ids = [
        compute_message_id(message.chat_name, message.sender, message.ts_iso or "", message.text)
        for message in messages
    ]
    with get_session() as session:
        for message, message_id in zip(messages, message_ids):
            session.add(
                RawMessage(
                    message_id=message_id,
                    chat_name=message.chat_name,
                    sender=message.sender,
                    sender_hash=sender_hash(message.sender),
                    ts_iso=message.ts_iso,
                    ts_epoch=message.ts_epoch,
                    text=message.text,
                    source="zip_import",
                )
            )
        session.add(
            MessageDecision(
                message_id=message_ids[0],
                chosen_source="llm",
                llm_json=json.dumps({"review_status": "completed", "confidence": 90}),
            )
        )
        session.add(MessageDecision(message_id=message_ids[1], chosen_source="none", llm_json="{}"))
        session.commit()

    calls = []

    def fake_classify(session, raw, *, allow_filing_job):
        assert allow_filing_job is False
        calls.append(raw.message_id)
        decision = session.get(MessageDecision, raw.message_id)
        decision.llm_json = json.dumps({"review_status": "completed", "confidence": 91})
        session.flush()
        return ""

    monkeypatch.setattr("packages.incident.extractor.classify_and_upsert_incident", fake_classify)

    result = weekly_audit.retry_incomplete_llm_reviews(export_path, since="2026-06-05", llm_mode="all")

    assert result["pending_before"] == 1
    assert result["completed"] == 1
    assert result["failed"] == 0
    assert calls == [message_ids[1]]
