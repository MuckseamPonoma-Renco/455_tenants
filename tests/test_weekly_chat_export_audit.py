import json
import sys
import pytest

from packages.audit import compute_message_id, sender_hash
from packages.db import MessageDecision, RawMessage, get_session
import scripts.run_weekly_chat_export_audit as weekly_audit


@pytest.mark.parametrize("skip_import", [True, False])
def test_audit_only_keeps_reconciliation_read_only_and_normal_import_applies_it(tmp_path, monkeypatch, skip_import):
    calls = []
    args = ["weekly-audit", "--export", str(tmp_path / "chat.txt"), "--out-dir", str(tmp_path)]
    if skip_import:
        args.append("--skip-import")
    monkeypatch.setattr(sys, "argv", args)
    monkeypatch.setattr(weekly_audit, "import_export", lambda *a, **k: calls.append("import"))
    monkeypatch.setattr(weekly_audit, "retry_incomplete_llm_reviews", lambda *a, **k: calls.append("retry") or {})
    monkeypatch.setattr(weekly_audit, "run_reconciliation", lambda **kwargs: calls.append(("reconcile", kwargs["dry_run"])) or {"dry_run": kwargs["dry_run"]})
    monkeypatch.setattr(weekly_audit, "run_audit", lambda *a, **k: {
        "ok": True, "missing_db_messages": 0, "missing_decisions": 0, "llm_review_complete": True,
    })
    monkeypatch.setattr(weekly_audit, "sync_sheets_after_success", lambda: pytest.fail("Unrequested publication"))

    weekly_audit.main()

    assert ("reconcile", skip_import) in calls
    assert ("import" in calls) is not skip_import
    assert ("retry" in calls) is not skip_import
    result = json.loads((tmp_path / "summary.json").read_text())
    assert result["cross_source_reconciliation"]["dry_run"] is skip_import


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


def test_public_sheet_readback_uses_full_audit_window(monkeypatch):
    calls = []
    monkeypatch.setenv("PUBLIC_TENANT_LOG_AUDIT_DAYS", "365")
    monkeypatch.setattr(
        "scripts.audit_public_tenant_log.run_audit",
        lambda **kwargs: calls.append(kwargs) or {"ok": True},
    )

    assert weekly_audit.verify_public_sheet_readback() == {"ok": True}
    assert calls == [
        {"days": 365, "resync": False, "retries": 3, "retry_sleep": 2.0, "limit": 20}
    ]


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
