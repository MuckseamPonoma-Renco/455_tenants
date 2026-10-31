import sys
import zipfile

import pytest

from packages.audit import sender_hash
from packages.db import FilingJob, MessageDecision, RawMessage, get_session
from packages.timeutil import parse_ts_to_epoch
from packages import queue
from apps.api.routers import ingest
from scripts import import_whatsapp_zip
from scripts import import_whatsapp_export
from scripts.audit_whatsapp_export_decisions import _match_export_message, iter_export_messages


CHAT_NAME = "WhatsApp Chat - Test Building.txt"
CHAT = (
    "[10/30/26, 7:22:47 AM] Resident: The north elevator is stuck on 17.\n"
    "[10/30/26, 7:22:48 AM] Resident: image omitted\n"
    "[10/30/26, 7:22:48 AM] Resident: image omitted\n"
)


def _importer(client, tmp_path, monkeypatch, kind, chat=CHAT):
    if kind == "api":
        def run():
            return client.post("/ingest/export", headers={"Authorization": "Bearer test-token"},
                               files={"file": (CHAT_NAME, chat.encode(), "text/plain")})
        return run, ingest, "enqueue_process_export_message"
    path = tmp_path / "export.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(CHAT_NAME, chat)
    monkeypatch.setattr(sys, "argv", ["import", "--zip", str(path), "--llm-mode", "off"])
    return import_whatsapp_zip.main, import_whatsapp_zip, "enqueue_process_message"


@pytest.mark.parametrize("kind", ["api", "zip"])
def test_export_retry_resumes_only_missing_decisions_after_interruption(client, tmp_path, monkeypatch, kind):
    run, module, enqueue_name = _importer(client, tmp_path, monkeypatch, kind)
    original = getattr(module, enqueue_name)
    completed = []

    def interrupted(mid, *, sync_sheets):
        if completed:
            raise RuntimeError("simulated process interruption after raw commit")
        result = original(mid, sync_sheets=sync_sheets)
        completed.append(mid)
        return result

    monkeypatch.setattr(module, enqueue_name, interrupted)
    with pytest.raises(RuntimeError, match="simulated process interruption"):
        run()
    with get_session() as session:
        assert session.query(RawMessage).count() == 3
        assert session.query(MessageDecision).count() == 1
        assert session.query(FilingJob).count() == 0

    resumed = []

    def recording(mid, *, sync_sheets):
        resumed.append(mid)
        return original(mid, sync_sheets=sync_sheets)

    monkeypatch.setattr(module, enqueue_name, recording)
    run()
    assert len(resumed) == 2
    assert completed[0] not in resumed
    with get_session() as session:
        assert session.query(RawMessage).count() == 3
        assert session.query(MessageDecision).count() == 3
        assert session.query(FilingJob).count() == 0
        assert any(row.message_id.endswith("~2") for row in session.query(RawMessage))
    resumed.clear()
    run()
    assert resumed == []


@pytest.mark.parametrize("kind", ["api", "zip"])
def test_export_resume_of_live_source_alias_never_creates_filing(client, tmp_path, monkeypatch, kind):
    chat = "[10/30/26, 7:22:47 AM] Resident: The north elevator is stuck on 17.\n"
    with get_session() as session:
        session.add(RawMessage(message_id="live-canonical-message", chat_name="Test Building", sender="Resident",
                              sender_hash=sender_hash("Resident"), ts_epoch=parse_ts_to_epoch("10/30/26 7:22:47 AM"),
                              ts_iso="2026-10-30T11:22:47Z", text="The north elevator is stuck on 17.",
                              source="whatsapp_web"))
        session.commit()
    run, _, _ = _importer(client, tmp_path, monkeypatch, kind, chat)
    run()
    with get_session() as session:
        assert session.query(RawMessage).count() == 1
        assert session.get(MessageDecision, "live-canonical-message") is not None
        assert session.query(FilingJob).count() == 0


def test_queued_export_resume_carries_no_filing_constraint(monkeypatch):
    calls = []

    class FakeQueue:
        def enqueue(self, function, *args, **kwargs):
            calls.append((function.__name__, args, kwargs))
            return type("Job", (), {"id": "queued-export"})()

    monkeypatch.setattr(queue, "_queue_or_none", lambda: FakeQueue())
    assert queue.enqueue_process_export_message("missing-decision", sync_sheets=False) == "queued-export"
    assert calls == [("process_message", ("missing-decision",), {"sync_sheets": False, "allow_filing_job": False})]


@pytest.mark.parametrize(("status", "payload"), [(403, {"detail": "denied"}), (200, {"ok": False})])
def test_text_import_cli_propagates_failed_api_result(tmp_path, monkeypatch, status, payload):
    path = tmp_path / CHAT_NAME
    path.write_text(CHAT)
    monkeypatch.setattr(sys, "argv", ["import", str(path), "--llm-mode", "off"])

    class FakeClient:
        def __init__(self, app):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def post(self, *args, **kwargs):
            return type("Response", (), {"status_code": status, "json": lambda self: payload})()

    monkeypatch.setattr(import_whatsapp_export, "TestClient", FakeClient)
    with pytest.raises(SystemExit) as error:
        import_whatsapp_export.main()
    assert error.value.code == 1


@pytest.mark.parametrize("kind", ["api", "zip"])
def test_archive_duplicate_at_distinct_second_is_preserved_and_audit_detects_loss(client, tmp_path, monkeypatch, kind):
    chat = (
        "[10/30/26, 7:22:47 AM] Resident: Cloudy water video omitted\n"
        "[10/30/26, 7:22:48 AM] Resident: Cloudy water video omitted\n"
    )
    path = tmp_path / CHAT_NAME
    path.write_text(chat)
    first, second = iter_export_messages(path)
    with get_session() as session:
        session.add(RawMessage(message_id=second.message_id, chat_name=second.chat_name, sender=second.sender,
                              sender_hash=sender_hash(second.sender), ts_epoch=second.ts_epoch,
                              ts_iso=second.ts_iso, text=second.text, source="zip_import"))
        session.commit()
        assert _match_export_message(session, first)[0] is None
    run, _, _ = _importer(client, tmp_path, monkeypatch, kind, chat)
    run()
    with get_session() as session:
        assert session.query(RawMessage).count() == 2
        assert _match_export_message(session, first)[0].message_id == first.message_id
        assert _match_export_message(session, second)[0].message_id == second.message_id
        assert session.query(MessageDecision).count() == 2
        assert session.query(FilingJob).count() == 0


@pytest.mark.parametrize("kind", ["api", "zip"])
def test_unique_legacy_archive_one_second_alias_is_retained(client, tmp_path, monkeypatch, kind):
    chat = "[10/30/26, 7:22:48 AM] Resident: Cloudy water video omitted\n"
    path = tmp_path / CHAT_NAME
    path.write_text(chat)
    message = iter_export_messages(path)[0]
    with get_session() as session:
        session.add(RawMessage(message_id="legacy-one-second-alias", chat_name=message.chat_name,
                              sender=message.sender, sender_hash=sender_hash(message.sender),
                              ts_epoch=message.ts_epoch - 1, ts_iso="10/30/26 7:22:47 AM",
                              text=message.text, source="zip_import"))
        session.commit()
        assert _match_export_message(session, message)[0].message_id == "legacy-one-second-alias"
    run, _, _ = _importer(client, tmp_path, monkeypatch, kind, chat)
    run()
    with get_session() as session:
        assert session.query(RawMessage).count() == 1
        assert session.get(MessageDecision, "legacy-one-second-alias") is not None
        assert session.query(FilingJob).count() == 0
