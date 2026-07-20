import csv
import zipfile
from pathlib import Path

from packages.audit import compute_message_id, sender_hash
from packages.db import MessageDecision, RawMessage, get_session
from packages.whatsapp.export import parse_export_path
from scripts.audit_whatsapp_export_decisions import iter_export_messages, run_audit


def test_iter_export_messages_reads_all_txt_files_in_zip(tmp_path):
    export_path = tmp_path / "all_chats.zip"
    with zipfile.ZipFile(export_path, "w") as archive:
        archive.writestr("WhatsApp Chat - 455 Tenants.txt", "[6/5/26, 9:00:00 AM] Karen: North lift dead\n")
        archive.writestr("notes.txt", "not a chat export\n")

    messages = iter_export_messages(export_path)

    assert len(messages) == 1
    assert messages[0].chat_name == "455 Tenants"
    assert messages[0].text == "North lift dead"


def test_parse_export_path_reads_zip_entries_without_loading_the_archive_bytes(tmp_path, monkeypatch):
    export_path = tmp_path / "all_chats.zip"
    with zipfile.ZipFile(export_path, "w") as archive:
        archive.writestr("WhatsApp Chat - 455 Tenants.txt", "[6/5/26, 9:00:00 AM] Karen: North lift dead\n")
        archive.writestr("evidence/video.mp4", b"x" * 1024)

    monkeypatch.setattr(Path, "read_bytes", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not read the whole ZIP")))

    parsed = parse_export_path(export_path)

    assert parsed.is_zip is True
    assert len(parsed.messages) == 1
    assert parsed.messages[0].text == "North lift dead"


def test_run_audit_creates_review_roster_for_missing_and_review_rows(client, tmp_path):
    response = client.post(
        "/ingest/tasker",
        headers={"Authorization": "Bearer test-token"},
        json={
            "chat_name": "455 Tenants",
            "text": "Any update on the elevator?",
            "sender": "Karen",
            "ts_epoch": 1780668000,
        },
    )
    assert response.status_code == 200, response.text

    export_path = tmp_path / "WhatsApp Chat - 455 Tenants.txt"
    export_path.write_text(
        "[6/5/26, 9:00:00 AM] Karen: Any update on the elevator?\n"
        "[6/5/26, 9:05:00 AM] Molly: South lift dead\n",
        encoding="utf-8",
    )
    out_dir = tmp_path / "audit"

    summary = run_audit(export_path, since="2026-06-05", out_dir=out_dir)

    assert summary["audited_messages"] == 2
    assert summary["review_roster_rows"] == 1

    with (out_dir / "review_roster.csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert rows[0]["text"] == "South lift dead"
    assert rows[0]["suspect_reasons"] == "no_db_match"


def test_run_audit_does_not_roster_deterministic_context_followups(client, tmp_path, monkeypatch):
    monkeypatch.setattr("packages.incident.extractor.LLM_MODE", "off")

    first = client.post(
        "/ingest/whatsapp_web",
        headers={"Authorization": "Bearer test-token"},
        json={
            "chat_name": "455 Tenants",
            "text": "South elevator not working.",
            "sender": "Molly",
            "ts_epoch": 1781262566,
        },
    )
    second = client.post(
        "/ingest/whatsapp_web",
        headers={"Authorization": "Bearer test-token"},
        json={
            "chat_name": "455 Tenants",
            "text": "Mechanic called. Stuck on pool level.",
            "sender": "Karen",
            "ts_epoch": 1781262741,
        },
    )
    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text

    export_path = tmp_path / "WhatsApp Chat - 455 Tenants.txt"
    export_path.write_text(
        "[6/12/26, 7:09:26 AM] Molly: South elevator not working.\n"
        "[6/12/26, 7:12:21 AM] Karen: Mechanic called. Stuck on pool level.\n",
        encoding="utf-8",
    )

    summary = run_audit(export_path, since="2026-06-05", out_dir=tmp_path / "audit")

    assert summary["review_roster_rows"] == 0


def test_run_audit_prefers_live_capture_and_rosters_conflicting_archive_decision(client, tmp_path):
    text = "A resident reports the lobby door is too heavy to open when the doorman is away."
    export_path = tmp_path / "WhatsApp Chat - 455 Tenants.txt"
    export_path.write_text(
        f"[7/13/26, 10:04:37 AM] ~ Millie: {text}\n",
        encoding="utf-8",
    )
    export_message = iter_export_messages(export_path)[0]
    archive_id = compute_message_id(
        export_message.chat_name,
        export_message.sender,
        export_message.ts_iso or "",
        export_message.text,
    )
    live_id = "live-alias"

    with get_session() as session:
        session.add_all([
            RawMessage(
                message_id=archive_id,
                chat_name=export_message.chat_name,
                sender=export_message.sender,
                sender_hash=sender_hash(export_message.sender),
                ts_iso=export_message.ts_iso,
                ts_epoch=export_message.ts_epoch,
                text=text,
                source="zip_import",
            ),
            RawMessage(
                message_id=live_id,
                chat_name="455 Tenants",
                sender="Unknown sender",
                sender_hash=sender_hash("Unknown sender"),
                ts_epoch=int(export_message.ts_epoch or 0) - 37,
                text=text,
                source="whatsapp_web",
            ),
            MessageDecision(
                message_id=archive_id,
                chosen_source="rules",
                is_issue=False,
                category=None,
                event_type=None,
                needs_review=False,
            ),
            MessageDecision(
                message_id=live_id,
                chosen_source="llm",
                is_issue=True,
                category="security_access",
                event_type="new_issue",
                confidence=90,
                needs_review=True,
            ),
        ])
        session.commit()

    out_dir = tmp_path / "audit"
    summary = run_audit(export_path, since="2026-06-05", out_dir=out_dir)

    assert summary["matched_messages"] == 1
    assert summary["review_roster_rows"] == 1
    assert summary["reason_counts"]["cross_source_decision_conflict"] == 1
    with (out_dir / "review_roster.csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert rows[0]["matched_message_id"] == live_id
    assert rows[0]["match_method"] == "cross_source"
    assert rows[0]["stored_source"] == "whatsapp_web"
    assert rows[0]["cross_source_message_id"] == archive_id
    assert rows[0]["cross_source_source"] == "zip_import"
    assert rows[0]["suspect_reasons"] == "cross_source_decision_conflict;needs_review"
