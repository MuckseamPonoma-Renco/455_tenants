import csv
import zipfile
from pathlib import Path

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
