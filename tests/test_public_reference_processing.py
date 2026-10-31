import json

from packages.db import FilingJob, RawMessage, get_session
from packages.whatsapp.attachments import build_attachment_manifest, make_attachment_item
from packages.whatsapp.media import public_attachment_entries
from packages.worker_jobs import process_message, process_pending_messages


def test_regular_and_resumed_processing_persist_portable_references(client, monkeypatch, tmp_path):
    monkeypatch.setenv("WHATSAPP_CAPTURE_MEDIA_DIR", str(tmp_path))
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://tenant.example.test")
    photo = tmp_path / "fixture.png"
    photo.write_bytes(b"fixture attachment")
    manifest = build_attachment_manifest(items=[make_attachment_item(
        kind="image", path=photo, status="downloaded", content_type="image/png",
    )])
    with get_session() as session:
        for mid in ("normal-photo", "resumed-photo"):
            session.add(RawMessage(message_id=mid, sender_hash="fixture", text="image omitted", source="zip_import", attachments=manifest))
        session.commit()
    process_message("normal-photo", sync_sheets=False, allow_filing_job=False)
    process_pending_messages(resync_sheets=False)
    with get_session() as session:
        for mid in ("normal-photo", "resumed-photo"):
            raw = session.get(RawMessage, mid)
            assert json.loads(raw.attachments)["items"][0]["public_reference"]["version"] == 1
            assert len(public_attachment_entries(mid, raw.attachments)) == 1
        assert session.query(FilingJob).count() == 0
