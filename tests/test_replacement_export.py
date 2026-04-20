import json

from packages.db import Incident, RawMessage, ServiceRequestCase, get_session
from packages.nyc311.replacement_export import export_elevator_replacement_bundle
from packages.whatsapp.attachments import build_attachment_manifest


def test_export_elevator_replacement_bundle_includes_media_and_portal_screenshots(client, monkeypatch, tmp_path):
    export_dir = tmp_path / "exports"
    screenshot_dir = tmp_path / "portal_shots"
    screenshot_dir.mkdir()
    portal_shot = screenshot_dir / "20260420_review.png"
    portal_shot.write_text("shot", encoding="utf-8")

    media_file = tmp_path / "evidence.png"
    media_file.write_text("media", encoding="utf-8")

    monkeypatch.setenv("EXPORT_DIR", str(export_dir))
    monkeypatch.setenv("NYC311_PORTAL_SCREENSHOT_DIR", str(screenshot_dir))

    with get_session() as session:
        session.add(
            Incident(
                incident_id="inc-elevator",
                category="elevator",
                asset="elevator_north",
                status="open",
                severity=4,
                start_ts="2026-04-20T00:40:00Z",
                start_ts_epoch=1776645600,
                end_ts=None,
                end_ts_epoch=None,
                last_ts_epoch=1776646020,
                title="North elevator still broken",
                summary="Repeated outages continue.",
                proof_refs="raw-proof",
                report_count=2,
                witness_count=2,
                confidence=90,
                needs_review=False,
                updated_at="2026-04-20T00:48:00Z",
            )
        )
        session.add(
            RawMessage(
                message_id="raw-proof",
                chat_name="455 Tenants",
                sender="Max",
                sender_hash="hash-max",
                ts_iso="2026-04-20T00:47:00Z",
                ts_epoch=1776646020,
                text="Here is the latest elevator photo",
                attachments=build_attachment_manifest(
                    items=[{"kind": "image", "status": "downloaded", "path": str(media_file)}],
                    source="whatsapp_web",
                ),
                source="whatsapp_web",
            )
        )
        session.add(
            ServiceRequestCase(
                service_request_number="311-99990000",
                incident_id="inc-elevator",
                filing_job_id=None,
                source="portal_playwright",
                complaint_type="Elevator or Escalator Complaint",
                status="submitted",
                agency="DOB",
                submitted_at="2026-04-20T01:00:00Z",
                last_checked_at=None,
                closed_at=None,
                resolution_description=None,
                raw_status_json=None,
            )
        )
        session.commit()

    with get_session() as session:
        result = export_elevator_replacement_bundle(session)

    assert result["incidents"] == 1
    assert result["cases"] == 1
    assert result["media_files"] == 1
    assert result["portal_screenshots"] == 1

    bundle_json = json.loads(open(result["json"], encoding="utf-8").read())
    assert bundle_json["portal_screenshots"] == [str(portal_shot.resolve())]
    assert bundle_json["incidents"][0]["messages"][0]["attachment_paths"] == [str(media_file)]
    assert "311-99990000" in open(result["markdown"], encoding="utf-8").read()
