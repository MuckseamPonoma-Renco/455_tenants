import json
from argparse import Namespace

from scripts import cleanup_test_artifacts as cleanup_script
from packages.db import MessageDecision, RawMessage, get_session


def test_cleanup_by_chat_name_removes_only_targeted_rows(client, monkeypatch, tmp_path):
    state_path = tmp_path / "capture_state.json"
    state_path.write_text(
        json.dumps(
            {
                "seen_entries": [
                    {"chat_name": "General", "fingerprint": "general-1"},
                    {"chat_name": "Real Chat", "fingerprint": "real-1"},
                ],
                "primed_chats": ["General", "Real Chat"],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("WHATSAPP_CAPTURE_STATE_PATH", str(state_path))
    monkeypatch.setattr(cleanup_script, "_stop_service", lambda name: {"was_running": False, "was_launchd_loaded": False})
    monkeypatch.setattr(cleanup_script, "_restart_service", lambda name, prior_state: None)
    resync_calls = []
    monkeypatch.setattr(cleanup_script, "full_resync_sheets", lambda: resync_calls.append("resynced"))

    with get_session() as session:
        session.add_all(
            [
                RawMessage(
                    message_id="general-raw",
                    chat_name="General",
                    sender="Max",
                    sender_hash="general-hash",
                    ts_iso="2026-04-20T00:47:00Z",
                    ts_epoch=1776646020,
                    text="Hello all",
                    attachments=None,
                    source="whatsapp_web",
                ),
                RawMessage(
                    message_id="real-raw",
                    chat_name="Real Chat",
                    sender="Max",
                    sender_hash="real-hash",
                    ts_iso="2026-04-20T00:48:00Z",
                    ts_epoch=1776646080,
                    text="Real message",
                    attachments=None,
                    source="whatsapp_web",
                ),
            ]
        )
        session.add_all(
            [
                MessageDecision(
                    message_id="general-raw",
                    incident_id=None,
                    created_at="2026-04-20T00:47:10Z",
                    chosen_source="rules",
                    is_issue=False,
                    category=None,
                    event_type=None,
                    confidence=20,
                    needs_review=False,
                    auto_file_candidate=False,
                    rules_json="{}",
                    llm_json="{}",
                    final_json="{}",
                ),
                MessageDecision(
                    message_id="real-raw",
                    incident_id=None,
                    created_at="2026-04-20T00:48:10Z",
                    chosen_source="rules",
                    is_issue=False,
                    category=None,
                    event_type=None,
                    confidence=20,
                    needs_review=False,
                    auto_file_candidate=False,
                    rules_json="{}",
                    llm_json="{}",
                    final_json="{}",
                ),
            ]
        )
        session.commit()

    args = Namespace(
        message_id=None,
        incident_id=None,
        job_id=None,
        service_request_number=None,
        chat_name="General",
        source="whatsapp_web",
        before_ts=None,
        resync_sheets=True,
        reset_capture_state=True,
        apply=True,
    )

    targets = cleanup_script.collect_targets(args)
    assert len(targets.raw_messages) == 1
    assert targets.raw_messages[0].chat_name == "General"

    result = cleanup_script.apply_cleanup(args)
    assert result["state_reset"] is True
    assert resync_calls == ["resynced"]

    with get_session() as session:
        raws = session.query(RawMessage).all()
        decisions = session.query(MessageDecision).all()
        assert [row.chat_name for row in raws] == ["Real Chat"]
        assert [row.message_id for row in decisions] == ["real-raw"]

    state_payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert state_payload["primed_chats"] == ["Real Chat"]
    assert state_payload["seen_entries"] == [{"chat_name": "Real Chat", "fingerprint": "real-1"}]
