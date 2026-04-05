from packages.db import MessageDecision, RawMessage, get_session
from packages.tasker_capture import normalize_tasker_capture


def auth_headers():
    return {"Authorization": "Bearer test-token"}


def test_normalize_tasker_capture_extracts_chat_and_sender_from_notification_title():
    normalized = normalize_tasker_capture(
        "455 Tenants (3 messages): ~\u202fMolly",
        "%ansubtext",
        "Btw, I did forward the News12 story to Weinreb.",
    )

    assert normalized.chat_name == "455 Tenants"
    assert normalized.sender == "Molly"
    assert normalized.text == "Btw, I did forward the News12 story to Weinreb."


def test_normalize_tasker_capture_extracts_sender_from_text_when_needed():
    normalized = normalize_tasker_capture(
        "455 Tenants",
        "%ansubtext",
        "~\u202fMolly: Emma, did you send this to Weinreb?",
    )

    assert normalized.chat_name == "455 Tenants"
    assert normalized.sender == "Molly"
    assert normalized.text == "Emma, did you send this to Weinreb?"


def test_tasker_ingest_dedupes_recent_duplicate_notification(client):
    first = client.post("/ingest/tasker", headers=auth_headers(), json={
        "chat_name": "455 Tenants (3 messages): ~\u202fMolly",
        "text": "Btw, I did forward the News12 story to Weinreb.",
        "sender": "%ansubtext",
        "ts_epoch": 1775421265,
    })
    second = client.post("/ingest/tasker", headers=auth_headers(), json={
        "chat_name": "455 Tenants (3 messages): ~\u202fMolly",
        "text": "Btw, I did forward the News12 story to Weinreb.",
        "sender": "%ansubtext",
        "ts_epoch": 1775421341,
    })

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["deduped"] is False
    assert second.json()["deduped"] is True
    assert second.json()["message_id"] == first.json()["message_id"]

    with get_session() as session:
        raws = session.query(RawMessage).all()
        decisions = session.query(MessageDecision).all()

    assert len(raws) == 1
    assert len(decisions) == 1
    assert raws[0].chat_name == "455 Tenants"
    assert raws[0].sender == "Molly"
    assert raws[0].text == "Btw, I did forward the News12 story to Weinreb."
