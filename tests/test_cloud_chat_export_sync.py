import io
import json
import zipfile

import httpx
import pytest

import scripts.sync_cloud_chat_export_inbox as cloud_sync


def _zip_bytes() -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("WhatsApp Chat - 455 Tenants.txt", "[6/5/26, 9:00:00 AM] Karen: North elevator stopped\n")
    return output.getvalue()


def _record(payload: bytes) -> dict[str, object]:
    return {
        "key": "pending/20260720T030405Z-0123456789abcdef0123456789abcdef-WhatsApp Chat - 455 Tenants 12.zip",
        "filename": "WhatsApp Chat - 455 Tenants 12.zip",
        "size_bytes": len(payload),
        "uploaded_at": "2026-07-20T03:04:05Z",
        "download_url": "https://signed.example.test/export",
    }


def _client(handler):
    return httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False)


def test_run_once_downloads_audits_then_acknowledges(tmp_path, monkeypatch):
    payload = _zip_bytes()
    record = _record(payload)
    acknowledgements = []

    def handler(request):
        if request.url == httpx.URL("https://uploads.example.test/v1/exports"):
            return httpx.Response(200, json={"exports": [record]})
        if request.url == httpx.URL("https://signed.example.test/export"):
            return httpx.Response(200, content=payload, headers={"Content-Length": str(len(payload))})
        if request.url == httpx.URL("https://uploads.example.test/v1/exports/ack"):
            acknowledgements.append(json.loads(request.content))
            return httpx.Response(200, json={"acknowledged": True})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    monkeypatch.setattr(
        cloud_sync,
        "run_import_and_audit",
        lambda export_path, *, since: {
            "export": str(export_path),
            "audit_summary": {
                "parsed_messages": 12,
                "audited_messages": 4,
                "matched_messages": 4,
                "missing_db_messages": 0,
                "missing_decisions": 0,
                "review_roster_rows": 1,
                "message_text": "must not leave the machine",
            },
        },
    )
    config = cloud_sync.ReceiverConfig("https://uploads.example.test", "pull-token")
    result = cloud_sync.run_once(
        config,
        dest_dir=tmp_path / "incoming",
        state_path=tmp_path / "state.json",
        client=_client(handler),
    )

    assert result["action"] == "processed"
    assert len(result["processed"]) == 1
    assert list((tmp_path / "incoming").glob("*.zip"))
    assert len(acknowledgements) == 1
    assert acknowledgements[0]["key"] == record["key"]
    assert acknowledgements[0]["sha256"].isalnum()
    assert acknowledgements[0]["audit"] == {
        "parsed_messages": 12,
        "audited_messages": 4,
        "matched_messages": 4,
        "missing_db_messages": 0,
        "missing_decisions": 0,
        "review_roster_rows": 1,
    }
    state = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
    assert state["pending_acknowledgements"] == {}


def test_run_once_recovers_a_saved_acknowledgement_before_listing(tmp_path):
    key = "pending/20260720T030405Z-0123456789abcdef0123456789abcdef-WhatsApp Chat - 455 Tenants 12.zip"
    state_path = tmp_path / "state.json"
    state_path.write_text(
        json.dumps(
            {
                "pending_acknowledgements": {
                    key: {"key": key, "sha256": "a" * 64, "audit": {"parsed_messages": 1}},
                }
            }
        ),
        encoding="utf-8",
    )
    call_order = []

    def handler(request):
        if request.url == httpx.URL("https://uploads.example.test/v1/exports/ack"):
            call_order.append("ack")
            return httpx.Response(200, json={"acknowledged": True, "idempotent": True})
        if request.url == httpx.URL("https://uploads.example.test/v1/exports"):
            call_order.append("list")
            return httpx.Response(200, json={"exports": []})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    result = cloud_sync.run_once(
        cloud_sync.ReceiverConfig("https://uploads.example.test", "pull-token"),
        dest_dir=tmp_path / "incoming",
        state_path=state_path,
        client=_client(handler),
    )

    assert result["action"] == "unchanged_skip"
    assert result["recovered_acknowledgements"] == 1
    assert call_order == ["ack", "list"]
    assert json.loads(state_path.read_text(encoding="utf-8"))["pending_acknowledgements"] == {}


def test_pending_exports_follows_cloud_receiver_pagination():
    first = _record(_zip_bytes())
    first["key"] = "pending/20260720T030405Z-0123456789abcdef0123456789abcdef-WhatsApp Chat - 455 Tenants 12.zip"
    second = _record(_zip_bytes())
    second["key"] = "pending/20260720T040405Z-fedcba9876543210fedcba9876543210-WhatsApp Chat - 455 Tenants 13.zip"
    second["filename"] = "WhatsApp Chat - 455 Tenants 13.zip"
    second["uploaded_at"] = "2026-07-20T04:04:05Z"
    requests = []

    def handler(request):
        requests.append(str(request.url))
        if request.url == httpx.URL("https://uploads.example.test/v1/exports"):
            return httpx.Response(200, json={"exports": [first], "truncated": True, "cursor": "page-two"})
        if request.url == httpx.URL("https://uploads.example.test/v1/exports?cursor=page-two"):
            return httpx.Response(200, json={"exports": [second], "truncated": False, "cursor": None})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    records = cloud_sync.pending_exports(
        _client(handler),
        cloud_sync.ReceiverConfig("https://uploads.example.test", "pull-token"),
        max_bytes=cloud_sync.DEFAULT_MAX_BYTES,
    )

    assert [record["key"] for record in records] == [first["key"], second["key"]]
    assert requests == [
        "https://uploads.example.test/v1/exports",
        "https://uploads.example.test/v1/exports?cursor=page-two",
    ]


def test_pending_exports_rejects_a_truncated_response_without_a_cursor():
    def handler(request):
        return httpx.Response(200, json={"exports": [], "truncated": True})

    with pytest.raises(cloud_sync.CloudReceiverError, match="pagination cursor is invalid"):
        cloud_sync.pending_exports(
            _client(handler),
            cloud_sync.ReceiverConfig("https://uploads.example.test", "pull-token"),
            max_bytes=cloud_sync.DEFAULT_MAX_BYTES,
        )


def test_probe_checks_public_health_and_authenticated_listing():
    def handler(request):
        if request.url == httpx.URL("https://uploads.example.test/health"):
            return httpx.Response(200, json={"ok": True, "r2_ready": True})
        if request.url == httpx.URL("https://uploads.example.test/v1/exports"):
            assert request.headers["Authorization"] == "Bearer pull-token"
            return httpx.Response(200, json={"exports": []})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    result = cloud_sync.probe(
        cloud_sync.ReceiverConfig("https://uploads.example.test", "pull-token"),
        client=_client(handler),
    )

    assert result == {"ok": True, "action": "ready", "pending_exports": 0}


@pytest.mark.parametrize("url", ["http://uploads.example.test", "https://uploads.example.test/v1/exports", "not-a-url"])
def test_receiver_config_rejects_non_origin_urls(url):
    with pytest.raises(cloud_sync.CloudReceiverError):
        cloud_sync.receiver_config(url, "pull-token")
