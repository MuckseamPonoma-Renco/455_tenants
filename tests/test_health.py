from packages.whatsapp.status import write_capture_status


def test_health_includes_whatsapp_capture_status(client, monkeypatch, tmp_path):
    status_path = tmp_path / "whatsapp_status.json"
    monkeypatch.setenv("WHATSAPP_CAPTURE_STATUS_PATH", str(status_path))
    write_capture_status(status_path, state="login_required", login_required=True, chat_names=["455 Tenants"])

    response = client.get("/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["whatsapp_capture"]["state"] == "login_required"
    assert payload["whatsapp_capture"]["login_required"] is True
