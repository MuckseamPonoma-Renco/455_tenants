import io
import json
import stat
import sys
import zipfile

import httpx
import pytest

import scripts.sync_cloud_chat_export_inbox as cloud_sync
from packages.run_lock import try_file_lock


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


def _client(handler, *, receipts=None):
    def with_receipts(request):
        if request.url == httpx.URL("https://uploads.example.test/v1/receipts/latest"):
            return httpx.Response(200, json={"receipt": (receipts or [None])[0]})
        return handler(request)

    return httpx.Client(transport=httpx.MockTransport(with_receipts), follow_redirects=False)


def _successful_certification_summary() -> dict[str, object]:
    return {
        "ok": True,
        "parsed_messages": 12,
        "audited_messages": 4,
        "matched_messages": 4,
        "unique_physical_message_ids": 4,
        "missing_db_messages": 0,
        "missing_decisions": 0,
        "llm_review_required": 4,
        "llm_review_completed": 4,
        "llm_review_missing": 0,
        "llm_review_failed": 0,
        "llm_review_complete": True,
        "review_roster_rows": 0,
        "sheet_sync_requested": True,
        "sheet_sync_complete": True,
        "sheet_readback_requested": True,
        "sheet_readback_verified": True,
        "sheet_readback_audit": {"ok": True},
    }


def _write_legacy_receipt(tmp_path, *, payload: bytes | None = None):
    payload = payload or _zip_bytes()
    dest_dir = tmp_path / "incoming"
    dest_dir.mkdir()
    staged = dest_dir / "cloud-0123456789abcdef-WhatsApp Chat - 455 Tenants.zip"
    staged.write_bytes(payload)
    sha256 = cloud_sync._sha256_file(staged)
    key = f"legacy-local/{sha256}"
    reconstructed_at = "2026-09-05T12:10:08Z"
    receipt = {
        "receipt_version": cloud_sync.CLOUD_RECEIPT_VERSION,
        "source": "cloud_receiver",
        "key": key,
        "filename": staged.name,
        "size_bytes": len(payload),
        "uploaded_at": "",
        "discovered_at": reconstructed_at,
        "updated_at": reconstructed_at,
        "sha256": sha256,
        "staged_export": str(staged),
        "legacy_reconstructed": True,
        "status": "legacy_unverified",
        "stages": {
            "upload": {"state": "complete"},
            "discovery": {"state": "complete", "at": reconstructed_at},
            "download": {"state": "complete", "at": reconstructed_at},
            "processing": {"state": "not_reported"},
            "audit": {"state": "not_reported"},
            "sheet_sync": {"state": "not_reported"},
            "sheet_readback": {"state": "not_reported"},
            "acknowledgement": {"state": "not_reported"},
        },
    }
    state_path = tmp_path / "state.json"
    state_path.write_text(
        json.dumps(
            {
                "schema_version": cloud_sync.CLOUD_STATE_SCHEMA_VERSION,
                "receipts": {key: receipt},
                "latest_receipt_key": key,
                "pending_acknowledgements": {},
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return dest_dir, state_path, staged, key


def test_completed_receipts_tolerates_legacy_receiver_during_rolling_upgrade():
    def handler(request):
        assert request.url == httpx.URL("https://uploads.example.test/v1/receipts/latest")
        return httpx.Response(404, json={"error": "not_found"})

    result = cloud_sync.completed_receipts(
        httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False),
        cloud_sync.ReceiverConfig("https://uploads.example.test", "pull-token"),
    )

    assert result == []


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
                "unique_physical_message_ids": 12,
                "colliding_base_message_ids": 1,
                "collision_followup_occurrences": 1,
                "sheet_sync_requested": True,
                "sheet_sync_complete": True,
                "sheet_readback_verified": True,
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
        "unique_physical_message_ids": 12,
        "colliding_base_message_ids": 1,
        "collision_followup_occurrences": 1,
    }
    assert acknowledgements[0]["pipeline_receipt"]["receipt_version"] == cloud_sync.CLOUD_RECEIPT_VERSION
    assert acknowledgements[0]["pipeline_receipt"]["stages"]["sheet_sync"]["state"] == "complete"
    assert acknowledgements[0]["pipeline_receipt"]["stages"]["sheet_readback"]["state"] == "complete"
    assert acknowledgements[0]["pipeline_receipt"]["stages"]["acknowledgement"]["state"] == "pending"
    assert "staged_export" not in acknowledgements[0]["pipeline_receipt"]
    state = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
    assert stat.S_IMODE((tmp_path / "state.json").stat().st_mode) == 0o600
    assert state["schema_version"] == cloud_sync.CLOUD_STATE_SCHEMA_VERSION
    assert state["pending_acknowledgements"] == {}
    assert state["pending_export_keys"] == []
    assert state["latest_receipt_key"] == record["key"]
    receipt = state["receipts"][record["key"]]
    assert receipt["receipt_version"] == cloud_sync.CLOUD_RECEIPT_VERSION
    assert receipt["uploaded_at"] == record["uploaded_at"]
    assert receipt["status"] == "ready"
    assert {name: stage["state"] for name, stage in receipt["stages"].items()} == {
        "upload": "complete",
        "discovery": "complete",
        "download": "complete",
        "processing": "complete",
        "audit": "complete",
        "sheet_sync": "complete",
        "sheet_readback": "complete",
        "acknowledgement": "complete",
    }


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
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["pending_acknowledgements"] == {}
    receipt = state["receipts"][key]
    assert receipt["recovered_from_pending_acknowledgement"] is True
    assert receipt["status"] == "sheet_readback_unverified"
    assert receipt["stages"]["sheet_sync"]["state"] == "complete"
    assert receipt["stages"]["sheet_readback"]["state"] == "not_reported"
    assert receipt["stages"]["acknowledgement"]["state"] == "complete"


def test_run_once_reconstructs_an_unverified_receipt_for_pre_upgrade_staged_export(tmp_path):
    dest_dir = tmp_path / "incoming"
    dest_dir.mkdir()
    staged = dest_dir / "cloud-0123456789abcdef-WhatsApp Chat - 455 Tenants.zip"
    staged.write_bytes(_zip_bytes())
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps({"last_success_at": "2026-07-20T03:10:00Z"}), encoding="utf-8")

    def handler(request):
        if request.url == httpx.URL("https://uploads.example.test/v1/exports"):
            return httpx.Response(200, json={"exports": []})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    result = cloud_sync.run_once(
        cloud_sync.ReceiverConfig("https://uploads.example.test", "pull-token"),
        dest_dir=dest_dir,
        state_path=state_path,
        client=_client(handler),
    )

    assert result["action"] == "unchanged_skip"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    receipt = state["receipts"][state["latest_receipt_key"]]
    assert receipt["legacy_reconstructed"] is True
    assert receipt["status"] == "legacy_unverified"
    assert receipt["stages"]["upload"]["state"] == "complete"
    assert receipt["stages"]["download"]["state"] == "complete"
    assert receipt["stages"]["audit"]["state"] == "not_reported"
    assert receipt["stages"]["sheet_readback"]["state"] == "not_reported"
    assert receipt["stages"]["acknowledgement"]["state"] == "not_reported"


def test_certify_legacy_receipt_runs_full_checks_then_atomically_promotes_latest(
    tmp_path, monkeypatch
):
    dest_dir, state_path, staged, key = _write_legacy_receipt(tmp_path)
    audited = []
    requests = []

    monkeypatch.setattr(
        cloud_sync,
        "run_import_and_audit",
        lambda export_path, *, since: audited.append((export_path, since))
        or {"audit_summary": _successful_certification_summary()},
    )

    def handler(request):
        requests.append((request.method, str(request.url)))
        if request.url == httpx.URL("https://uploads.example.test/v1/exports"):
            assert request.headers["Authorization"] == "Bearer pull-token"
            return httpx.Response(200, json={"exports": []})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    result = cloud_sync.certify_legacy_receipt(
        cloud_sync.ReceiverConfig("https://uploads.example.test", "pull-token"),
        dest_dir=dest_dir,
        state_path=state_path,
        client=_client(handler),
    )

    assert result["action"] == "legacy_receipt_certified"
    assert result["receipt_key"] == key
    assert result["pending_exports_checked"] == 0
    assert result["same_size_pending_exports_hashed"] == 0
    assert audited == [(staged.resolve(), cloud_sync.DEFAULT_SINCE)]
    assert requests == [("GET", "https://uploads.example.test/v1/exports")]
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert stat.S_IMODE(state_path.stat().st_mode) == 0o600
    receipt = state["receipts"][key]
    assert "legacy_reconstructed" not in receipt
    assert receipt["status"] == "ready"
    assert receipt["acknowledgement_basis"] == (
        "certified_absent_from_authenticated_pending_export_listing"
    )
    assert {name: stage["state"] for name, stage in receipt["stages"].items()} == {
        name: "complete" for name in cloud_sync.RECEIPT_STAGE_NAMES
    }
    certification = receipt["legacy_certification"]
    assert certification["receipt_key"] == key
    assert certification["sha256"] == cloud_sync._sha256_file(staged)
    assert certification["matching_pending_exports"] == 0
    assert certification["pending_exports_checked"] == 0
    assert certification["same_size_pending_exports_hashed"] == 0
    assert certification["basis"] == [
        "staged_export_valid_and_sha256_matched",
        "full_import_and_audit_completed",
        "database_and_decision_reconciliation_completed",
        "model_review_completed",
        "sheet_sync_completed",
        "sheet_readback_verified",
        "authenticated_receiver_has_no_matching_pending_export",
    ]
    assert receipt["stages"]["acknowledgement"]["at"] == certification["certified_at"]


def test_certify_legacy_receipt_fails_closed_when_audit_is_incomplete(tmp_path, monkeypatch):
    dest_dir, state_path, _staged, _key = _write_legacy_receipt(tmp_path)
    original_state = state_path.read_bytes()
    summary = _successful_certification_summary()
    summary["sheet_readback_verified"] = False
    summary["sheet_readback_audit"] = {"ok": False}
    monkeypatch.setattr(
        cloud_sync,
        "run_import_and_audit",
        lambda *_args, **_kwargs: {"audit_summary": summary},
    )

    with pytest.raises(
        cloud_sync.LegacyReceiptCertificationError,
        match="Sheet readback is not verified complete",
    ):
        cloud_sync.certify_legacy_receipt(
            cloud_sync.ReceiverConfig("https://uploads.example.test", "pull-token"),
            dest_dir=dest_dir,
            state_path=state_path,
            client=_client(lambda request: (_ for _ in ()).throw(AssertionError(request.url))),
        )

    assert state_path.read_bytes() == original_state


def test_certify_legacy_receipt_fails_closed_when_matching_export_is_still_pending(
    tmp_path, monkeypatch
):
    payload = _zip_bytes()
    dest_dir, state_path, _staged, _key = _write_legacy_receipt(tmp_path, payload=payload)
    original_state = state_path.read_bytes()
    record = _record(payload)
    requests = []
    monkeypatch.setattr(
        cloud_sync,
        "run_import_and_audit",
        lambda *_args, **_kwargs: {"audit_summary": _successful_certification_summary()},
    )

    def handler(request):
        requests.append((request.method, str(request.url)))
        if request.url == httpx.URL("https://uploads.example.test/v1/exports"):
            assert request.headers["Authorization"] == "Bearer pull-token"
            return httpx.Response(200, json={"exports": [record]})
        if request.url == httpx.URL("https://signed.example.test/export"):
            return httpx.Response(
                200,
                content=payload,
                headers={"Content-Length": str(len(payload))},
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    with pytest.raises(
        cloud_sync.LegacyReceiptCertificationError,
        match="matching cloud export remains pending",
    ):
        cloud_sync.certify_legacy_receipt(
            cloud_sync.ReceiverConfig("https://uploads.example.test", "pull-token"),
            dest_dir=dest_dir,
            state_path=state_path,
            client=_client(handler),
        )

    assert requests == [
        ("GET", "https://uploads.example.test/v1/exports"),
        ("GET", "https://signed.example.test/export"),
    ]
    assert state_path.read_bytes() == original_state


def test_certify_legacy_receipt_rejects_a_staged_sha_mismatch_before_audit(
    tmp_path, monkeypatch
):
    dest_dir, state_path, _staged, key = _write_legacy_receipt(tmp_path)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    bad_sha256 = "0" * 64
    receipt = state["receipts"].pop(key)
    receipt["key"] = f"legacy-local/{bad_sha256}"
    receipt["sha256"] = bad_sha256
    state["receipts"][receipt["key"]] = receipt
    state["latest_receipt_key"] = receipt["key"]
    state_path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    original_state = state_path.read_bytes()
    monkeypatch.setattr(
        cloud_sync,
        "run_import_and_audit",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("audit must not run")),
    )

    with pytest.raises(
        cloud_sync.LegacyReceiptCertificationError,
        match="SHA256 does not match",
    ):
        cloud_sync.certify_legacy_receipt(
            cloud_sync.ReceiverConfig("https://uploads.example.test", "pull-token"),
            dest_dir=dest_dir,
            state_path=state_path,
            client=_client(lambda request: (_ for _ in ()).throw(AssertionError(request.url))),
        )

    assert state_path.read_bytes() == original_state


def test_certify_legacy_receipt_skips_when_pipeline_lock_is_held(tmp_path):
    dest_dir, state_path, _staged, key = _write_legacy_receipt(tmp_path)
    with try_file_lock(tmp_path / "chat-export-pipeline.lock") as acquired:
        assert acquired is True
        result = cloud_sync.certify_legacy_receipt(
            cloud_sync.ReceiverConfig("https://uploads.example.test", "pull-token"),
            dest_dir=dest_dir,
            state_path=state_path,
            receipt_key=key,
            client=_client(lambda request: (_ for _ in ()).throw(AssertionError(request.url))),
        )

    assert result == {
        "ok": True,
        "action": "skipped_concurrent_run",
        "receipt_key": key,
        "state_path": str(state_path),
    }


def test_certify_legacy_receipt_cli_accepts_an_exact_receipt_key(monkeypatch, capsys):
    key = "legacy-local/" + "a" * 64
    captured = {}
    config = cloud_sync.ReceiverConfig("https://uploads.example.test", "pull-token")
    monkeypatch.setattr(cloud_sync, "receiver_config", lambda *_args, **_kwargs: config)
    monkeypatch.setattr(
        cloud_sync,
        "certify_legacy_receipt",
        lambda supplied_config, **kwargs: captured.update(config=supplied_config, **kwargs)
        or {"ok": True, "action": "legacy_receipt_certified"},
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "sync_cloud_chat_export_inbox.py",
            "--certify-legacy-receipt",
            "--receipt-key",
            key,
            "--state-path",
            "/tmp/test-cloud-state.json",
            "--dest-dir",
            "/tmp/test-cloud-exports",
        ],
    )

    cloud_sync.main()

    assert json.loads(capsys.readouterr().out)["action"] == "legacy_receipt_certified"
    assert captured["config"] == config
    assert captured["receipt_key"] == key
    assert captured["state_path"] == cloud_sync.Path("/tmp/test-cloud-state.json")
    assert captured["dest_dir"] == cloud_sync.Path("/tmp/test-cloud-exports")


def test_run_once_keeps_quota_blocked_export_pending_without_failing(tmp_path, monkeypatch):
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

    summary = {
        "parsed_messages": 12,
        "audited_messages": 4,
        "matched_messages": 4,
        "llm_review_required": 4,
        "llm_review_completed": 0,
        "llm_review_missing": 0,
        "llm_review_failed": 4,
        "review_roster_rows": 4,
        "llm_review_complete": False,
        "llm_review_retry": {"error": "insufficient_quota"},
    }
    monkeypatch.setattr(
        cloud_sync,
        "run_import_and_audit",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(cloud_sync.ModelReviewIncompleteError(summary)),
    )

    result = cloud_sync.run_once(
        cloud_sync.ReceiverConfig("https://uploads.example.test", "pull-token"),
        dest_dir=tmp_path / "incoming",
        state_path=tmp_path / "state.json",
        client=_client(handler),
    )

    assert result["ok"] is True
    assert result["action"] == "blocked_model_review"
    assert result["pending_exports"] == 1
    assert result["blocked_exports"][0]["reason"] == "insufficient_quota"
    assert acknowledgements == []
    state = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
    receipt = state["receipts"][record["key"]]
    assert receipt["status"] == "blocked_model_review"
    assert receipt["stages"]["processing"]["state"] == "complete"
    assert receipt["stages"]["audit"]["state"] == "blocked_model_review"
    assert receipt["stages"]["acknowledgement"]["state"] == "pending"


def test_run_once_persists_failed_acknowledgement_as_a_retryable_receipt(tmp_path, monkeypatch):
    payload = _zip_bytes()
    record = _record(payload)

    def handler(request):
        if request.url == httpx.URL("https://uploads.example.test/v1/exports"):
            return httpx.Response(200, json={"exports": [record]})
        if request.url == httpx.URL("https://signed.example.test/export"):
            return httpx.Response(200, content=payload, headers={"Content-Length": str(len(payload))})
        if request.url == httpx.URL("https://uploads.example.test/v1/exports/ack"):
            return httpx.Response(503, json={"error": "unavailable"})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    monkeypatch.setattr(
        cloud_sync,
        "run_import_and_audit",
        lambda *_args, **_kwargs: {
            "audit_summary": {
                "parsed_messages": 12,
                "sheet_sync_requested": True,
                "sheet_sync_complete": True,
                "sheet_readback_complete": True,
            }
        },
    )
    state_path = tmp_path / "state.json"

    with pytest.raises(cloud_sync.CloudReceiverError, match="acknowledgement returned HTTP 503"):
        cloud_sync.run_once(
            cloud_sync.ReceiverConfig("https://uploads.example.test", "pull-token"),
            dest_dir=tmp_path / "incoming",
            state_path=state_path,
            client=_client(handler),
        )

    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert record["key"] in state["pending_acknowledgements"]
    assert record["key"] in state["pending_export_keys"]
    receipt = state["receipts"][record["key"]]
    assert receipt["status"] == "acknowledgement_error"
    assert receipt["stages"]["sheet_sync"]["state"] == "complete"
    assert receipt["stages"]["sheet_readback"]["state"] == "complete"
    assert receipt["stages"]["acknowledgement"]["state"] == "error"


def test_run_once_preserves_audit_success_when_sheet_sync_fails(tmp_path, monkeypatch):
    payload = _zip_bytes()
    record = _record(payload)

    def handler(request):
        if request.url == httpx.URL("https://uploads.example.test/v1/exports"):
            return httpx.Response(200, json={"exports": [record]})
        if request.url == httpx.URL("https://signed.example.test/export"):
            return httpx.Response(200, content=payload, headers={"Content-Length": str(len(payload))})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    summary = {
        "parsed_messages": 12,
        "audited_messages": 4,
        "llm_review_complete": True,
        "sheet_sync_requested": True,
        "sheet_sync_complete": False,
        "error": "post-audit sheet sync failed: temporary Google API failure",
    }
    monkeypatch.setattr(
        cloud_sync,
        "run_import_and_audit",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            cloud_sync.AuditPipelineError(summary, "post-audit sheet sync failed")
        ),
    )
    state_path = tmp_path / "state.json"

    with pytest.raises(cloud_sync.AuditPipelineError, match="sheet sync failed"):
        cloud_sync.run_once(
            cloud_sync.ReceiverConfig("https://uploads.example.test", "pull-token"),
            dest_dir=tmp_path / "incoming",
            state_path=state_path,
            client=_client(handler),
        )

    receipt = json.loads(state_path.read_text(encoding="utf-8"))["receipts"][record["key"]]
    assert receipt["status"] == "sheet_sync_failed"
    assert receipt["stages"]["processing"]["state"] == "complete"
    assert receipt["stages"]["audit"]["state"] == "complete"
    assert receipt["stages"]["sheet_sync"]["state"] == "error"
    assert receipt["stages"]["acknowledgement"]["state"] == "pending"


def test_run_once_does_not_mislabel_audit_integrity_failure_as_sheet_failure(tmp_path, monkeypatch):
    payload = _zip_bytes()
    record = _record(payload)

    def handler(request):
        if request.url == httpx.URL("https://uploads.example.test/v1/exports"):
            return httpx.Response(200, json={"exports": [record]})
        if request.url == httpx.URL("https://signed.example.test/export"):
            return httpx.Response(200, content=payload, headers={"Content-Length": str(len(payload))})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    summary = {
        "parsed_messages": 12,
        "missing_db_messages": 1,
        "missing_decisions": 0,
        "error": "strict audit reconciliation failed",
    }
    monkeypatch.setattr(
        cloud_sync,
        "run_import_and_audit",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(cloud_sync.AuditPipelineError(summary, "audit failed")),
    )
    state_path = tmp_path / "state.json"

    with pytest.raises(cloud_sync.AuditPipelineError, match="audit failed"):
        cloud_sync.run_once(
            cloud_sync.ReceiverConfig("https://uploads.example.test", "pull-token"),
            dest_dir=tmp_path / "incoming",
            state_path=state_path,
            client=_client(handler),
        )

    receipt = json.loads(state_path.read_text(encoding="utf-8"))["receipts"][record["key"]]
    assert receipt["status"] == "audit_error"
    assert receipt["stages"]["processing"]["state"] == "complete"
    assert receipt["stages"]["audit"]["state"] == "error"
    assert receipt["stages"]["sheet_sync"]["state"] == "not_started"


def test_run_once_records_explicit_sheet_readback_failure_without_acknowledging(tmp_path, monkeypatch):
    payload = _zip_bytes()
    record = _record(payload)

    def handler(request):
        if request.url == httpx.URL("https://uploads.example.test/v1/exports"):
            return httpx.Response(200, json={"exports": [record]})
        if request.url == httpx.URL("https://signed.example.test/export"):
            return httpx.Response(200, content=payload, headers={"Content-Length": str(len(payload))})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    summary = {
        "parsed_messages": 12,
        "missing_db_messages": 0,
        "missing_decisions": 0,
        "llm_review_complete": True,
        "sheet_sync_requested": True,
        "sheet_sync_complete": True,
        "sheet_readback_verified": False,
    }
    monkeypatch.setattr(
        cloud_sync,
        "run_import_and_audit",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            cloud_sync.AuditPipelineError(summary, "sheet readback failed")
        ),
    )
    state_path = tmp_path / "state.json"

    with pytest.raises(cloud_sync.AuditPipelineError, match="readback failed"):
        cloud_sync.run_once(
            cloud_sync.ReceiverConfig("https://uploads.example.test", "pull-token"),
            dest_dir=tmp_path / "incoming",
            state_path=state_path,
            client=_client(handler),
        )

    receipt = json.loads(state_path.read_text(encoding="utf-8"))["receipts"][record["key"]]
    assert receipt["status"] == "sheet_readback_failed"
    assert receipt["stages"]["audit"]["state"] == "complete"
    assert receipt["stages"]["sheet_sync"]["state"] == "complete"
    assert receipt["stages"]["sheet_readback"]["state"] == "error"
    assert receipt["stages"]["acknowledgement"]["state"] == "pending"


def test_run_once_checks_newest_export_first_and_continues_after_quota_block(tmp_path, monkeypatch):
    payload = _zip_bytes()
    older = _record(payload)
    newer = dict(older)
    newer.update(
        {
            "key": "pending/20260812T234850Z-fedcba9876543210fedcba9876543210-WhatsApp Chat - 455 Tenants.zip",
            "filename": "WhatsApp Chat - 455 Tenants.zip",
            "uploaded_at": "2026-08-12T23:48:50Z",
            "download_url": "https://signed.example.test/newer",
        }
    )
    older["download_url"] = "https://signed.example.test/older"
    audited = []

    def handler(request):
        if request.url == httpx.URL("https://uploads.example.test/v1/exports"):
            return httpx.Response(200, json={"exports": [older, newer]})
        if request.url in {
            httpx.URL("https://signed.example.test/newer"),
            httpx.URL("https://signed.example.test/older"),
        }:
            return httpx.Response(200, content=payload, headers={"Content-Length": str(len(payload))})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    summary = {
        "llm_review_complete": False,
        "llm_review_retry": {"error": "insufficient_quota"},
    }

    def blocked_review(export_path, *, since):
        audited.append(export_path.name)
        raise cloud_sync.ModelReviewIncompleteError(summary)

    monkeypatch.setattr(cloud_sync, "run_import_and_audit", blocked_review)

    result = cloud_sync.run_once(
        cloud_sync.ReceiverConfig("https://uploads.example.test", "pull-token"),
        dest_dir=tmp_path / "incoming",
        state_path=tmp_path / "state.json",
        client=_client(handler),
        max_exports=2,
    )

    assert result["ok"] is True
    assert result["action"] == "blocked_model_review"
    assert len(audited) == 2
    assert audited[0].endswith(str(newer["filename"]))
    assert audited[1].endswith(str(older["filename"]))
    assert [row["key"] for row in result["blocked_exports"]] == [newer["key"], older["key"]]


def test_run_once_skips_when_another_export_pipeline_run_holds_the_lock(tmp_path):
    state_path = tmp_path / "state.json"
    lock_path = tmp_path / "chat-export-pipeline.lock"

    with try_file_lock(lock_path) as acquired:
        assert acquired is True
        result = cloud_sync.run_once(
            cloud_sync.ReceiverConfig("https://uploads.example.test", "pull-token"),
            dest_dir=tmp_path / "incoming",
            state_path=state_path,
            client=_client(lambda request: (_ for _ in ()).throw(AssertionError(request.url))),
        )

    assert result["action"] == "skipped_concurrent_run"
    assert result["processed"] == []


def test_run_once_recovers_latest_completed_receipt_from_cloud_state(tmp_path):
    payload = _zip_bytes()
    key = str(_record(payload)["key"])
    stages = {
        name: {"state": "complete", "at": "2026-07-20T03:10:00Z"}
        for name in cloud_sync.RECEIPT_STAGE_NAMES
    }
    remote_receipt = {
        "receipt_version": cloud_sync.CLOUD_RECEIPT_VERSION,
        "key": key,
        "uploaded_at": "2026-07-20T03:04:05Z",
        "discovered_at": "2026-07-20T03:05:00Z",
        "updated_at": "2026-07-20T03:10:00Z",
        "acknowledged_at": "2026-07-20T03:10:00Z",
        "sha256": "a" * 64,
        "audit": {
            "parsed_messages": 12,
            "unique_physical_message_ids": 12,
            "colliding_base_message_ids": 0,
            "collision_followup_occurrences": 0,
        },
        "stages": stages,
    }

    def handler(request):
        if request.url == httpx.URL("https://uploads.example.test/v1/exports"):
            return httpx.Response(200, json={"exports": []})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    state_path = tmp_path / "state.json"
    result = cloud_sync.run_once(
        cloud_sync.ReceiverConfig("https://uploads.example.test", "pull-token"),
        dest_dir=tmp_path / "incoming",
        state_path=state_path,
        client=_client(handler, receipts=[remote_receipt]),
    )

    assert result["recovered_cloud_receipts"] == 1
    state = json.loads(state_path.read_text(encoding="utf-8"))
    receipt = state["receipts"][key]
    assert state["latest_receipt_key"] == key
    assert receipt["status"] == "ready"
    assert receipt["recovered_from_cloud_receipt"] is True
    assert receipt["audit"]["unique_physical_message_ids"] == 12


def test_receipt_retention_keeps_every_pending_key_and_newest_completed_receipt():
    pending_keys = [
        f"pending/20260720T030405000Z-{index:032x}-WhatsApp Chat.zip"
        for index in range(105)
    ]
    receipts = {
        key: {
            "key": key,
            "uploaded_at": "2026-07-20T03:04:05Z",
            "discovered_at": "2026-07-20T06:00:00Z",
        }
        for key in pending_keys
    }
    newest_key = "pending/20260721T030405000Z-ffffffffffffffffffffffffffffffff-WhatsApp Chat.zip"
    receipts[newest_key] = {
        "key": newest_key,
        "uploaded_at": "2026-07-21T03:04:05Z",
        "discovered_at": "2026-07-21T03:05:00Z",
    }
    state = {
        "receipts": receipts,
        "pending_export_keys": pending_keys,
        "pending_acknowledgements": {},
    }

    cloud_sync._refresh_receipt_index(state, max_receipts=1)

    assert set(pending_keys).issubset(state["receipts"])
    assert newest_key in state["receipts"]
    assert len(state["receipts"]) == len(pending_keys) + 1
    assert state["latest_receipt_key"] == newest_key


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

    assert result == {
        "ok": True,
        "action": "ready",
        "pending_exports": 0,
        "completed_receipts": 0,
    }


@pytest.mark.parametrize("url", ["http://uploads.example.test", "https://uploads.example.test/v1/exports", "not-a-url"])
def test_receiver_config_rejects_non_origin_urls(url):
    with pytest.raises(cloud_sync.CloudReceiverError):
        cloud_sync.receiver_config(url, "pull-token")
