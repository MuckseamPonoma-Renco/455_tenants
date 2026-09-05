from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.local_env import load_local_env_file
from packages.run_lock import try_file_lock
from scripts.sync_chat_export_inbox import (
    AuditPipelineError,
    DEFAULT_SINCE,
    ModelReviewIncompleteError,
    _is_ready_export,
    run_import_and_audit,
)

load_local_env_file(ROOT / ".env")

LOCAL_CLOUD_EXPORT_DIR = ROOT / "incoming" / "cloud_chat_exports"
DEFAULT_STATE_PATH = Path.home() / ".local" / "state" / "tenant-issue-os" / "cloud-chat-export-sync.json"
DEFAULT_MAX_BYTES = 512 * 1024 * 1024
DEFAULT_MAX_EXPORTS = 5
DEFAULT_MAX_LIST_PAGES = 100
CLOUD_STATE_SCHEMA_VERSION = 2
CLOUD_RECEIPT_VERSION = 1
DEFAULT_MAX_RECEIPTS = 100
RECEIPT_STAGE_NAMES = (
    "upload",
    "discovery",
    "download",
    "processing",
    "audit",
    "sheet_sync",
    "sheet_readback",
    "acknowledgement",
)
RECEIPT_STAGE_STATES = {
    "complete",
    "pending",
    "running",
    "not_started",
    "not_reported",
    "not_requested",
    "blocked_model_review",
    "error",
}
SAFE_AUDIT_KEYS = (
    "parsed_messages",
    "audited_messages",
    "matched_messages",
    "missing_db_messages",
    "missing_decisions",
    "llm_review_required",
    "llm_review_completed",
    "llm_review_missing",
    "llm_review_failed",
    "review_roster_rows",
    "unique_physical_message_ids",
    "colliding_base_message_ids",
    "collision_followup_occurrences",
)


class CloudReceiverError(RuntimeError):
    pass


@dataclass(frozen=True)
class ReceiverConfig:
    base_url: str
    pull_token: str


def _now() -> str:
    return dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")


def _load_state(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.partial")
    temporary.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.chmod(0o600)
    os.replace(temporary, path)


def _stage(state: str, *, at: str | None = None) -> dict[str, str]:
    result = {"state": state}
    if at:
        result["at"] = at
    return result


def _receipts(state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    receipts = state.get("receipts")
    if not isinstance(receipts, dict):
        return {}
    normalized: dict[str, dict[str, Any]] = {}
    for stored_key, value in receipts.items():
        if not isinstance(value, dict):
            continue
        key = str(value.get("key") or stored_key or "").strip()
        if not key:
            continue
        value["key"] = key
        normalized[key] = value
    return normalized


def _timestamp_sort_value(value: Any) -> float:
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return float("-inf")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed.timestamp()


def _receipt_sort_key(receipt: dict[str, Any]) -> tuple[float, float, str]:
    key = str(receipt.get("key") or "")
    uploaded_at = _timestamp_sort_value(receipt.get("uploaded_at"))
    keyed_at = _timestamp_sort_value(_uploaded_at_from_key(key))
    acknowledged_at = _timestamp_sort_value(receipt.get("acknowledged_at"))
    if (
        uploaded_at != float("-inf")
        and (keyed_at == float("-inf") or uploaded_at >= keyed_at)
        and (acknowledged_at == float("-inf") or uploaded_at <= acknowledged_at)
    ):
        effective_at = uploaded_at
    elif keyed_at != float("-inf") and (
        acknowledged_at == float("-inf") or keyed_at <= acknowledged_at
    ):
        effective_at = keyed_at
    else:
        effective_at = _timestamp_sort_value(receipt.get("discovered_at"))
    return effective_at, acknowledged_at, key


def _receipt_status(receipt: dict[str, Any]) -> str:
    if receipt.get("legacy_reconstructed") is True:
        return "legacy_unverified"
    stages = receipt.get("stages") if isinstance(receipt.get("stages"), dict) else {}
    stage_states = {
        name: str(stages.get(name, {}).get("state") or "not_started")
        if isinstance(stages.get(name), dict)
        else "not_started"
        for name in RECEIPT_STAGE_NAMES
    }
    if stage_states["audit"] == "blocked_model_review":
        return "blocked_model_review"
    if any(stage_states[name] == "error" for name in ("download", "processing")):
        return "processing_error"
    if stage_states["audit"] == "error":
        return "audit_error"
    if stage_states["sheet_sync"] == "error":
        return "sheet_sync_failed"
    if stage_states["sheet_readback"] == "error":
        return "sheet_readback_failed"
    if stage_states["acknowledgement"] == "error":
        return "acknowledgement_error"
    if stage_states["processing"] in {"pending", "running"} or stage_states["audit"] == "running":
        return "processing"
    if stage_states["audit"] != "complete":
        return "discovered"
    if stage_states["sheet_sync"] != "complete":
        return "sheet_sync_unverified" if stage_states["sheet_sync"] == "not_reported" else "sheet_sync_pending"
    if stage_states["sheet_readback"] != "complete":
        return (
            "sheet_readback_unverified"
            if stage_states["sheet_readback"] == "not_reported"
            else "sheet_readback_pending"
        )
    if stage_states["acknowledgement"] != "complete":
        return "pending_acknowledgement"
    return "ready"


def _set_receipt_stage(
    receipt: dict[str, Any],
    name: str,
    stage_state: str,
    *,
    at: str | None = None,
) -> None:
    stages = receipt.get("stages")
    if not isinstance(stages, dict):
        stages = {}
        receipt["stages"] = stages
    stages[name] = _stage(stage_state, at=at)
    receipt["updated_at"] = at or _now()
    receipt["status"] = _receipt_status(receipt)


def _new_receipt(record: dict[str, Any], *, discovered_at: str) -> dict[str, Any]:
    uploaded_at = str(record.get("uploaded_at") or "")
    receipt: dict[str, Any] = {
        "receipt_version": CLOUD_RECEIPT_VERSION,
        "source": "cloud_receiver",
        "key": str(record["key"]),
        "filename": str(record["filename"]),
        "size_bytes": int(record["size_bytes"]),
        "uploaded_at": uploaded_at,
        "discovered_at": discovered_at,
        "updated_at": discovered_at,
        "stages": {
            "upload": _stage("complete", at=uploaded_at or None),
            "discovery": _stage("complete", at=discovered_at),
            "download": _stage("pending"),
            "processing": _stage("pending"),
            "audit": _stage("pending"),
            "sheet_sync": _stage("pending"),
            "sheet_readback": _stage("not_reported"),
            "acknowledgement": _stage("pending"),
        },
    }
    receipt["status"] = _receipt_status(receipt)
    return receipt


def _uploaded_at_from_key(key: str) -> str:
    stamp = key.removeprefix("pending/").split("-", 1)[0]
    for pattern in ("%Y%m%dT%H%M%S%fZ", "%Y%m%dT%H%M%SZ"):
        try:
            return dt.datetime.strptime(stamp, pattern).replace(tzinfo=dt.UTC).isoformat().replace("+00:00", "Z")
        except ValueError:
            continue
    return ""


def _receipt_from_pending_acknowledgement(
    key: str,
    acknowledgement: dict[str, Any],
    *,
    recovered_at: str,
) -> dict[str, Any]:
    uploaded_at = _uploaded_at_from_key(key)
    filename_parts = key.removeprefix("pending/").split("-", 2)
    receipt: dict[str, Any] = {
        "receipt_version": CLOUD_RECEIPT_VERSION,
        "source": "cloud_receiver",
        "key": key,
        "filename": filename_parts[2] if len(filename_parts) == 3 else "",
        "uploaded_at": uploaded_at,
        "discovered_at": recovered_at,
        "updated_at": recovered_at,
        "recovered_from_pending_acknowledgement": True,
        "audit": compact_audit(acknowledgement.get("audit") if isinstance(acknowledgement.get("audit"), dict) else {}),
        "stages": {
            "upload": _stage("complete", at=uploaded_at or None),
            "discovery": _stage("complete", at=recovered_at),
            "download": _stage("complete"),
            "processing": _stage("complete"),
            "audit": _stage("complete"),
            # Legacy pending acknowledgements were only created after the
            # post-audit Sheet write returned successfully.
            "sheet_sync": _stage("complete"),
            "sheet_readback": _stage("not_reported"),
            "acknowledgement": _stage("pending"),
        },
    }
    sha256 = acknowledgement.get("sha256")
    if isinstance(sha256, str) and len(sha256) == 64:
        receipt["sha256"] = sha256
    receipt["status"] = _receipt_status(receipt)
    return receipt


def _reconstruct_staged_legacy_receipt(state: dict[str, Any], dest_dir: Path) -> None:
    if _receipts(state) or not dest_dir.is_dir():
        return
    candidates = [
        path
        for path in dest_dir.glob("cloud-*")
        if path.is_file() and path.suffix.casefold() in {".zip", ".txt"} and _is_ready_export(path)
    ]
    if not candidates:
        return
    staged = max(candidates, key=lambda path: path.stat().st_mtime_ns)
    observed_at = dt.datetime.fromtimestamp(staged.stat().st_mtime, tz=dt.UTC).isoformat().replace("+00:00", "Z")
    sha256 = _sha256_file(staged)
    key = f"legacy-local/{sha256}"
    receipt: dict[str, Any] = {
        "receipt_version": CLOUD_RECEIPT_VERSION,
        "source": "cloud_receiver",
        "key": key,
        "filename": staged.name,
        "size_bytes": staged.stat().st_size,
        "uploaded_at": "",
        "discovered_at": observed_at,
        "updated_at": _now(),
        "sha256": sha256,
        "staged_export": str(staged),
        "legacy_reconstructed": True,
        "stages": {
            "upload": _stage("complete"),
            "discovery": _stage("complete", at=observed_at),
            "download": _stage("complete", at=observed_at),
            "processing": _stage("not_reported"),
            "audit": _stage("not_reported"),
            "sheet_sync": _stage("not_reported"),
            "sheet_readback": _stage("not_reported"),
            "acknowledgement": _stage("not_reported"),
        },
    }
    receipt["status"] = _receipt_status(receipt)
    state["receipts"] = {key: receipt}


def _upsert_discovered_receipt(state: dict[str, Any], record: dict[str, Any], *, discovered_at: str) -> dict[str, Any]:
    receipts = state.setdefault("receipts", {})
    key = str(record["key"])
    existing = receipts.get(key)
    try:
        existing_version = int(existing.get("receipt_version") or 0) if isinstance(existing, dict) else 0
    except (TypeError, ValueError):
        existing_version = 0
    if not isinstance(existing, dict) or existing_version != CLOUD_RECEIPT_VERSION:
        existing = _new_receipt(record, discovered_at=discovered_at)
        receipts[key] = existing
    else:
        existing["uploaded_at"] = str(record.get("uploaded_at") or existing.get("uploaded_at") or "")
        existing["filename"] = str(record["filename"])
        existing["size_bytes"] = int(record["size_bytes"])
        _set_receipt_stage(existing, "discovery", "complete", at=discovered_at)
    return existing


def _refresh_receipt_index(state: dict[str, Any], *, max_receipts: int = DEFAULT_MAX_RECEIPTS) -> None:
    receipts = _receipts(state)
    pending_export_keys = state.get("pending_export_keys")
    pending_acknowledgements = state.get("pending_acknowledgements")
    required_keys = {
        str(key)
        for key in (pending_export_keys if isinstance(pending_export_keys, list) else [])
    } | {
        str(key)
        for key in (pending_acknowledgements if isinstance(pending_acknowledgements, dict) else {})
    }
    required = sorted(
        (receipt for key, receipt in receipts.items() if key in required_keys),
        key=_receipt_sort_key,
        reverse=True,
    )
    history = sorted(
        (receipt for key, receipt in receipts.items() if key not in required_keys),
        key=_receipt_sort_key,
        reverse=True,
    )
    # Pending work is operational state, not disposable history. Retain every
    # pending key even when it exceeds the history budget, while independently
    # bounding completed receipt history.
    retained = sorted(required + history[:max_receipts], key=_receipt_sort_key, reverse=True)
    state["receipts"] = {str(receipt["key"]): receipt for receipt in retained}
    state["latest_receipt_key"] = str(retained[0]["key"]) if retained else ""
    state["schema_version"] = CLOUD_STATE_SCHEMA_VERSION


def _save_cloud_state(path: Path, state: dict[str, Any]) -> None:
    _refresh_receipt_index(state)
    _save_state(path, state)


def _record_sheet_stages(receipt: dict[str, Any], audit: dict[str, Any], *, at: str) -> None:
    if audit.get("sheet_sync_complete") is True:
        _set_receipt_stage(receipt, "sheet_sync", "complete", at=at)
    elif audit.get("sheet_sync_complete") is False:
        _set_receipt_stage(receipt, "sheet_sync", "error", at=at)
    elif audit.get("sheet_sync_requested") is False:
        _set_receipt_stage(receipt, "sheet_sync", "not_requested", at=at)
    else:
        _set_receipt_stage(receipt, "sheet_sync", "not_reported", at=at)

    readback_values = [audit.get("sheet_readback_complete"), audit.get("sheet_readback_verified")]
    if any(value is True for value in readback_values):
        _set_receipt_stage(receipt, "sheet_readback", "complete", at=at)
    elif any(value is False for value in readback_values):
        _set_receipt_stage(receipt, "sheet_readback", "error", at=at)
    else:
        _set_receipt_stage(receipt, "sheet_readback", "not_reported", at=at)


def receiver_config(receiver_url: str | None = None, pull_token: str | None = None) -> ReceiverConfig | None:
    url = (receiver_url or os.environ.get("CLOUD_EXPORT_RECEIVER_URL") or "").strip().rstrip("/")
    token = (pull_token or os.environ.get("CLOUD_EXPORT_RECEIVER_PULL_TOKEN") or "").strip()
    if not url and not token:
        return None
    if not url or not token:
        raise CloudReceiverError("CLOUD_EXPORT_RECEIVER_URL and CLOUD_EXPORT_RECEIVER_PULL_TOKEN must both be set")
    parsed = urlparse(url)
    if parsed.scheme != "https" or not parsed.netloc or parsed.path not in {"", "/"}:
        raise CloudReceiverError("CLOUD_EXPORT_RECEIVER_URL must be an https origin without a path")
    return ReceiverConfig(base_url=url, pull_token=token)


def _authorized_headers(config: ReceiverConfig) -> dict[str, str]:
    return {"Authorization": f"Bearer {config.pull_token}"}


def _require_success(response: httpx.Response, action: str) -> None:
    if response.is_error:
        raise CloudReceiverError(f"cloud receiver {action} returned HTTP {response.status_code}")


def _json(response: httpx.Response, action: str) -> dict[str, Any]:
    _require_success(response, action)
    try:
        data = response.json()
    except ValueError as exc:
        raise CloudReceiverError(f"cloud receiver {action} returned invalid JSON") from exc
    if not isinstance(data, dict):
        raise CloudReceiverError(f"cloud receiver {action} returned an invalid response")
    return data


def _is_valid_export_record(value: Any, *, max_bytes: int) -> bool:
    if not isinstance(value, dict):
        return False
    key = value.get("key")
    filename = value.get("filename")
    download_url = value.get("download_url")
    size_bytes = value.get("size_bytes")
    if not isinstance(key, str) or not key.startswith("pending/") or ".." in key:
        return False
    if not isinstance(filename, str) or "/" in filename or "\\" in filename:
        return False
    if not filename.casefold().startswith("whatsapp chat") or Path(filename).suffix.casefold() not in {".zip", ".txt"}:
        return False
    if not isinstance(download_url, str) or urlparse(download_url).scheme != "https":
        return False
    return isinstance(size_bytes, int) and 0 < size_bytes <= max_bytes


def pending_exports(
    client: httpx.Client,
    config: ReceiverConfig,
    *,
    max_bytes: int,
    max_pages: int = DEFAULT_MAX_LIST_PAGES,
) -> list[dict[str, Any]]:
    if max_pages <= 0:
        raise ValueError("max_pages must be positive")
    records: list[dict[str, Any]] = []
    cursor: str | None = None
    seen_cursors: set[str] = set()
    for _page in range(max_pages):
        try:
            response = client.get(
                f"{config.base_url}/v1/exports",
                headers=_authorized_headers(config),
                params={"cursor": cursor} if cursor else None,
            )
        except httpx.HTTPError as exc:
            raise CloudReceiverError("cloud receiver export listing failed") from exc
        data = _json(response, "export listing")
        exports = data.get("exports")
        if not isinstance(exports, list):
            raise CloudReceiverError("cloud receiver export listing is missing exports")
        page_records = [record for record in exports if _is_valid_export_record(record, max_bytes=max_bytes)]
        if len(page_records) != len(exports):
            raise CloudReceiverError("cloud receiver returned an invalid export record")
        records.extend(page_records)

        if data.get("truncated") is not True:
            return sorted(records, key=lambda record: (str(record.get("uploaded_at") or ""), str(record["key"])))
        next_cursor = data.get("cursor")
        if not isinstance(next_cursor, str) or not next_cursor or next_cursor in seen_cursors:
            raise CloudReceiverError("cloud receiver pagination cursor is invalid")
        seen_cursors.add(next_cursor)
        cursor = next_cursor
    raise CloudReceiverError(f"cloud receiver export listing exceeded {max_pages} pages")


def completed_receipts(
    client: httpx.Client,
    config: ReceiverConfig,
) -> list[dict[str, Any]]:
    try:
        response = client.get(
            f"{config.base_url}/v1/receipts/latest",
            headers=_authorized_headers(config),
        )
    except httpx.HTTPError as exc:
        raise CloudReceiverError("cloud receiver latest receipt readback failed") from exc
    data = _json(response, "latest receipt readback")
    receipt = data.get("receipt")
    if receipt is None:
        return []
    if not isinstance(receipt, dict):
        raise CloudReceiverError("cloud receiver latest receipt readback is invalid")
    return [receipt]


def _target_path(dest_dir: Path, record: dict[str, Any]) -> Path:
    digest = hashlib.sha256(str(record["key"]).encode("utf-8")).hexdigest()[:16]
    return dest_dir / f"cloud-{digest}-{record['filename']}"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_download(path: Path, *, expected_size: int) -> None:
    if not path.is_file() or path.stat().st_size != expected_size or path.stat().st_size <= 0:
        raise CloudReceiverError(f"downloaded export is incomplete: {path.name}")
    if path.suffix.casefold() == ".zip" and not zipfile.is_zipfile(path):
        raise CloudReceiverError(f"downloaded export is not a valid ZIP: {path.name}")
    if not _is_ready_export(path):
        raise CloudReceiverError(f"downloaded export is not ready: {path.name}")


def download_export(
    client: httpx.Client,
    record: dict[str, Any],
    *,
    dest_dir: Path,
    max_bytes: int,
) -> Path:
    target = _target_path(dest_dir, record)
    expected_size = int(record["size_bytes"])
    if target.exists():
        try:
            _validate_download(target, expected_size=expected_size)
            return target
        except CloudReceiverError:
            target.unlink(missing_ok=True)

    dest_dir.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.partial")
    total = 0
    try:
        temporary.unlink(missing_ok=True)
        try:
            with client.stream("GET", str(record["download_url"])) as response:
                _require_success(response, "export download")
                content_length = response.headers.get("Content-Length")
                if content_length:
                    try:
                        declared_size = int(content_length)
                    except ValueError as exc:
                        raise CloudReceiverError("cloud receiver download has an invalid Content-Length") from exc
                    if declared_size != expected_size or declared_size > max_bytes:
                        raise CloudReceiverError("cloud receiver download size does not match the signed export")
                with temporary.open("wb") as handle:
                    for chunk in response.iter_bytes(chunk_size=1024 * 1024):
                        total += len(chunk)
                        if total > max_bytes or total > expected_size:
                            raise CloudReceiverError("cloud receiver download exceeded its permitted size")
                        handle.write(chunk)
                    handle.flush()
                    os.fsync(handle.fileno())
        except httpx.HTTPError as exc:
            raise CloudReceiverError("cloud receiver export download failed") from exc
        if total != expected_size:
            raise CloudReceiverError("cloud receiver download size does not match the export record")
        _validate_download(temporary, expected_size=expected_size)
        os.replace(temporary, target)
        return target
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def compact_audit(summary: dict[str, Any]) -> dict[str, int]:
    return {
        key: int(summary[key])
        for key in SAFE_AUDIT_KEYS
        if isinstance(summary.get(key), int) and int(summary[key]) >= 0
    }


def _timestamp_text(value: Any) -> str:
    if not isinstance(value, str) or _timestamp_sort_value(value) == float("-inf"):
        return ""
    return value


def _compact_receipt_stages(receipt: dict[str, Any]) -> dict[str, dict[str, str]]:
    supplied = receipt.get("stages")
    if not isinstance(supplied, dict):
        supplied = {}
    stages: dict[str, dict[str, str]] = {}
    for name in RECEIPT_STAGE_NAMES:
        value = supplied.get(name)
        state = str(value.get("state") or "") if isinstance(value, dict) else ""
        if state not in RECEIPT_STAGE_STATES:
            state = "not_reported"
        at = _timestamp_text(value.get("at")) if isinstance(value, dict) else ""
        stages[name] = _stage(state, at=at or None)
    return stages


def compact_pipeline_receipt(receipt: dict[str, Any]) -> dict[str, Any]:
    return {
        "receipt_version": CLOUD_RECEIPT_VERSION,
        "source": "cloud_receiver",
        "uploaded_at": _timestamp_text(receipt.get("uploaded_at")) or None,
        "discovered_at": _timestamp_text(receipt.get("discovered_at")) or None,
        "stages": _compact_receipt_stages(receipt),
    }


def _merge_completed_receipts(state: dict[str, Any], remote_receipts: list[dict[str, Any]]) -> int:
    merged = 0
    for remote in remote_receipts:
        key = remote.get("key")
        acknowledged_at = _timestamp_text(remote.get("acknowledged_at"))
        if not isinstance(key, str) or not key.startswith("pending/") or ".." in key or not acknowledged_at:
            continue
        try:
            receipt_version = int(remote.get("receipt_version") or 0)
        except (TypeError, ValueError):
            receipt_version = 0
        if receipt_version == CLOUD_RECEIPT_VERSION and isinstance(remote.get("stages"), dict):
            completed = {
                "receipt_version": CLOUD_RECEIPT_VERSION,
                "source": "cloud_receiver",
                "key": key,
                "uploaded_at": _timestamp_text(remote.get("uploaded_at")) or _uploaded_at_from_key(key),
                "discovered_at": _timestamp_text(remote.get("discovered_at")) or acknowledged_at,
                "updated_at": _timestamp_text(remote.get("updated_at")) or acknowledged_at,
                "acknowledged_at": acknowledged_at,
                "audit": compact_audit(remote.get("audit") if isinstance(remote.get("audit"), dict) else {}),
                "stages": _compact_receipt_stages(remote),
                "recovered_from_cloud_receipt": True,
            }
            completed["stages"]["acknowledgement"] = _stage("complete", at=acknowledged_at)
        else:
            completed = _receipt_from_pending_acknowledgement(key, remote, recovered_at=acknowledged_at)
            completed["recovered_from_cloud_receipt"] = True
            completed["stages"]["acknowledgement"] = _stage("complete", at=acknowledged_at)
            completed["acknowledged_at"] = acknowledged_at
        sha256 = remote.get("sha256")
        if isinstance(sha256, str) and len(sha256) == 64:
            completed["sha256"] = sha256
        completed["status"] = _receipt_status(completed)
        existing = state["receipts"].get(key)
        if isinstance(existing, dict):
            for name in ("filename", "size_bytes", "staged_export"):
                if existing.get(name) is not None:
                    completed[name] = existing[name]
        state["receipts"][key] = completed
        merged += 1
    return merged


def acknowledge_export(client: httpx.Client, config: ReceiverConfig, acknowledgement: dict[str, Any]) -> dict[str, Any]:
    try:
        response = client.post(
            f"{config.base_url}/v1/exports/ack",
            headers=_authorized_headers(config),
            json=acknowledgement,
        )
    except httpx.HTTPError as exc:
        raise CloudReceiverError("cloud receiver export acknowledgement failed") from exc
    data = _json(response, "export acknowledgement")
    if data.get("acknowledged") is not True:
        raise CloudReceiverError("cloud receiver did not acknowledge the export")
    return data


def _pending_acknowledgements(state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    pending = state.get("pending_acknowledgements")
    if isinstance(pending, dict):
        return {str(key): value for key, value in pending.items() if isinstance(value, dict)}
    return {}


def _run_once_unlocked(
    config: ReceiverConfig,
    *,
    dest_dir: Path = LOCAL_CLOUD_EXPORT_DIR,
    state_path: Path = DEFAULT_STATE_PATH,
    since: str = DEFAULT_SINCE,
    max_bytes: int = DEFAULT_MAX_BYTES,
    max_exports: int = DEFAULT_MAX_EXPORTS,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    if max_bytes <= 0 or max_exports <= 0:
        raise ValueError("max_bytes and max_exports must be positive")
    state = _load_state(state_path)
    state["schema_version"] = CLOUD_STATE_SCHEMA_VERSION
    state["receipts"] = _receipts(state)
    state["last_checked_at"] = _now()
    state["pending_acknowledgements"] = _pending_acknowledgements(state)
    _reconstruct_staged_legacy_receipt(state, dest_dir)
    owns_client = client is None
    if client is None:
        client = httpx.Client(timeout=httpx.Timeout(connect=30.0, read=900.0, write=30.0, pool=30.0), follow_redirects=False)
    try:
        remote_receipts = completed_receipts(client, config)
        recovered_cloud_receipts = _merge_completed_receipts(state, remote_receipts)
        state["last_remote_receipt_sync_at"] = _now()
        recovered_acks = 0
        for key in list(state["pending_acknowledgements"]):
            receipt = state["receipts"].get(key)
            stages = receipt.get("stages") if isinstance(receipt, dict) else None
            acknowledgement_stage = stages.get("acknowledgement") if isinstance(stages, dict) else None
            if isinstance(acknowledgement_stage, dict) and acknowledgement_stage.get("state") == "complete":
                del state["pending_acknowledgements"][key]
                recovered_acks += 1
        _save_cloud_state(state_path, state)
        for key, acknowledgement in list(state["pending_acknowledgements"].items()):
            receipt = state["receipts"].get(key)
            if not isinstance(receipt, dict):
                receipt = _receipt_from_pending_acknowledgement(key, acknowledgement, recovered_at=_now())
                state["receipts"][key] = receipt
            _set_receipt_stage(receipt, "acknowledgement", "pending")
            acknowledgement["pipeline_receipt"] = compact_pipeline_receipt(receipt)
            _save_cloud_state(state_path, state)
            try:
                acknowledge_export(client, config, acknowledgement)
            except Exception:
                _set_receipt_stage(receipt, "acknowledgement", "error", at=_now())
                _save_cloud_state(state_path, state)
                raise
            del state["pending_acknowledgements"][key]
            _set_receipt_stage(receipt, "acknowledgement", "complete", at=_now())
            recovered_acks += 1
            _save_cloud_state(state_path, state)

        records = pending_exports(client, config, max_bytes=max_bytes)
        state["pending_export_keys"] = [str(record["key"]) for record in records]
        discovered_at = _now()
        for record in records:
            _upsert_discovered_receipt(state, record, discovered_at=discovered_at)
        _save_cloud_state(state_path, state)
        processed: list[dict[str, Any]] = []
        blocked: list[dict[str, Any]] = []
        # A stale blocked export must not prevent a newer iPhone upload from
        # reaching the database. Receiver acknowledgement still waits for a
        # complete model review.
        for record in list(reversed(records))[:max_exports]:
            receipt = state["receipts"][str(record["key"])]
            try:
                staged = download_export(client, record, dest_dir=dest_dir, max_bytes=max_bytes)
            except Exception:
                _set_receipt_stage(receipt, "download", "error", at=_now())
                _save_cloud_state(state_path, state)
                raise
            downloaded_at = _now()
            receipt["sha256"] = _sha256_file(staged)
            receipt["staged_export"] = str(staged)
            _set_receipt_stage(receipt, "download", "complete", at=downloaded_at)
            _set_receipt_stage(receipt, "processing", "running", at=downloaded_at)
            _set_receipt_stage(receipt, "audit", "running", at=downloaded_at)
            _save_cloud_state(state_path, state)
            try:
                audit = run_import_and_audit(staged, since=since)["audit_summary"]
            except ModelReviewIncompleteError as exc:
                if exc.reason != "insufficient_quota":
                    failed_at = _now()
                    _set_receipt_stage(receipt, "processing", "error", at=failed_at)
                    _set_receipt_stage(receipt, "audit", "error", at=failed_at)
                    receipt["last_error"] = str(exc)[:1000]
                    receipt["audit"] = compact_audit(exc.summary)
                    _save_cloud_state(state_path, state)
                    raise
                blocked_at = _now()
                _set_receipt_stage(receipt, "processing", "complete", at=blocked_at)
                _set_receipt_stage(receipt, "audit", "blocked_model_review", at=blocked_at)
                receipt["last_error"] = exc.reason
                receipt["audit"] = compact_audit(exc.summary)
                _save_cloud_state(state_path, state)
                blocked.append(
                    {
                        "key": record["key"],
                        "reason": exc.reason,
                        "audit": compact_audit(exc.summary),
                    }
                )
                continue
            except AuditPipelineError as exc:
                failed_at = _now()
                receipt["last_error"] = str(exc)[:1000]
                receipt["audit"] = compact_audit(exc.summary)
                _set_receipt_stage(receipt, "processing", "complete", at=failed_at)
                audit_complete = (
                    int(exc.summary.get("parsed_messages") or 0) > 0
                    and int(exc.summary.get("missing_db_messages") or 0) == 0
                    and int(exc.summary.get("missing_decisions") or 0) == 0
                    and exc.summary.get("llm_review_complete") is not False
                )
                if audit_complete:
                    _set_receipt_stage(receipt, "audit", "complete", at=failed_at)
                    _record_sheet_stages(receipt, exc.summary, at=failed_at)
                else:
                    _set_receipt_stage(receipt, "audit", "error", at=failed_at)
                    _set_receipt_stage(receipt, "sheet_sync", "not_started", at=failed_at)
                    _set_receipt_stage(receipt, "sheet_readback", "not_reported", at=failed_at)
                _save_cloud_state(state_path, state)
                raise
            except Exception as exc:
                failed_at = _now()
                _set_receipt_stage(receipt, "processing", "error", at=failed_at)
                _set_receipt_stage(receipt, "audit", "error", at=failed_at)
                receipt["last_error"] = str(exc)[:1000]
                _save_cloud_state(state_path, state)
                raise
            completed_at = _now()
            receipt.pop("last_error", None)
            receipt["audit"] = compact_audit(audit)
            _set_receipt_stage(receipt, "processing", "complete", at=completed_at)
            _set_receipt_stage(receipt, "audit", "complete", at=completed_at)
            _record_sheet_stages(receipt, audit, at=completed_at)
            acknowledgement = {
                "key": record["key"],
                "sha256": receipt["sha256"],
                "audit": compact_audit(audit),
                "pipeline_receipt": compact_pipeline_receipt(receipt),
            }
            state["pending_acknowledgements"][record["key"]] = acknowledgement
            _set_receipt_stage(receipt, "acknowledgement", "pending", at=completed_at)
            _save_cloud_state(state_path, state)
            try:
                acknowledge_export(client, config, acknowledgement)
            except Exception:
                _set_receipt_stage(receipt, "acknowledgement", "error", at=_now())
                _save_cloud_state(state_path, state)
                raise
            del state["pending_acknowledgements"][record["key"]]
            state["pending_export_keys"] = [
                key for key in state.get("pending_export_keys", []) if key != str(record["key"])
            ]
            _set_receipt_stage(receipt, "acknowledgement", "complete", at=_now())
            _save_cloud_state(state_path, state)
            processed.append({"key": record["key"], "staged_export": str(staged), "audit": compact_audit(audit)})

        state["last_error"] = ""
        state["last_blocked_model_review"] = blocked
        state["last_success_at"] = _now()
        _save_cloud_state(state_path, state)
        action = "processed" if processed else "unchanged_skip"
        if blocked:
            action = "processed_with_blocked_model_review" if processed else "blocked_model_review"
        return {
            "ok": True,
            "action": action,
            "processed": processed,
            "blocked_exports": blocked,
            "pending_exports": max(0, len(records) - len(processed)),
            "recovered_acknowledgements": recovered_acks,
            "recovered_cloud_receipts": recovered_cloud_receipts,
            "state_path": str(state_path),
        }
    except Exception as exc:
        state["last_error"] = str(exc)[:1000]
        _save_cloud_state(state_path, state)
        raise
    finally:
        if owns_client:
            client.close()


def run_once(
    config: ReceiverConfig,
    *,
    dest_dir: Path = LOCAL_CLOUD_EXPORT_DIR,
    state_path: Path = DEFAULT_STATE_PATH,
    since: str = DEFAULT_SINCE,
    max_bytes: int = DEFAULT_MAX_BYTES,
    max_exports: int = DEFAULT_MAX_EXPORTS,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    lock_path = state_path.parent / "chat-export-pipeline.lock"
    with try_file_lock(lock_path) as acquired:
        if not acquired:
            return {
                "ok": True,
                "action": "skipped_concurrent_run",
                "processed": [],
                "blocked_exports": [],
                "pending_exports": 0,
                "recovered_acknowledgements": 0,
                "state_path": str(state_path),
            }
        return _run_once_unlocked(
            config,
            dest_dir=dest_dir,
            state_path=state_path,
            since=since,
            max_bytes=max_bytes,
            max_exports=max_exports,
            client=client,
        )


def probe(config: ReceiverConfig, *, max_bytes: int = DEFAULT_MAX_BYTES, client: httpx.Client | None = None) -> dict[str, Any]:
    owns_client = client is None
    if client is None:
        client = httpx.Client(timeout=httpx.Timeout(connect=15.0, read=30.0, write=15.0, pool=15.0), follow_redirects=False)
    try:
        try:
            health_response = client.get(f"{config.base_url}/health")
        except httpx.HTTPError as exc:
            raise CloudReceiverError("cloud receiver health probe failed") from exc
        health = _json(health_response, "health probe")
        if health.get("ok") is not True or health.get("r2_ready") is not True:
            raise CloudReceiverError("cloud receiver is not ready")
        records = pending_exports(client, config, max_bytes=max_bytes)
        receipts = completed_receipts(client, config)
        return {
            "ok": True,
            "action": "ready",
            "pending_exports": len(records),
            "completed_receipts": len(receipts),
        }
    finally:
        if owns_client:
            client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Recover unaudited WhatsApp exports from the private Cloudflare receiver.")
    parser.add_argument("--receiver-url", help="Cloud receiver https origin. Defaults to CLOUD_EXPORT_RECEIVER_URL.")
    parser.add_argument("--pull-token", help="Cloud receiver pull token. Defaults to CLOUD_EXPORT_RECEIVER_PULL_TOKEN.")
    parser.add_argument("--dest-dir", default=str(LOCAL_CLOUD_EXPORT_DIR), help="Local directory for downloaded cloud exports.")
    parser.add_argument("--state-path", default=str(DEFAULT_STATE_PATH), help="State path for acknowledgement recovery.")
    parser.add_argument("--since", default=DEFAULT_SINCE, help=f"Audit cutoff timestamp. Default: {DEFAULT_SINCE}")
    parser.add_argument("--max-bytes", type=int, default=DEFAULT_MAX_BYTES, help="Reject exports larger than this many bytes.")
    parser.add_argument("--max-exports", type=int, default=DEFAULT_MAX_EXPORTS, help="Maximum cloud exports to process in one run.")
    parser.add_argument("--probe", action="store_true", help="Verify receiver health and authenticated listing without downloading exports.")
    args = parser.parse_args()

    config = receiver_config(args.receiver_url, args.pull_token)
    if config is None:
        print(json.dumps({"ok": True, "action": "not_configured"}, sort_keys=True))
        return
    if args.probe:
        result = probe(config, max_bytes=args.max_bytes)
    else:
        result = run_once(
            config,
            dest_dir=Path(args.dest_dir).expanduser(),
            state_path=Path(args.state_path).expanduser(),
            since=args.since,
            max_bytes=args.max_bytes,
            max_exports=args.max_exports,
        )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
