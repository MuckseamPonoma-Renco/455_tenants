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
from scripts.sync_chat_export_inbox import DEFAULT_SINCE, _is_ready_export, run_import_and_audit

load_local_env_file(ROOT / ".env")

LOCAL_CLOUD_EXPORT_DIR = ROOT / "incoming" / "cloud_chat_exports"
DEFAULT_STATE_PATH = Path.home() / ".local" / "state" / "tenant-issue-os" / "cloud-chat-export-sync.json"
DEFAULT_MAX_BYTES = 512 * 1024 * 1024
DEFAULT_MAX_EXPORTS = 5
DEFAULT_MAX_LIST_PAGES = 100
SAFE_AUDIT_KEYS = (
    "parsed_messages",
    "audited_messages",
    "matched_messages",
    "missing_db_messages",
    "missing_decisions",
    "review_roster_rows",
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
    os.replace(temporary, path)


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
    if max_bytes <= 0 or max_exports <= 0:
        raise ValueError("max_bytes and max_exports must be positive")
    state = _load_state(state_path)
    state["last_checked_at"] = _now()
    state["pending_acknowledgements"] = _pending_acknowledgements(state)
    owns_client = client is None
    if client is None:
        client = httpx.Client(timeout=httpx.Timeout(connect=30.0, read=900.0, write=30.0, pool=30.0), follow_redirects=False)
    try:
        recovered_acks = 0
        for key, acknowledgement in list(state["pending_acknowledgements"].items()):
            acknowledge_export(client, config, acknowledgement)
            del state["pending_acknowledgements"][key]
            recovered_acks += 1
            _save_state(state_path, state)

        records = pending_exports(client, config, max_bytes=max_bytes)
        processed: list[dict[str, Any]] = []
        for record in records[:max_exports]:
            staged = download_export(client, record, dest_dir=dest_dir, max_bytes=max_bytes)
            audit = run_import_and_audit(staged, since=since)["audit_summary"]
            acknowledgement = {
                "key": record["key"],
                "sha256": _sha256_file(staged),
                "audit": compact_audit(audit),
            }
            state["pending_acknowledgements"][record["key"]] = acknowledgement
            _save_state(state_path, state)
            acknowledge_export(client, config, acknowledgement)
            del state["pending_acknowledgements"][record["key"]]
            _save_state(state_path, state)
            processed.append({"key": record["key"], "staged_export": str(staged), "audit": compact_audit(audit)})

        state["last_error"] = ""
        state["last_success_at"] = _now()
        _save_state(state_path, state)
        return {
            "ok": True,
            "action": "processed" if processed else "unchanged_skip",
            "processed": processed,
            "pending_exports": max(0, len(records) - len(processed)),
            "recovered_acknowledgements": recovered_acks,
            "state_path": str(state_path),
        }
    except Exception as exc:
        state["last_error"] = str(exc)[:1000]
        _save_state(state_path, state)
        raise
    finally:
        if owns_client:
            client.close()


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
        return {"ok": True, "action": "ready", "pending_exports": len(records)}
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
