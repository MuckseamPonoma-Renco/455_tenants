from __future__ import annotations

import argparse
import datetime as dt
import errno
import json
import os
import shutil
import subprocess
import sys
import time
import zipfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

ICLOUD_DRIVE_ROOT = Path.home() / "Library/Mobile Documents/com~apple~CloudDocs"
ICLOUD_CHAT_EXPORT_DIR = ICLOUD_DRIVE_ROOT / "455 Tenant Chat Exports"
LOCAL_CHAT_EXPORT_DIR = ROOT / "incoming" / "chat_exports"
DEFAULT_STATE_PATH = Path.home() / ".local" / "state" / "tenant-issue-os" / "chat-export-sync.json"
DEFAULT_SINCE = "2026-06-05"
EXPORT_EXTENSIONS = {".zip", ".txt"}
STAGE_COPY_ATTEMPTS = 5
STAGE_COPY_RETRY_SECONDS = 2.0
TRANSIENT_ICLOUD_COPY_ERRNOS = frozenset(
    error_number
    for error_number in (
        errno.EAGAIN,
        errno.EBUSY,
        errno.ETIMEDOUT,
        errno.EINTR,
        getattr(errno, "EDEADLK", None),
        getattr(errno, "ESTALE", None),
    )
    if error_number is not None
)


def _split_source_dirs(value: str | None) -> list[Path]:
    if not value:
        return []
    return [Path(item).expanduser() for item in value.split(os.pathsep) if item.strip()]


def default_source_dirs() -> list[Path]:
    configured = _split_source_dirs(os.environ.get("CHAT_EXPORT_SOURCE_DIRS"))
    if configured:
        return configured
    explicit_icloud = os.environ.get("CHAT_EXPORT_ICLOUD_DIR")
    sources = [Path(explicit_icloud).expanduser()] if explicit_icloud else [ICLOUD_CHAT_EXPORT_DIR]
    # iOS Share Sheet saves often land at the top level of iCloud Drive even when
    # the intended inbox folder was created. Scan that level safely for WhatsApp
    # exports so a correct export is not silently missed.
    if ICLOUD_DRIVE_ROOT not in sources:
        sources.append(ICLOUD_DRIVE_ROOT)
    sources.append(LOCAL_CHAT_EXPORT_DIR)
    return sources


def _looks_like_whatsapp_chat_export(path: Path) -> bool:
    name = path.name.casefold()
    return "whatsapp" in name and "chat" in name and path.suffix.casefold() in EXPORT_EXTENSIONS


def _source_paths(source_dir: Path) -> list[Path]:
    """Return candidate paths without recursively walking all of iCloud Drive."""
    if source_dir == ICLOUD_DRIVE_ROOT:
        return list(source_dir.glob("*"))
    return list(source_dir.rglob("*"))


def _export_paths(source_dirs: list[Path]) -> list[Path]:
    candidates: list[Path] = []
    seen: set[Path] = set()
    for source_dir in source_dirs:
        if source_dir != ICLOUD_DRIVE_ROOT:
            try:
                source_dir.mkdir(parents=True, exist_ok=True)
            except OSError:
                continue
        try:
            paths = _source_paths(source_dir)
        except OSError:
            continue
        for path in paths:
            if not path.is_file() or path.name.startswith("."):
                continue
            if path.suffix.casefold() not in EXPORT_EXTENSIONS:
                continue
            if source_dir == ICLOUD_DRIVE_ROOT and not _looks_like_whatsapp_chat_export(path):
                continue
            resolved = path.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            candidates.append(path)
    return candidates


def _is_ready_export(path: Path) -> bool:
    try:
        if path.stat().st_size <= 0:
            return False
        if path.suffix.casefold() == ".zip":
            return zipfile.is_zipfile(path)
    except OSError:
        return False
    return True


def export_candidates(source_dirs: list[Path]) -> list[Path]:
    return [path for path in _export_paths(source_dirs) if _is_ready_export(path)]


def newest_export(source_dirs: list[Path]) -> Path | None:
    candidates = export_candidates(source_dirs)
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def newest_pending_export(source_dirs: list[Path], *, ignored_dir: Path | None = None) -> Path | None:
    ignored = ignored_dir.resolve() if ignored_dir is not None else None
    pending: list[Path] = []
    for path in _export_paths(source_dirs):
        try:
            if ignored is not None and path.resolve().is_relative_to(ignored):
                continue
        except OSError:
            continue
        if not _is_ready_export(path):
            pending.append(path)
    if not pending:
        return None
    return max(pending, key=lambda path: path.stat().st_mtime)


def file_fingerprint(path: Path) -> dict[str, Any]:
    stat = path.stat()
    return {
        "path": str(path.resolve()),
        "name": path.name,
        "size": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
    }


def _export_identity(fingerprint: Any) -> tuple[str, int, int] | None:
    if not isinstance(fingerprint, dict):
        return None
    try:
        name = str(fingerprint["name"])
        size = int(fingerprint["size"])
        mtime_ns = int(fingerprint["mtime_ns"])
    except (KeyError, TypeError, ValueError):
        return None
    return (name, size, mtime_ns) if name and size > 0 and mtime_ns > 0 else None


def _same_export_identity(left: Any, right: Any) -> bool:
    left_identity = _export_identity(left)
    right_identity = _export_identity(right)
    return left_identity is not None and left_identity == right_identity


def load_state(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _discard_invalid_processed_state(state: dict[str, Any]) -> None:
    fingerprint = state.get("last_processed_fingerprint")
    if not isinstance(fingerprint, dict):
        return
    try:
        size = int(fingerprint.get("size") or 0)
    except (TypeError, ValueError):
        size = 0
    if size > 0:
        return
    for key in ("last_processed_at", "last_processed_fingerprint", "last_result", "last_staged_export"):
        state.pop(key, None)
    seen = state.get("last_seen_fingerprint")
    if seen == fingerprint:
        state.pop("last_seen_fingerprint", None)


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _safe_destination(dest_dir: Path, source: Path) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    candidate = dest_dir / source.name
    if not candidate.exists() or candidate.resolve() == source.resolve():
        return candidate
    source_fp = file_fingerprint(source)
    try:
        existing_fp = file_fingerprint(candidate)
    except OSError:
        existing_fp = {}
    if existing_fp.get("size", 0) <= 0 or not _is_ready_export(candidate):
        return candidate
    if source_fp.get("size") == existing_fp.get("size") and source_fp.get("mtime_ns") == existing_fp.get("mtime_ns"):
        return candidate
    stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    return dest_dir / f"{source.stem}-{stamp}{source.suffix}"


def _buffered_copy_export(source: Path, temporary: Path, source_fingerprint: dict[str, Any]) -> None:
    """Copy without macOS fcopyfile, which can deadlock on an iCloud placeholder."""
    with source.open("rb") as source_file, temporary.open("wb") as destination_file:
        shutil.copyfileobj(source_file, destination_file, length=1024 * 1024)
        destination_file.flush()
        os.fsync(destination_file.fileno())
    mtime_ns = int(source_fingerprint["mtime_ns"])
    os.utime(temporary, ns=(mtime_ns, mtime_ns))


def _copy_export(source: Path, temporary: Path, source_fingerprint: dict[str, Any]) -> None:
    try:
        shutil.copy2(source, temporary)
    except OSError as exc:
        deadlock_errno = getattr(errno, "EDEADLK", None)
        if exc.errno != deadlock_errno:
            raise
        # shutil.copy2 uses fcopyfile on macOS. Fall back to buffered I/O once
        # before sleeping and retrying so EDEADLK does not strand a valid export.
        temporary.unlink(missing_ok=True)
        _buffered_copy_export(source, temporary, source_fingerprint)


def stage_export(source: Path, dest_dir: Path) -> Path:
    dest = _safe_destination(dest_dir, source)
    if dest.resolve() == source.resolve():
        return source
    temporary = dest.with_name(f".{dest.name}.{os.getpid()}.partial")
    try:
        source_fingerprint = file_fingerprint(source)
        for attempt in range(1, STAGE_COPY_ATTEMPTS + 1):
            try:
                temporary.unlink(missing_ok=True)
                _copy_export(source, temporary, source_fingerprint)
                copied_fingerprint = file_fingerprint(temporary)
                if copied_fingerprint["size"] != source_fingerprint["size"] or not _is_ready_export(temporary):
                    raise OSError(f"copied export is incomplete: {temporary}")
                if file_fingerprint(source) != source_fingerprint:
                    raise OSError(f"source export changed while staging: {source}")
                break
            except OSError as exc:
                temporary.unlink(missing_ok=True)
                retryable = exc.errno in TRANSIENT_ICLOUD_COPY_ERRNOS
                if retryable and attempt < STAGE_COPY_ATTEMPTS:
                    time.sleep(STAGE_COPY_RETRY_SECONDS * attempt)
                    continue
                raise
        os.replace(temporary, dest)
    except Exception:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    return dest


def run_import_and_audit(export_path: Path, *, since: str) -> dict[str, Any]:
    audit_dir = ROOT / "exports" / "message_decision_audits" / (
        dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%S%fZ") + f"-{os.getpid()}"
    )
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "run_weekly_chat_export_audit.py"),
        "--export",
        str(export_path),
        "--since",
        since,
        "--out-dir",
        str(audit_dir),
    ]
    completed = subprocess.run(
        cmd,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    if completed.returncode:
        detail = completed.stdout[-2000:].strip()
        raise RuntimeError(f"weekly chat export import/audit failed ({completed.returncode}): {detail}")
    summary_path = audit_dir / "summary.json"
    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"weekly chat export audit did not create a readable summary: {summary_path}") from exc
    if int(summary.get("parsed_messages") or 0) <= 0:
        raise RuntimeError(f"weekly chat export audit parsed zero messages: {export_path}")
    return {
        "cmd": cmd,
        "export": str(export_path),
        "audit_summary": summary,
    }


def sync_once(
    *,
    source_dirs: list[Path],
    dest_dir: Path,
    state_path: Path,
    since: str,
    force: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    state = load_state(state_path)
    _discard_invalid_processed_state(state)
    state["last_checked_at"] = dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")
    source = newest_export(source_dirs)
    pending = newest_pending_export(source_dirs, ignored_dir=dest_dir)
    pending_is_newest = source is None
    if pending is not None and source is not None:
        try:
            pending_is_newest = pending.stat().st_mtime >= source.stat().st_mtime
        except OSError:
            pending_is_newest = True
    if pending is not None and pending_is_newest:
        fingerprint = file_fingerprint(pending)
        state["last_pending_fingerprint"] = fingerprint
        state["last_error"] = f"waiting for complete iCloud export: {pending}"
        save_state(state_path, state)
        return {
            "ok": True,
            "action": "waiting_for_download",
            "source": str(pending),
            "fingerprint": fingerprint,
            "state_path": str(state_path),
        }
    if source is None:
        state.pop("last_pending_fingerprint", None)
        state["last_error"] = ""
        save_state(state_path, state)
        return {
            "ok": True,
            "action": "no_export_found",
            "source_dirs": [str(path) for path in source_dirs],
            "state_path": str(state_path),
        }

    fingerprint = file_fingerprint(source)
    state["last_seen_fingerprint"] = fingerprint
    previous = state.get("last_processed_fingerprint")
    if not force and _same_export_identity(previous, fingerprint):
        state.pop("last_pending_fingerprint", None)
        state["last_error"] = ""
        save_state(state_path, state)
        return {
            "ok": True,
            "action": "unchanged_skip",
            "source": str(source),
            "fingerprint": fingerprint,
            "state_path": str(state_path),
        }

    staged = _safe_destination(dest_dir, source)
    if dry_run:
        return {
            "ok": True,
            "action": "would_process",
            "source": str(source),
            "staged_export": str(staged),
            "fingerprint": fingerprint,
            "state_path": str(state_path),
        }

    try:
        staged = stage_export(source, dest_dir)
        result = run_import_and_audit(staged, since=since)
    except Exception as exc:
        state["last_attempt_at"] = dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")
        state["last_error"] = str(exc)[:1000]
        save_state(state_path, state)
        raise

    state["last_processed_at"] = dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")
    state["last_processed_fingerprint"] = fingerprint
    state["last_staged_export"] = str(staged)
    state["last_result"] = result
    state.pop("last_pending_fingerprint", None)
    state["last_error"] = ""
    save_state(state_path, state)
    return {
        "ok": True,
        "action": "processed",
        "source": str(source),
        "staged_export": str(staged),
        "fingerprint": fingerprint,
        "state_path": str(state_path),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Pull the newest WhatsApp export from iCloud/local inbox and run the app import/audit pipeline.")
    parser.add_argument("--source-dir", action="append", help="Folder to scan for .zip/.txt exports. Can be repeated.")
    parser.add_argument("--dest-dir", default=str(LOCAL_CHAT_EXPORT_DIR), help="Local repo inbox where exports are staged.")
    parser.add_argument("--state-path", default=str(DEFAULT_STATE_PATH), help="State file used to skip unchanged exports.")
    parser.add_argument("--since", default=DEFAULT_SINCE, help=f"Audit cutoff timestamp. Default: {DEFAULT_SINCE}")
    parser.add_argument("--force", action="store_true", help="Process the newest export even if it matches the last processed fingerprint.")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be processed without copying/importing/auditing.")
    args = parser.parse_args()

    source_dirs = [Path(item).expanduser() for item in args.source_dir] if args.source_dir else default_source_dirs()
    result = sync_once(
        source_dirs=source_dirs,
        dest_dir=Path(args.dest_dir).expanduser(),
        state_path=Path(args.state_path).expanduser(),
        since=args.since,
        force=args.force,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
