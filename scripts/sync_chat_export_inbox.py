from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.audit_whatsapp_export_decisions import DEFAULT_SINCE, EXPORT_EXTENSIONS

ICLOUD_CHAT_EXPORT_DIR = Path.home() / "Library/Mobile Documents/com~apple~CloudDocs/455 Tenant Chat Exports"
LOCAL_CHAT_EXPORT_DIR = ROOT / "incoming" / "chat_exports"
DEFAULT_STATE_PATH = Path.home() / ".local" / "state" / "tenant-issue-os" / "chat-export-sync.json"


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
    sources.append(LOCAL_CHAT_EXPORT_DIR)
    return sources


def export_candidates(source_dirs: list[Path]) -> list[Path]:
    candidates: list[Path] = []
    seen: set[Path] = set()
    for source_dir in source_dirs:
        try:
            source_dir.mkdir(parents=True, exist_ok=True)
        except OSError:
            continue
        try:
            paths = list(source_dir.rglob("*"))
        except OSError:
            continue
        for path in paths:
            if not path.is_file() or path.name.startswith("."):
                continue
            if path.suffix.casefold() not in EXPORT_EXTENSIONS:
                continue
            resolved = path.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            candidates.append(path)
    return candidates


def newest_export(source_dirs: list[Path]) -> Path | None:
    candidates = export_candidates(source_dirs)
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def file_fingerprint(path: Path) -> dict[str, Any]:
    stat = path.stat()
    return {
        "path": str(path.resolve()),
        "name": path.name,
        "size": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
    }


def load_state(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


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
    if source_fp.get("size") == existing_fp.get("size") and source_fp.get("mtime_ns") == existing_fp.get("mtime_ns"):
        return candidate
    stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    return dest_dir / f"{source.stem}-{stamp}{source.suffix}"


def stage_export(source: Path, dest_dir: Path) -> Path:
    dest = _safe_destination(dest_dir, source)
    if dest.resolve() == source.resolve():
        return source
    shutil.copy2(source, dest)
    return dest


def run_import_and_audit(export_path: Path, *, since: str) -> dict[str, Any]:
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "run_weekly_chat_export_audit.py"),
        "--export",
        str(export_path),
        "--since",
        since,
    ]
    subprocess.run(cmd, cwd=ROOT, check=True)
    return {"cmd": cmd, "export": str(export_path)}


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
    source = newest_export(source_dirs)
    if source is None:
        return {
            "ok": True,
            "action": "no_export_found",
            "source_dirs": [str(path) for path in source_dirs],
            "state_path": str(state_path),
        }

    fingerprint = file_fingerprint(source)
    previous = state.get("last_processed_fingerprint")
    if not force and previous == fingerprint:
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

    staged = stage_export(source, dest_dir)
    result = run_import_and_audit(staged, since=since)
    state["last_processed_at"] = dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")
    state["last_processed_fingerprint"] = fingerprint
    state["last_staged_export"] = str(staged)
    state["last_result"] = result
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
