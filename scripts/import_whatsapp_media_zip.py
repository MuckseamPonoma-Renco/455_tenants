from __future__ import annotations

import argparse
import json
import mimetypes
import os
import re
import shutil
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.local_env import load_local_env_file

load_local_env_file(ROOT / ".env")

from packages.audit import append_audit_event, compute_message_id, sender_hash
from packages.db import Incident, MessageDecision, RawMessage, get_session
from packages.incident.extractor import classify_and_upsert_incident
from packages.sheets.sync import (
    sync_311_cases_to_sheets,
    sync_311_queue_to_sheets,
    sync_coverage_to_sheets,
    sync_dashboard_to_sheets,
    sync_decisions_to_sheets,
    sync_incidents_to_sheets,
    sync_public_updates_to_sheets,
)
from packages.timeutil import epoch_to_iso, parse_ts_to_epoch
from packages.whatsapp.attachments import (
    build_attachment_manifest,
    make_attachment_item,
    merge_attachment_manifests,
    parse_attachment_manifest,
    strip_reply_context_from_text,
)
from packages.whatsapp.media import DEFAULT_MEDIA_DIR, STAGED_RUNTIME_ROOT, media_root
from packages.whatsapp.parser import ParsedMessage, is_media_placeholder_text, parse_export_text

ATTACHED_SPLIT_RE = re.compile(r"\s*,\s*")
PHONE_RE = re.compile(r"(?<!\d)(?:\+?1[\s.\-]?)?(?:\(\d{3}\)|\d{3})[\s.\-]\d{3}[\s.\-]\d{4}(?!\d)")
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
UNIT_LINE_RE = re.compile(r"^(?:apt\.?|apartment|unit)?\s*\d{1,3}[A-Z]?(?:\s+here)?\.?$", re.IGNORECASE)
TIME_LINE_RE = re.compile(r"^\d{1,2}:\d{2}\s*(?:AM|PM)?$", re.IGNORECASE)
ISSUE_RE = re.compile(
    r"\b(elevator|elevators|lift|lifts|stair|stairs|handrail|broken|kaputt|stuck|dead|out|not working|no longer|leak|mold|heat|hot water)\b",
    re.IGNORECASE,
)


@dataclass
class MediaMessage:
    index: int
    parsed: ParsedMessage
    ts_epoch: int | None
    ts_iso: str | None
    attachment_names: list[str]
    manifest: str | None
    text_for_storage: str


def _clean(value: str | None) -> str:
    return (value or "").replace("\u202f", " ").replace("\u200e", "").replace("\u200f", "").strip()


def _slug(value: str) -> str:
    clean = re.sub(r"[^A-Za-z0-9_.-]+", "_", _clean(value).lower()).strip("_")
    return clean or "whatsapp_export"


def _attachment_names(raw: str | None, zip_names: set[str]) -> list[str]:
    if not raw:
        return []
    out: list[str] = []
    for token in ATTACHED_SPLIT_RE.split(raw):
        clean = _clean(token)
        if not clean or clean.startswith("omitted:"):
            continue
        name = Path(clean).name
        if name in zip_names:
            out.append(name)
    return out


def _kind_for_filename(filename: str) -> str:
    upper = filename.upper()
    suffix = Path(filename).suffix.lower()
    if "VIDEO" in upper or suffix in {".mp4", ".mov", ".m4v"}:
        return "video"
    if "AUDIO" in upper or suffix in {".opus", ".ogg", ".mp3", ".m4a", ".wav"}:
        return "audio"
    if "STICKER" in upper:
        return "image"
    if "PHOTO" in upper or suffix in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
        return "image"
    return "document"


def _generic_media_text(names: list[str]) -> str:
    kinds = {_kind_for_filename(name) for name in names}
    if "image" in kinds:
        return "Photo attached"
    if "video" in kinds:
        return "Video attached"
    if "audio" in kinds:
        return "Audio attached"
    return "File attached"


def _extract_media_files(zf: zipfile.ZipFile, zip_path: Path, chat_name: str, message: ParsedMessage, names: list[str]) -> str | None:
    if not names:
        return None
    ts_epoch = parse_ts_to_epoch(message.ts_iso)
    day = (epoch_to_iso(ts_epoch) or _clean(message.ts_iso) or "unknown")[:10].replace("/", "-")
    root = media_root()
    if not os.environ.get("WHATSAPP_CAPTURE_MEDIA_DIR") and STAGED_RUNTIME_ROOT.exists():
        root = (STAGED_RUNTIME_ROOT / DEFAULT_MEDIA_DIR).resolve()
    target_dir = root / "exports" / _slug(zip_path.stem) / _slug(chat_name) / day
    target_dir.mkdir(parents=True, exist_ok=True)

    items: list[dict[str, object]] = []
    for name in names:
        target = target_dir / Path(name).name
        if not target.exists():
            with zf.open(name) as source, target.open("wb") as dest:
                shutil.copyfileobj(source, dest)
        kind = _kind_for_filename(name)
        content_type = mimetypes.guess_type(name)[0]
        items.append(
            make_attachment_item(
                kind=kind,
                label="whatsapp_export",
                status="downloaded",
                path=target.resolve(),
                content_type=content_type,
                filename=target.name,
                extra={"export_filename": name},
            )
        )
    return build_attachment_manifest(items=items, source="whatsapp_export")


def _message_text_for_storage(message: ParsedMessage, names: list[str]) -> str:
    text = _clean(message.text)
    if names and (not text or is_media_placeholder_text(text)):
        return _generic_media_text(names)
    return text


def _text_key(value: str | None) -> str:
    lines = []
    for line in _clean(value).splitlines():
        clean = _clean(line)
        if not clean:
            continue
        if PHONE_RE.fullmatch(clean) or EMAIL_RE.fullmatch(clean):
            continue
        if UNIT_LINE_RE.fullmatch(clean) or TIME_LINE_RE.fullmatch(clean):
            continue
        if clean.startswith("+") and PHONE_RE.search(clean):
            continue
        lines.append(clean)
    joined = " ".join(lines)
    joined = re.sub(r"<attached:\s*[^>]+>", "", joined, flags=re.IGNORECASE)
    joined = re.sub(r"<This message was edited>", "", joined, flags=re.IGNORECASE)
    joined = re.sub(r"\b(?:photo|video|audio|file) attached\b", "", joined, flags=re.IGNORECASE)
    joined = re.sub(r"\s+", " ", joined).strip().casefold()
    return re.sub(r"[^\w]+", " ", joined).strip()


def _row_text_key(row: RawMessage) -> str:
    return _text_key(strip_reply_context_from_text(row.text, row.attachments))


def _candidate_rows(session, ts_epoch: int | None, *, window: int = 180) -> list[RawMessage]:
    query = session.query(RawMessage)
    if ts_epoch is not None:
        query = query.filter(
            RawMessage.ts_epoch.is_not(None),
            RawMessage.ts_epoch >= int(ts_epoch) - window,
            RawMessage.ts_epoch <= int(ts_epoch) + window,
        )
    return query.order_by(RawMessage.ts_epoch.asc().nullslast()).all()


def _best_text_match(session, message: MediaMessage, text: str | None = None) -> RawMessage | None:
    key = _text_key(text if text is not None else message.text_for_storage)
    if not key:
        return None
    best: tuple[int, RawMessage] | None = None
    for row in _candidate_rows(session, message.ts_epoch):
        row_key = _row_text_key(row)
        if not row_key:
            continue
        score = 0
        if row_key == key:
            score = 100
        elif len(key) >= 8 and key in row_key:
            score = 85
        elif len(row_key) >= 8 and row_key in key:
            score = 75
        if score <= 0:
            continue
        delta = abs(int(row.ts_epoch or 0) - int(message.ts_epoch or 0)) if message.ts_epoch is not None else 0
        score -= min(delta // 10, 20)
        if best is None or score > best[0]:
            best = (score, row)
    return best[1] if best and best[0] >= 65 else None


def _neighbor_text(parsed_messages: list[ParsedMessage], message: MediaMessage) -> str | None:
    sender = _clean(message.parsed.sender).casefold()
    candidates: list[tuple[int, str]] = []
    for idx, other in enumerate(parsed_messages):
        other_ts_epoch = parse_ts_to_epoch(other.ts_iso)
        if idx == message.index or other_ts_epoch is None or message.ts_epoch is None:
            continue
        if _clean(other.sender).casefold() != sender:
            continue
        if is_media_placeholder_text(other.text):
            continue
        key = _text_key(other.text)
        if not key:
            continue
        delta = int(other_ts_epoch) - int(message.ts_epoch)
        if abs(delta) > 180:
            continue
        # Prefer a nearby explanatory message after a captionless media row.
        preference = abs(delta) + (0 if delta >= 0 else 60)
        candidates.append((preference, other.text))
    if not candidates:
        return None
    return sorted(candidates, key=lambda item: item[0])[0][1]


def _find_existing_target(session, parsed_messages: list[ParsedMessage], message: MediaMessage) -> RawMessage | None:
    match = _best_text_match(session, message)
    if match is not None:
        return match
    if is_media_placeholder_text(message.parsed.text) or not _text_key(message.parsed.text):
        neighbor = _neighbor_text(parsed_messages, message)
        if neighbor:
            return _best_text_match(session, message, neighbor)
    return None


def _upsert_unmatched_media(session, message: MediaMessage) -> tuple[RawMessage, bool]:
    sender = _clean(message.parsed.sender)
    chat_name = _clean(message.parsed.chat_name)
    ts = message.ts_iso or message.parsed.ts_iso or ""
    mid = compute_message_id(chat_name, sender, ts, message.text_for_storage)
    existing = session.get(RawMessage, mid)
    if existing:
        existing.attachments = merge_attachment_manifests(existing.attachments, message.manifest)
        return existing, False
    row = RawMessage(
        message_id=mid,
        chat_name=chat_name,
        sender=sender,
        sender_hash=sender_hash(sender),
        ts_iso=ts,
        ts_epoch=message.ts_epoch,
        text=message.text_for_storage,
        attachments=message.manifest,
        source="export_media",
    )
    session.add(row)
    session.flush()
    return row, True


def _sync_all_sheets() -> None:
    sync_incidents_to_sheets()
    sync_dashboard_to_sheets()
    sync_coverage_to_sheets()
    sync_311_cases_to_sheets()
    sync_311_queue_to_sheets()
    sync_decisions_to_sheets()
    sync_public_updates_to_sheets()


def _repair_reply_context_rows(session) -> int:
    changed = 0
    rows = session.query(RawMessage).filter(RawMessage.attachments.is_not(None)).all()
    for row in rows:
        stripped = strip_reply_context_from_text(row.text, row.attachments)
        if stripped and stripped != _clean(row.text):
            row.text = stripped
            changed += 1

    incident = (
        session.query(Incident)
        .filter(Incident.summary == "North lift working. No longer.")
        .order_by(Incident.last_ts_epoch.desc().nullslast())
        .first()
    )
    if incident is not None:
        incident.title = "North lift reported no longer working"
        incident.summary = "North lift was reported no longer working after an earlier working update."
        changed += 1
        decision = session.get(MessageDecision, "26887a6e969805964a284e29e8551b5f711152664ddb2d208b86c6ce5d92b950")
        if decision and decision.final_json:
            try:
                payload = json.loads(decision.final_json)
                if isinstance(payload, dict):
                    payload["title"] = incident.title
                    payload["summary"] = incident.summary
                    decision.final_json = json.dumps(payload, ensure_ascii=False)
            except Exception:
                pass
    return changed


def import_media_zip(zip_path: Path, *, chat_name: str, repair_reply_context: bool, sync_sheets: bool) -> dict[str, int]:
    with zipfile.ZipFile(zip_path) as zf:
        names = set(zf.namelist())
        txt_name = "_chat.txt" if "_chat.txt" in names else next((name for name in names if name.lower().endswith(".txt")), None)
        if not txt_name:
            raise SystemExit("No .txt chat file found in ZIP")
        parsed = parse_export_text(zf.read(txt_name).decode("utf-8", errors="replace"), chat_name=chat_name)

        media_messages: list[MediaMessage] = []
        for idx, message in enumerate(parsed):
            attachment_names = _attachment_names(message.attachments, names)
            if not attachment_names:
                continue
            ts_epoch = parse_ts_to_epoch(message.ts_iso)
            media_messages.append(
                MediaMessage(
                    index=idx,
                    parsed=message,
                    ts_epoch=ts_epoch,
                    ts_iso=epoch_to_iso(ts_epoch) or message.ts_iso,
                    attachment_names=attachment_names,
                    manifest=_extract_media_files(zf, zip_path, chat_name, message, attachment_names),
                    text_for_storage=_message_text_for_storage(message, attachment_names),
                )
            )

    stats = {
        "media_messages": len(media_messages),
        "media_files": sum(len(message.attachment_names) for message in media_messages),
        "merged_existing": 0,
        "inserted_media_rows": 0,
        "processed_inserted_rows": 0,
        "reply_rows_repaired": 0,
    }
    inserted_to_process: list[str] = []
    with get_session() as session:
        if repair_reply_context:
            stats["reply_rows_repaired"] = _repair_reply_context_rows(session)

        for message in media_messages:
            if not message.manifest:
                continue
            target = _find_existing_target(session, parsed, message)
            if target is not None:
                merged = merge_attachment_manifests(target.attachments, message.manifest)
                if merged != target.attachments:
                    target.attachments = merged
                    stats["merged_existing"] += 1
                continue

            row, inserted = _upsert_unmatched_media(session, message)
            if inserted:
                stats["inserted_media_rows"] += 1
                if ISSUE_RE.search(message.text_for_storage):
                    inserted_to_process.append(row.message_id)
                else:
                    session.merge(
                        MessageDecision(
                            message_id=row.message_id,
                            chosen_source="media_attachment",
                            is_issue=False,
                            category=None,
                            event_type="non_issue",
                            confidence=95,
                            needs_review=False,
                            rules_json=json.dumps({"kind": "media_attachment"}, ensure_ascii=False),
                            llm_json=json.dumps({}, ensure_ascii=False),
                            final_json=json.dumps({"is_issue": False, "event_type": "non_issue"}, ensure_ascii=False),
                        )
                    )
        session.commit()

    for message_id in inserted_to_process:
        with get_session() as session:
            row = session.get(RawMessage, message_id)
            if row is None:
                continue
            classify_and_upsert_incident(session, row)
            session.commit()
            stats["processed_inserted_rows"] += 1

    if sync_sheets:
        _sync_all_sheets()

    append_audit_event("IMPORT_WHATSAPP_MEDIA_ZIP", None, stats | {"zip": zip_path.name})
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Attach media from a WhatsApp ZIP export to existing messages and sheets.")
    parser.add_argument("zip_path", type=Path)
    parser.add_argument("--chat-name", default="455 Tenants")
    parser.add_argument("--no-repair-reply-context", action="store_true")
    parser.add_argument("--no-sync-sheets", action="store_true")
    args = parser.parse_args()

    result = import_media_zip(
        args.zip_path,
        chat_name=args.chat_name,
        repair_reply_context=not args.no_repair_reply_context,
        sync_sheets=not args.no_sync_sheets,
    )
    print(result)


if __name__ == "__main__":
    main()
