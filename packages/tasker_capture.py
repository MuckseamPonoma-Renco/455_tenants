from __future__ import annotations

import os
import re
from dataclasses import dataclass

from packages.audit import sender_hash
from packages.db import RawMessage

PLACEHOLDER_RE = re.compile(r"^%[A-Za-z0-9_]+$")
CHAT_SENDER_RE = re.compile(r"^(?P<chat>.+?)(?: \((?:\d+) messages?\))?(?:: (?P<sender>.+))?$")
TEXT_SENDER_RE = re.compile(r"^(?:~\s*)?(?P<sender>[^:]{1,120}): (?P<text>.+)$")


def _clean(value: str | None) -> str:
    return (value or "").replace("\u202f", " ").replace("\u200e", "").replace("\u200f", "").strip()


def _is_placeholder(value: str | None) -> bool:
    return bool(PLACEHOLDER_RE.fullmatch(_clean(value)))


def _clean_sender(value: str | None) -> str:
    sender = _clean(value)
    while sender.startswith("~"):
        sender = sender[1:].strip()
    return sender


@dataclass(frozen=True)
class NormalizedTaskerCapture:
    chat_name: str
    sender: str
    text: str

    @property
    def sender_hash(self) -> str:
        return sender_hash(self.sender)

    @property
    def signature(self) -> tuple[str, str, str]:
        return (self.chat_name, self.sender, self.text)


def tasker_duplicate_window_seconds() -> int:
    raw = (os.environ.get("TASKER_DUPLICATE_WINDOW_SECONDS") or "").strip()
    try:
        return max(0, int(raw))
    except Exception:
        return 180


def normalize_tasker_capture(chat_name: str | None, sender: str | None, text: str | None) -> NormalizedTaskerCapture:
    raw_chat = _clean(chat_name)
    raw_sender = _clean(sender)
    raw_text = _clean(text)

    chat = "" if _is_placeholder(raw_chat) else raw_chat
    resolved_sender = "" if _is_placeholder(raw_sender) else _clean_sender(raw_sender)
    resolved_text = "" if _is_placeholder(raw_text) else raw_text

    parsed_sender = ""
    if chat:
        match = CHAT_SENDER_RE.match(chat)
        if match:
            chat = _clean(match.group("chat"))
            parsed_sender = _clean_sender(match.group("sender"))

    if not resolved_sender and parsed_sender:
        resolved_sender = parsed_sender

    if not resolved_sender and resolved_text:
        match = TEXT_SENDER_RE.match(resolved_text)
        if match:
            resolved_sender = _clean_sender(match.group("sender"))
            resolved_text = _clean(match.group("text"))

    return NormalizedTaskerCapture(
        chat_name=chat,
        sender=resolved_sender,
        text=resolved_text,
    )


def find_recent_tasker_duplicate(
    session,
    *,
    chat_name: str | None,
    sender: str | None,
    text: str | None,
    ts_epoch: int | None,
) -> RawMessage | None:
    if ts_epoch is None:
        return None

    target = normalize_tasker_capture(chat_name, sender, text)
    if not target.text:
        return None

    window = tasker_duplicate_window_seconds()
    if window <= 0:
        return None

    candidates = (
        session.query(RawMessage)
        .filter(
            RawMessage.source == "tasker",
            RawMessage.ts_epoch.is_not(None),
            RawMessage.ts_epoch >= int(ts_epoch) - window,
            RawMessage.ts_epoch <= int(ts_epoch) + window,
        )
        .all()
    )
    for row in sorted(candidates, key=lambda item: abs(int(item.ts_epoch or 0) - int(ts_epoch))):
        if normalize_tasker_capture(row.chat_name, row.sender, row.text).signature == target.signature:
            return row
    return None
