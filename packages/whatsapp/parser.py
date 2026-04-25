from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import List, Optional, Tuple

@dataclass
class ParsedMessage:
    chat_name: str
    sender: str
    ts_iso: Optional[str]
    text: str
    attachments: Optional[str] = None

DASH_RE = re.compile(
    r"^(?P<date>\d{1,2}[\./]\d{1,2}[\./]\d{2,4}),\s*(?P<time>\d{1,2}:\d{2}(?::\d{2})?)"
    r"(?:[\s\u202f]*(?P<ampm>AM|PM))?\s*-\s*(?P<body>.*)$"
)
BRACKET_RE = re.compile(
    r"^\u200e?\[(?P<date>\d{1,2}[\./]\d{1,2}[\./]\d{2,4}),\s*(?P<time>\d{1,2}:\d{2}(?::\d{2})?)"
    r"(?:[\s\u202f]*(?P<ampm>AM|PM))?\]\s*(?P<body>.*)$"
)
ATTACH_RE = re.compile(r"<attached:\s*([^>]+)>", re.IGNORECASE)
MEDIA_OMITTED_RE = re.compile(r"^(?P<kind>image|video|audio|sticker|gif|document|file)\s+omitted$", re.IGNORECASE)

def _norm(s: str) -> str:
    s = (s or "").replace("\u202f", " ").replace("\u200e", "").replace("\u200f", "")
    return unicodedata.normalize("NFKC", s).strip()

def _parse_header(line: str) -> Tuple[Optional[str], Optional[str]]:
    line = _norm(line)
    m = BRACKET_RE.match(line)
    if m:
        ts = f"{m.group('date')} {m.group('time')} {(m.group('ampm') or '').strip()}".strip()
        return ts, m.group("body")
    m = DASH_RE.match(line)
    if m:
        ts = f"{m.group('date')} {m.group('time')} {(m.group('ampm') or '').strip()}".strip()
        return ts, m.group("body")
    return None, None


def omitted_media_kind(text: str | None) -> Optional[str]:
    match = MEDIA_OMITTED_RE.fullmatch(_norm(text))
    if not match:
        return None
    return match.group("kind").lower()


def is_media_placeholder_text(text: str | None) -> bool:
    return omitted_media_kind(text) is not None

def parse_export_text(text: str, chat_name: str = "Tenants WhatsApp") -> List[ParsedMessage]:
    msgs: List[ParsedMessage] = []
    cur: ParsedMessage | None = None

    for raw_line in text.splitlines():
        line = _norm(raw_line)
        ts, body = _parse_header(line)
        if ts is not None and body is not None:
            if cur:
                msgs.append(cur)

            sender = "SYSTEM"
            msg_text = body
            if ": " in body:
                sender, msg_text = body.split(": ", 1)

            sender = _norm(sender)
            msg_text = _norm(msg_text)

            atts = ATTACH_RE.findall(msg_text)
            attachments = ",".join([_norm(a) for a in atts]) if atts else None
            if atts:
                msg_text = _norm(ATTACH_RE.sub("", msg_text))
                if not msg_text:
                    first = _norm(atts[0]).casefold()
                    if "video" in first:
                        msg_text = "video omitted"
                    elif "audio" in first:
                        msg_text = "audio omitted"
                    elif "sticker" in first:
                        msg_text = "sticker omitted"
                    elif "gif" in first:
                        msg_text = "gif omitted"
                    elif first.endswith((".pdf", ".doc", ".docx", ".xls", ".xlsx", ".txt")):
                        msg_text = "document omitted"
                    else:
                        msg_text = "image omitted"
            omitted_kind = omitted_media_kind(msg_text)
            if omitted_kind and not attachments:
                attachments = f"omitted:{omitted_kind}"

            cur = ParsedMessage(chat_name=chat_name, sender=sender, ts_iso=ts, text=msg_text, attachments=attachments)
        else:
            if cur and line:
                cur.text += "\n" + line

    if cur:
        msgs.append(cur)
    return msgs
