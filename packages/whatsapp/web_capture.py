from __future__ import annotations

import base64
import hashlib
import json
import mimetypes
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Sequence

import httpx
from playwright.sync_api import BrowserContext, Download, Error as PlaywrightError, Locator, Page, sync_playwright

from packages.audit import append_audit_event
from packages.timeutil import NY, epoch_to_iso, parse_ts_to_epoch
from packages.whatsapp.attachments import build_attachment_manifest, make_attachment_item
from packages.whatsapp.status import default_status_path, write_capture_status

WHATSAPP_WEB_URL = "https://web.whatsapp.com/"
DEFAULT_CAPTURE_ROOT = Path.home() / ".local" / "share" / "tenant-issue-os" / "whatsapp_capture"
META_RE = re.compile(r"^\[(?P<first>[^,\]]+),\s*(?P<second>[^\]]+)\]\s*(?P<sender>.*?)(?::\s*)?$")
NUMERIC_DATE_RE = re.compile(r"^(?P<a>\d{1,2})[./-](?P<b>\d{1,2})[./-](?P<y>\d{2,4})$")
SEARCH_SELECTORS = (
    '#side div[contenteditable="true"][aria-label*="Search"]',
    '#side div[contenteditable="true"][title*="Search"]',
    '#side [role="textbox"][contenteditable="true"]',
    '#side div[contenteditable="true"][data-tab]',
)
READY_SELECTORS = (
    "#pane-side",
    "#side",
    "#main",
)
LOGIN_REQUIRED_SELECTORS = (
    "canvas",
    "[data-testid='qrcode']",
)
MESSAGE_NODE_SELECTOR = "#main [data-pre-plain-text]"
DOWNLOAD_SELECTORS = (
    'button[aria-label*="Download"]',
    '[title*="Download"]',
    'span[data-icon="download"]',
    'span[data-icon="download-filled"]',
)
OPEN_MEDIA_SELECTORS = (
    "img",
    "video",
    "[data-icon='document']",
    "[data-testid*='media-viewer']",
)


def _log(message: str) -> None:
    print(message, flush=True)


def _clean(value: str | None) -> str:
    return (value or "").replace("\u202f", " ").replace("\u200e", "").replace("\u200f", "").strip()


def _slug(value: str) -> str:
    clean = _clean(value).lower()
    slug = re.sub(r"[^a-z0-9]+", "_", clean).strip("_")
    return slug or "chat"


def _bool_env(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except Exception:
        return default


def _split_chat_names(values: Sequence[str] | str | None) -> tuple[str, ...]:
    if values is None:
        return ()
    parts = re.split(r"[\n,]+", values) if isinstance(values, str) else list(values)
    out: list[str] = []
    seen: set[str] = set()
    for item in parts:
        clean = _clean(item)
        if not clean:
            continue
        lowered = clean.casefold()
        if lowered in seen:
            continue
        seen.add(lowered)
        out.append(clean)
    return tuple(out)


def _default_user_data_dir() -> Path:
    return DEFAULT_CAPTURE_ROOT / "chrome_profile"


def _default_state_path() -> Path:
    return DEFAULT_CAPTURE_ROOT / "state.json"


def _default_status_path() -> Path:
    return default_status_path()


def _default_media_dir() -> Path:
    return Path(".local/whatsapp_media")


def _candidate_api_bases(explicit_base: str | None) -> tuple[str, ...]:
    candidates = (
        _clean(explicit_base),
        "http://127.0.0.1:8000",
        _clean(os.environ.get("PUBLIC_BASE_URL")),
    )
    out: list[str] = []
    seen: set[str] = set()
    for raw in candidates:
        if not raw:
            continue
        base = raw.rstrip("/")
        lowered = base.casefold()
        if lowered in seen:
            continue
        seen.add(lowered)
        out.append(base)
    return tuple(out)


def _fingerprint(chat_name: str, sender: str, text: str, ts_iso: str | None, ts_epoch: int | None) -> str:
    payload = f"{_clean(chat_name)}|{_clean(sender)}|{_clean(ts_iso) or ts_epoch or ''}|{_clean(text)}"
    return hashlib.sha256(payload.encode("utf-8", errors="replace")).hexdigest()


def _normalized_chat_title(chat_name: str) -> str:
    return " ".join(_clean(chat_name).split())


def _try_parse_numeric_date(date_part: str) -> list[str]:
    match = NUMERIC_DATE_RE.fullmatch(_clean(date_part))
    if not match:
        return [_clean(date_part)]

    a = int(match.group("a"))
    b = int(match.group("b"))
    y = int(match.group("y"))
    if y < 100:
        y += 2000

    candidates: list[tuple[int, int, int]] = []
    if 1 <= a <= 12 and 1 <= b <= 31:
        candidates.append((a, b, y))
    if 1 <= b <= 12 and 1 <= a <= 31 and (b, a, y) not in candidates:
        candidates.append((b, a, y))

    out: list[str] = []
    for month, day, year in candidates:
        out.append(f"{month}/{day}/{year}")
    return out or [_clean(date_part)]


def _normalize_date_token(date_part: str) -> list[str]:
    clean = _clean(date_part)
    lowered = clean.casefold()
    now = datetime.now(NY)
    if lowered == "today":
        return [now.strftime("%-m/%-d/%Y")]
    if lowered == "yesterday":
        return [(now - timedelta(days=1)).strftime("%-m/%-d/%Y")]
    return _try_parse_numeric_date(clean)


def _parse_meta_timestamp(first: str, second: str) -> tuple[str | None, int | None]:
    parts = (_clean(first), _clean(second))
    if ":" in parts[0] and ":" not in parts[1]:
        time_part, date_part = parts
    elif ":" in parts[1] and ":" not in parts[0]:
        date_part, time_part = parts
    else:
        date_part, time_part = parts

    for candidate_date in _normalize_date_token(date_part):
        ts_label = f"{candidate_date} {time_part}".strip()
        ts_epoch = parse_ts_to_epoch(ts_label)
        if ts_epoch is not None:
            return ts_label, ts_epoch
    ts_label = f"{date_part} {time_part}".strip()
    return ts_label or None, parse_ts_to_epoch(ts_label)


@dataclass(frozen=True)
class WhatsAppCaptureCandidate:
    chat_name: str
    sender: str
    text: str
    ts_iso: str | None
    ts_epoch: int | None
    fingerprint: str
    row: dict[str, Any]


@dataclass(frozen=True)
class WhatsAppCaptureMessage:
    chat_name: str
    sender: str
    text: str
    ts_iso: str | None
    ts_epoch: int | None
    fingerprint: str
    attachments: str | None = None


@dataclass(frozen=True)
class WhatsAppCaptureConfig:
    chat_names: tuple[str, ...]
    ingest_token: str
    api_bases: tuple[str, ...]
    headless: bool
    poll_seconds: int
    message_limit: int
    max_scroll_pages: int
    user_data_dir: Path
    state_path: Path
    status_path: Path
    media_dir: Path
    browser_channel: str
    login_timeout_seconds: int
    prime_visible_messages: bool


@dataclass(frozen=True)
class PlaywrightCaptureRuntime:
    playwright: Any
    context: BrowserContext


class CaptureStateStore:
    def __init__(self, path: str | Path, *, max_seen: int = 5000):
        self.path = Path(path).expanduser()
        self.max_seen = max(100, max_seen)
        self._seen_entries: list[dict[str, str]] = []
        self._legacy_seen: list[str] = []
        self._primed_chats: set[str] = set()
        self.load()

    def load(self) -> None:
        if not self.path.exists():
            return
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return
        entries = payload.get("seen_entries") or []
        self._seen_entries = [
            {
                "chat_name": _normalized_chat_title(str(item.get("chat_name") or "")),
                "fingerprint": str(item.get("fingerprint") or ""),
            }
            for item in entries
            if isinstance(item, dict) and item.get("fingerprint")
        ][: self.max_seen]
        self._legacy_seen = [str(item) for item in (payload.get("seen") or []) if item][: self.max_seen]
        primed = payload.get("primed_chats") or []
        self._primed_chats = {_normalized_chat_title(str(item)) for item in primed if item}

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "seen_entries": self._seen_entries[-self.max_seen :],
            "seen": self._legacy_seen[-self.max_seen :],
            "primed_chats": sorted(self._primed_chats),
        }
        self.path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    def has(self, chat_name: str, fingerprint: str) -> bool:
        normalized_chat = _normalized_chat_title(chat_name)
        if fingerprint in self._legacy_seen:
            return True
        return any(entry["chat_name"] == normalized_chat and entry["fingerprint"] == fingerprint for entry in self._seen_entries)

    def remember(self, chat_name: str, *fingerprints: str) -> None:
        normalized_chat = _normalized_chat_title(chat_name)
        existing = {(entry["chat_name"], entry["fingerprint"]) for entry in self._seen_entries}
        for fingerprint in fingerprints:
            if not fingerprint:
                continue
            key = (normalized_chat, fingerprint)
            if key in existing:
                continue
            self._seen_entries.append({"chat_name": normalized_chat, "fingerprint": fingerprint})
            existing.add(key)
        if len(self._seen_entries) > self.max_seen:
            self._seen_entries = self._seen_entries[-self.max_seen :]

    def is_primed(self, chat_name: str) -> bool:
        return _normalized_chat_title(chat_name) in self._primed_chats

    def mark_primed(self, chat_name: str) -> None:
        self._primed_chats.add(_normalized_chat_title(chat_name))

    def reset_chat(self, chat_name: str) -> bool:
        normalized_chat = _normalized_chat_title(chat_name)
        before_entries = len(self._seen_entries)
        self._seen_entries = [entry for entry in self._seen_entries if entry["chat_name"] != normalized_chat]
        removed = len(self._seen_entries) != before_entries
        if normalized_chat in self._primed_chats:
            self._primed_chats.remove(normalized_chat)
            removed = True
        if not self._seen_entries and len(self._primed_chats) <= 1 and self._legacy_seen:
            self._legacy_seen = []
            removed = True
        if removed:
            self.save()
        return removed


def capture_config_from_env(
    *,
    chat_names: Sequence[str] | None = None,
    api_base: str | None = None,
    headless: bool | None = None,
    poll_seconds: int | None = None,
    message_limit: int | None = None,
    user_data_dir: str | Path | None = None,
    state_path: str | Path | None = None,
    status_path: str | Path | None = None,
    media_dir: str | Path | None = None,
    browser_channel: str | None = None,
    login_timeout_seconds: int | None = None,
    prime_visible_messages: bool | None = None,
) -> WhatsAppCaptureConfig:
    resolved_chat_names = _split_chat_names(chat_names or os.environ.get("WHATSAPP_CAPTURE_CHAT_NAMES"))
    token = _clean(os.environ.get("INGEST_TOKEN"))
    if not token:
        raise RuntimeError("INGEST_TOKEN is required for WhatsApp Web capture")
    if not resolved_chat_names:
        raise RuntimeError("Set WHATSAPP_CAPTURE_CHAT_NAMES or pass --chat at least once")

    return WhatsAppCaptureConfig(
        chat_names=resolved_chat_names,
        ingest_token=token,
        api_bases=_candidate_api_bases(api_base or os.environ.get("WHATSAPP_CAPTURE_API_BASE")),
        headless=_bool_env("WHATSAPP_CAPTURE_HEADLESS", False) if headless is None else headless,
        poll_seconds=max(5, poll_seconds or _int_env("WHATSAPP_CAPTURE_POLL_SECONDS", 30)),
        message_limit=max(5, message_limit or _int_env("WHATSAPP_CAPTURE_MESSAGE_LIMIT", 30)),
        max_scroll_pages=max(1, _int_env("WHATSAPP_CAPTURE_SCROLL_PAGES", 8)),
        user_data_dir=Path(user_data_dir or os.environ.get("WHATSAPP_CAPTURE_USER_DATA_DIR") or _default_user_data_dir()).expanduser(),
        state_path=Path(state_path or os.environ.get("WHATSAPP_CAPTURE_STATE_PATH") or _default_state_path()).expanduser(),
        status_path=Path(status_path or os.environ.get("WHATSAPP_CAPTURE_STATUS_PATH") or _default_status_path()).expanduser(),
        media_dir=Path(media_dir or os.environ.get("WHATSAPP_CAPTURE_MEDIA_DIR") or _default_media_dir()).expanduser(),
        browser_channel=_clean(browser_channel or os.environ.get("WHATSAPP_CAPTURE_BROWSER_CHANNEL") or "chrome"),
        login_timeout_seconds=max(0, login_timeout_seconds if login_timeout_seconds is not None else _int_env("WHATSAPP_CAPTURE_LOGIN_TIMEOUT_SECONDS", 0)),
        prime_visible_messages=_bool_env("WHATSAPP_CAPTURE_PRIME_VISIBLE", True) if prime_visible_messages is None else prime_visible_messages,
    )


def parse_whatsapp_message_meta(pre_plain_text: str) -> tuple[str, str | None, int | None]:
    clean = _clean(pre_plain_text)
    if not clean:
        return "", None, None
    match = META_RE.match(clean)
    if not match:
        return "", None, None
    ts_iso, ts_epoch = _parse_meta_timestamp(match.group("first"), match.group("second"))
    sender = _clean(match.group("sender"))
    return sender, epoch_to_iso(ts_epoch) or ts_iso, ts_epoch


def _first_existing_locator(page: Page, selectors: Sequence[str]) -> Locator | None:
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            if locator.count() > 0:
                return locator
        except PlaywrightError:
            continue
    return None


def _chat_locator(page: Page, chat_name: str) -> Locator | None:
    containers = [page.locator("#pane-side"), page.locator("#side")]
    for container in containers:
        candidate = container.get_by_title(chat_name, exact=True).first
        try:
            if candidate.count() > 0:
                return candidate
        except PlaywrightError:
            continue
        fallback = container.get_by_text(chat_name, exact=True).first
        try:
            if fallback.count() > 0:
                return fallback
        except PlaywrightError:
            continue
    return None


def _clear_editable(locator: Locator) -> None:
    locator.click(timeout=5_000)
    locator.press("Meta+A")
    locator.press("Backspace")


def _open_chat(page: Page, chat_name: str) -> None:
    direct = _chat_locator(page, chat_name)
    if direct is not None:
        direct.click(timeout=5_000)
        page.wait_for_timeout(700)
        return

    search_box = _first_existing_locator(page, SEARCH_SELECTORS)
    if search_box is None:
        raise RuntimeError("WhatsApp sidebar search box not found")

    _clear_editable(search_box)
    search_box.fill(chat_name, timeout=5_000)
    page.wait_for_timeout(1_200)

    direct = _chat_locator(page, chat_name)
    if direct is None:
        raise RuntimeError(f'WhatsApp chat "{chat_name}" was not found in Chrome')

    direct.click(timeout=5_000)
    page.wait_for_timeout(900)


def _extract_visible_rows(page: Page) -> list[dict[str, Any]]:
    rows = page.evaluate(
        f"""
        () => {{
          const nodes = Array.from(document.querySelectorAll({MESSAGE_NODE_SELECTOR!r}));
          return nodes.map((node, domIndex) => {{
            const captureId = `tenant-capture-${{Date.now()}}-${{domIndex}}-${{Math.random().toString(36).slice(2)}}`;
            node.setAttribute('data-tenant-capture-id', captureId);
            const prePlainText = (node.getAttribute('data-pre-plain-text') || '').trim();
            const selectable = Array.from(node.querySelectorAll('span.selectable-text, div.selectable-text'))
              .map(el => (el.innerText || '').trim())
              .filter(Boolean);
            const links = Array.from(node.querySelectorAll('a[href]'))
              .map(el => (el.href || '').trim())
              .filter(Boolean);
            const bodyText = (node.innerText || '').trim();
            const imageSourceLooksUserMedia = (src) => {{
              const clean = String(src || '').trim().toLowerCase();
              return clean.startsWith('blob:') || clean.startsWith('data:image/');
            }};
            const mediaSizeFor = (el) => {{
              const rect = el.getBoundingClientRect();
              return {{
                width: Math.round(el.naturalWidth || el.width || rect.width || 0),
                height: Math.round(el.naturalHeight || el.height || rect.height || 0),
                area: Math.round((rect.width || el.naturalWidth || el.width || 0) * (rect.height || el.naturalHeight || el.height || 0)),
              }};
            }};
            const imageElementLooksUserMedia = (el) => {{
              const size = mediaSizeFor(el);
              if (size.width < 96 || size.height < 96 || size.area < 9000) return false;
              if (el.tagName === 'CANVAS') return true;
              const src = el.currentSrc || el.src || '';
              return imageSourceLooksUserMedia(src);
            }};
            const hasCssBackgroundImage = Array.from(node.querySelectorAll('div, span')).some(el => {{
              const labels = [
                el.getAttribute('aria-label'),
                el.getAttribute('title'),
                el.getAttribute('data-testid'),
                el.getAttribute('data-icon'),
              ].join(' ');
              if (!/photo|image|picture|media/i.test(labels)) return false;
              const size = mediaSizeFor(el);
              if (size.width < 96 || size.height < 96 || size.area < 9000) return false;
              const inlineStyle = String(el.getAttribute('style') || '');
              const inlineMatch = inlineStyle.match(/background-image\\s*:\\s*url\\(["']?(.*?)["']?\\)/i);
              if (inlineMatch && imageSourceLooksUserMedia(inlineMatch[1])) return true;
              try {{
                const background = window.getComputedStyle(el).backgroundImage || '';
                const match = background.match(/^url\\(["']?(.*?)["']?\\)$/);
                return Boolean(match && imageSourceLooksUserMedia(match[1]));
              }} catch (_) {{
                return false;
              }}
            }});
            const hasInlineUserImage = Array.from(node.querySelectorAll('img[src], img[srcset], canvas')).some(imageElementLooksUserMedia);
            const hasImageControl = Boolean(node.querySelector([
              '[aria-label*="Photo" i]',
              '[aria-label*="Image" i]',
              '[aria-label*="picture" i]',
              '[aria-label*="Open media" i]',
              '[data-testid*="image" i]',
              '[data-testid*="media-photo" i]',
            ].join(',')));
            const hasImageMedia = hasInlineUserImage || hasCssBackgroundImage || hasImageControl;
            const mediaKinds = [];
            if (hasImageMedia) mediaKinds.push('image');
            if (node.querySelector('video, [aria-label*="Video" i], [data-testid*="video" i]')) mediaKinds.push('video');
            if (node.querySelector('[data-icon="document"], [data-testid*="media-document"], a[download]')) mediaKinds.push('document');
            if (node.querySelector('[data-icon="audio-download"], audio, [data-testid*="audio-player"]')) mediaKinds.push('audio');
            const replyLabel = Array.from(node.querySelectorAll('[aria-label*="Quoted"], [data-testid*="quoted"]'))
              .map(el => (el.innerText || '').trim())
              .filter(Boolean)
              .join('\\n')
              .trim();
            const caption = selectable.length ? selectable[selectable.length - 1] : '';
            return {{
              capture_id: captureId,
              dom_index: domIndex,
              pre_plain_text: prePlainText,
              text: selectable.join('\\n').trim() || bodyText,
              caption,
              body_text: bodyText,
              links,
              media_kinds: Array.from(new Set(mediaKinds)),
              has_download_button: Boolean(node.querySelector('button[aria-label*="Download"], [title*="Download"], span[data-icon="download"], span[data-icon="download-filled"]')),
              reply_text: replyLabel,
            }};
          }});
        }}
        """
    )
    return [row for row in rows if isinstance(row, dict)]


def _media_placeholder_text(row: dict[str, Any]) -> str:
    kinds = [kind for kind in row.get("media_kinds") or [] if isinstance(kind, str) and kind]
    first = kinds[0] if kinds else "media"
    if first == "document":
        return "document omitted"
    return f"{first} omitted"


def _candidate_from_row(chat_name: str, row: dict[str, Any]) -> WhatsAppCaptureCandidate | None:
    pre_plain_text = _clean(str(row.get("pre_plain_text") or ""))
    sender, ts_iso, ts_epoch = parse_whatsapp_message_meta(pre_plain_text)
    text = _clean(str(row.get("caption") or "")) or _clean(str(row.get("text") or ""))
    if not text and (row.get("media_kinds") or []):
        text = _media_placeholder_text(row)
    if not text:
        return None
    fingerprint = _fingerprint(chat_name, sender, text, ts_iso, ts_epoch)
    return WhatsAppCaptureCandidate(
        chat_name=chat_name,
        sender=sender,
        text=text,
        ts_iso=ts_iso,
        ts_epoch=ts_epoch,
        fingerprint=fingerprint,
        row=row,
    )


def _is_ready(page: Page) -> bool:
    for selector in READY_SELECTORS:
        try:
            if page.locator(selector).count() == 0:
                return False
        except PlaywrightError:
            return False
    return True


def _body_text(page: Page) -> str:
    try:
        return _clean(page.locator("body").inner_text(timeout=2_000))
    except Exception:
        return ""


def _is_login_required(page: Page) -> bool:
    body_text = _body_text(page).casefold()
    phrases = (
        "scan the qr code",
        "log into whatsapp web",
        "keep your phone connected",
        "use whatsapp on your phone",
    )
    if any(phrase in body_text for phrase in phrases):
        return True
    for selector in LOGIN_REQUIRED_SELECTORS:
        try:
            if page.locator(selector).count() > 0 and not _is_ready(page):
                return True
        except PlaywrightError:
            continue
    return False


def _capture_status(config: WhatsAppCaptureConfig, **updates: Any) -> dict[str, Any]:
    return write_capture_status(config.status_path, **updates)


def wait_for_whatsapp_ready(page: Page, *, timeout_seconds: int = 0) -> None:
    deadline = time.monotonic() + timeout_seconds if timeout_seconds > 0 else None
    login_notice_shown = False
    while True:
        try:
            if _is_ready(page):
                return
        except Exception:
            pass

        if not login_notice_shown:
            _log("Waiting for WhatsApp Web in Chrome. If the QR code is visible, scan it once and leave this Chrome profile signed in.")
            login_notice_shown = True

        if deadline is not None and time.monotonic() > deadline:
            raise RuntimeError("Timed out waiting for WhatsApp Web to become ready")
        page.wait_for_timeout(2_000)


def _ensure_session_ready(page: Page, config: WhatsAppCaptureConfig, *, startup: bool = False) -> bool:
    if _is_ready(page):
        _capture_status(config, state="ready", login_required=False, last_ready_at=epoch_to_iso(int(time.time())))
        return True

    if _is_login_required(page):
        if startup:
            wait_for_whatsapp_ready(page, timeout_seconds=config.login_timeout_seconds)
            _capture_status(config, state="ready", login_required=False, last_ready_at=epoch_to_iso(int(time.time())))
            return True
        _capture_status(config, state="login_required", login_required=True)
        append_audit_event("WHATSAPP_WEB_CAPTURE_LOGIN_REQUIRED", None, {"chat_names": list(config.chat_names)})
        _log("WhatsApp Web needs a fresh login. Scan the QR code in the Chrome profile and the watcher will resume.")
        return False

    try:
        page.goto(WHATSAPP_WEB_URL, wait_until="domcontentloaded", timeout=120_000)
    except Exception as exc:
        _capture_status(config, state="navigation_error", login_required=False, last_error=str(exc)[:500])
        raise

    if _is_login_required(page):
        if startup:
            wait_for_whatsapp_ready(page, timeout_seconds=config.login_timeout_seconds)
            _capture_status(config, state="ready", login_required=False, last_ready_at=epoch_to_iso(int(time.time())))
            return True
        _capture_status(config, state="login_required", login_required=True)
        append_audit_event("WHATSAPP_WEB_CAPTURE_LOGIN_REQUIRED", None, {"chat_names": list(config.chat_names)})
        _log("WhatsApp Web needs a fresh login. Scan the QR code in the Chrome profile and the watcher will resume.")
        return False

    wait_for_whatsapp_ready(page, timeout_seconds=config.login_timeout_seconds)
    _capture_status(config, state="ready", login_required=False, last_ready_at=epoch_to_iso(int(time.time())))
    return True


def _post_messages(client: httpx.Client, config: WhatsAppCaptureConfig, messages: Sequence[WhatsAppCaptureMessage]) -> dict[str, Any]:
    if not messages:
        return {"ok": True, "inserted": 0, "deduped": 0}

    payload = {
        "items": [
            {
                "chat_name": message.chat_name,
                "text": message.text,
                "sender": message.sender or None,
                "ts_iso": message.ts_iso,
                "ts_epoch": message.ts_epoch,
                "attachments": message.attachments,
            }
            for message in messages
        ]
    }
    headers = {"Authorization": f"Bearer {config.ingest_token}"}
    last_error: Exception | None = None

    for base in config.api_bases:
        try:
            response = client.post(f"{base}/ingest/whatsapp_web_batch", json=payload, headers=headers)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            last_error = exc
            if exc.response.status_code < 500:
                break
        except Exception as exc:
            last_error = exc

    if last_error is None:
        raise RuntimeError("No API base URL was available for WhatsApp Web capture")
    raise RuntimeError(f"Failed to post WhatsApp Web capture: {last_error}")


def _locator_exists(locator: Locator) -> bool:
    try:
        return locator.count() > 0
    except PlaywrightError:
        return False


def _message_locator(page: Page, row: dict[str, Any]) -> Locator:
    capture_id = _clean(str(row.get("capture_id") or ""))
    if capture_id:
        marked = page.locator(f'{MESSAGE_NODE_SELECTOR}[data-tenant-capture-id="{capture_id}"]')
        if _locator_exists(marked):
            return marked.first

    pre_plain_text = _clean(str(row.get("pre_plain_text") or ""))
    expected_text = _clean(str(row.get("caption") or "")) or _clean(str(row.get("text") or "")) or _clean(str(row.get("body_text") or ""))
    fallback_id = f"tenant-capture-fallback-{hashlib.sha256((pre_plain_text + expected_text).encode('utf-8')).hexdigest()[:16]}"
    try:
        matched = page.evaluate(
            f"""
            (args) => {{
              const clean = (value) => String(value || '').replace(/\\u202f/g, ' ').replace(/[\\u200e\\u200f]/g, '').trim();
              const nodes = Array.from(document.querySelectorAll({MESSAGE_NODE_SELECTOR!r}));
              for (const node of nodes) {{
                const pre = clean(node.getAttribute('data-pre-plain-text'));
                const text = clean(node.innerText);
                if (pre === args.prePlainText && (!args.expectedText || text.includes(args.expectedText))) {{
                  node.setAttribute('data-tenant-capture-id', args.captureId);
                  return true;
                }}
              }}
              return false;
            }}
            """,
            {"prePlainText": pre_plain_text, "expectedText": expected_text, "captureId": fallback_id},
        )
        if matched:
            matched_locator = page.locator(f'{MESSAGE_NODE_SELECTOR}[data-tenant-capture-id="{fallback_id}"]')
            if _locator_exists(matched_locator):
                return matched_locator.first
    except Exception:
        pass

    return page.locator(MESSAGE_NODE_SELECTOR).nth(int(row.get("dom_index") or 0))


def _timestamp_slug(ts_iso: str | None) -> str:
    clean = _clean(ts_iso)
    if clean:
        return clean.replace(":", "").replace("-", "").replace("T", "_").replace("Z", "")
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _media_storage_dir(config: WhatsAppCaptureConfig, chat_name: str, ts_iso: str | None) -> Path:
    base = config.media_dir.expanduser()
    day = (_clean(ts_iso)[:10] or datetime.now().strftime("%Y-%m-%d")).replace("/", "-")
    path = base / _slug(chat_name) / day
    path.mkdir(parents=True, exist_ok=True)
    return path


def _save_download(download: Download, target_dir: Path, prefix: str, kind: str) -> str:
    suggested = _clean(download.suggested_filename) or f"{prefix}_{kind}"
    target = target_dir / f"{prefix}_{suggested}"
    download.save_as(str(target))
    return str(target.resolve())


def _download_succeeded(item: dict[str, Any] | None) -> bool:
    return bool(item and item.get("status") == "downloaded" and item.get("path"))


def _try_click_download(page: Page, locator: Locator, target_dir: Path, prefix: str, kind: str) -> dict[str, Any] | None:
    if not _locator_exists(locator):
        return None
    try:
        with page.expect_download(timeout=3_000) as info:
            locator.first.click(force=True)
        download = info.value
        path = _save_download(download, target_dir, prefix, kind)
        return make_attachment_item(
            kind=kind,
            label="downloaded_media",
            status="downloaded",
            path=path,
            filename=Path(path).name,
        )
    except Exception as exc:
        return make_attachment_item(kind=kind, label="download_error", status="download_error", error=str(exc)[:300])


def _image_extension(content_type: str | None) -> str:
    clean = _clean(content_type).split(";", 1)[0].casefold()
    if clean == "image/jpeg":
        return ".jpg"
    if clean == "image/png":
        return ".png"
    if clean == "image/webp":
        return ".webp"
    ext = mimetypes.guess_extension(clean) if clean else None
    return ext or ".png"


def _try_save_inline_image(locator: Locator, target_dir: Path, prefix: str) -> dict[str, Any] | None:
    try:
        payload = locator.evaluate(
            """
            async (root) => {
              const minSide = 96;
              const minArea = 9000;
              const dataUrlToParts = (dataUrl) => {
                const match = String(dataUrl || '').match(/^data:([^;,]+)?;base64,(.*)$/);
                if (!match) return null;
                return {contentType: match[1] || 'image/png', data: match[2] || ''};
              };
              const blobToDataUrl = (blob) => new Promise((resolve, reject) => {
                const reader = new FileReader();
                reader.onload = () => resolve(reader.result);
                reader.onerror = () => reject(reader.error);
                reader.readAsDataURL(blob);
              });
              const sourceLooksUserMedia = (src) => {
                const clean = String(src || '').trim().toLowerCase();
                return clean.startsWith('blob:') || clean.startsWith('data:image/');
              };
              const sizeFor = (el) => {
                const rect = el.getBoundingClientRect();
                return {
                  width: Math.round(el.naturalWidth || el.videoWidth || el.width || rect.width || 0),
                  height: Math.round(el.naturalHeight || el.videoHeight || el.height || rect.height || 0),
                  area: Math.round((rect.width || el.naturalWidth || el.width || 0) * (rect.height || el.naturalHeight || el.height || 0)),
                };
              };
              const candidates = [];
              for (const img of Array.from(root.querySelectorAll('img[src], img[srcset]'))) {
                const size = sizeFor(img);
                if (size.width < minSide || size.height < minSide || size.area < minArea) continue;
                const src = img.currentSrc || img.src || '';
                if (!src) continue;
                if (!sourceLooksUserMedia(src)) continue;
                candidates.push({type: 'url', src, ...size});
              }
              for (const canvas of Array.from(root.querySelectorAll('canvas'))) {
                const size = sizeFor(canvas);
                if (size.width < minSide || size.height < minSide || size.area < minArea) continue;
                candidates.push({type: 'canvas', canvas, ...size});
              }
              for (const el of Array.from(root.querySelectorAll('div, span'))) {
                const size = sizeFor(el);
                if (size.width < minSide || size.height < minSide || size.area < minArea) continue;
                const style = window.getComputedStyle(el);
                const match = String(style.backgroundImage || '').match(/^url\\(["']?(.*?)["']?\\)$/);
                if (!match || !match[1]) continue;
                if (!sourceLooksUserMedia(match[1])) continue;
                candidates.push({type: 'url', src: match[1], ...size});
              }
              candidates.sort((a, b) => b.area - a.area);
              for (const candidate of candidates) {
                try {
                  let parts = null;
                  if (candidate.type === 'canvas') {
                    parts = dataUrlToParts(candidate.canvas.toDataURL('image/png'));
                  } else if (String(candidate.src || '').startsWith('data:')) {
                    parts = dataUrlToParts(candidate.src);
                  } else {
                    const response = await fetch(candidate.src);
                    const blob = await response.blob();
                    if (!blob || blob.size <= 0) continue;
                    parts = dataUrlToParts(await blobToDataUrl(blob));
                    if (parts && blob.type) parts.contentType = blob.type;
                  }
                  if (!parts || !parts.data) continue;
                  return {
                    data_base64: parts.data,
                    content_type: parts.contentType || 'image/png',
                    width: candidate.width,
                    height: candidate.height,
                    source_url: String(candidate.src || ''),
                  };
                } catch (_) {}
              }
              return null;
            }
            """
        )
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    data = _clean(str(payload.get("data_base64") or ""))
    if not data:
        return None
    try:
        raw = base64.b64decode(data, validate=True)
    except Exception:
        return None
    if not raw:
        return None
    content_type = _clean(str(payload.get("content_type") or "")) or "image/png"
    target = target_dir / f"{prefix}_inline_image{_image_extension(content_type)}"
    try:
        target.write_bytes(raw)
    except Exception:
        return None
    extra: dict[str, Any] = {"capture_method": "inline_image"}
    for field in ("width", "height"):
        value = payload.get(field)
        if isinstance(value, int) and value > 0:
            extra[field] = value
    return make_attachment_item(
        kind="image",
        label="inline_image",
        status="downloaded",
        path=str(target.resolve()),
        content_type=content_type,
        filename=target.name,
        source_url=_clean(str(payload.get("source_url") or ""))[:300],
        extra=extra,
    )


def _try_download_message_media(page: Page, message_locator: Locator, target_dir: Path, prefix: str, kind: str) -> dict[str, Any] | None:
    fallback_item: dict[str, Any] | None = None
    direct = message_locator.locator(", ".join(DOWNLOAD_SELECTORS))
    item = _try_click_download(page, direct, target_dir, prefix, kind)
    if _download_succeeded(item):
        return item
    if item is not None:
        fallback_item = item

    if kind == "image":
        item = _try_save_inline_image(message_locator, target_dir, prefix)
        if _download_succeeded(item):
            return item

    opener = message_locator.locator(", ".join(OPEN_MEDIA_SELECTORS))
    if not _locator_exists(opener):
        return fallback_item
    try:
        opener.first.click(force=True)
        page.wait_for_timeout(600)
        viewer = page.locator(", ".join(DOWNLOAD_SELECTORS))
        item = _try_click_download(page, viewer, target_dir, prefix, kind)
        if _download_succeeded(item):
            page.keyboard.press("Escape")
            page.wait_for_timeout(250)
            return item
        if item is not None and fallback_item is None:
            fallback_item = item
        if kind == "image":
            inline_viewer = page.locator("[role='dialog'], [data-testid*='media-viewer'], body")
            item = _try_save_inline_image(inline_viewer, target_dir, prefix)
            if _download_succeeded(item):
                page.keyboard.press("Escape")
                page.wait_for_timeout(250)
                return item
        page.keyboard.press("Escape")
        page.wait_for_timeout(250)
        return fallback_item
    except Exception as exc:
        try:
            page.keyboard.press("Escape")
        except Exception:
            pass
        return fallback_item or make_attachment_item(kind=kind, label="viewer_error", status="download_error", error=str(exc)[:300])


def _attachment_manifest_for_candidate(page: Page, candidate: WhatsAppCaptureCandidate, config: WhatsAppCaptureConfig) -> str | None:
    row = candidate.row
    message_context: dict[str, Any] = {}
    if _clean(str(row.get("reply_text") or "")):
        message_context["reply_text"] = _clean(str(row.get("reply_text") or ""))
    links = [_clean(link) for link in (row.get("links") or []) if _clean(link)]

    media_items: list[dict[str, Any]] = []
    if row.get("media_kinds"):
        target_dir = _media_storage_dir(config, candidate.chat_name, candidate.ts_iso)
        prefix = f"{_timestamp_slug(candidate.ts_iso)}_{candidate.fingerprint[:8]}"
        message_locator = _message_locator(page, row)
        for kind in [kind for kind in row.get("media_kinds") or [] if isinstance(kind, str) and kind]:
            item = _try_download_message_media(page, message_locator, target_dir, prefix, kind)
            if item is None:
                item = make_attachment_item(kind=kind, label="metadata_only", status="metadata_only")
            media_items.append(item)

    return build_attachment_manifest(
        items=media_items,
        message_context=message_context,
        links=links,
        source="whatsapp_web",
    )


def _scroll_message_pane(page: Page) -> dict[str, Any]:
    return page.evaluate(
        """
        () => {
          const main = document.querySelector('#main');
          if (!main) {
            return {before: null, after: null, at_top: true};
          }
          const candidates = Array.from(main.querySelectorAll('div')).filter(el => el.scrollHeight > el.clientHeight + 40);
          const scrollable = candidates.sort((a, b) => b.scrollHeight - a.scrollHeight)[0] || main;
          const before = Number(scrollable.scrollTop || 0);
          const delta = Math.max(300, Math.floor((scrollable.clientHeight || 700) * 0.8));
          scrollable.scrollTop = Math.max(0, before - delta);
          return {
            before,
            after: Number(scrollable.scrollTop || 0),
            at_top: Number(scrollable.scrollTop || 0) === 0,
          };
        }
        """
    )


def _prime_visible_messages(chat_name: str, state: CaptureStateStore, candidates: Sequence[WhatsAppCaptureCandidate]) -> dict[str, Any]:
    if not candidates:
        return {"chat_name": chat_name, "primed": False, "captured": 0, "visible_messages": 0}
    state.remember(chat_name, *[candidate.fingerprint for candidate in candidates])
    state.mark_primed(chat_name)
    state.save()
    append_audit_event("WHATSAPP_WEB_CAPTURE_PRIMED", None, {"chat_name": chat_name, "visible_messages": len(candidates)})
    return {"chat_name": chat_name, "primed": True, "captured": 0, "visible_messages": len(candidates)}


def _collect_new_messages(page: Page, chat_name: str, state: CaptureStateStore, config: WhatsAppCaptureConfig) -> tuple[list[WhatsAppCaptureMessage], bool]:
    collected: dict[str, WhatsAppCaptureMessage] = {}
    seen_known_fingerprint = False
    stalled_pages = 0

    for _ in range(config.max_scroll_pages):
        visible_rows = _extract_visible_rows(page)
        visible_candidates = [candidate for row in visible_rows if (candidate := _candidate_from_row(chat_name, row)) is not None]
        if not visible_candidates:
            break

        added_this_page = 0
        for candidate in reversed(visible_candidates):
            if state.has(chat_name, candidate.fingerprint):
                seen_known_fingerprint = True
                continue
            if candidate.fingerprint in collected:
                continue
            attachments = _attachment_manifest_for_candidate(page, candidate, config)
            collected[candidate.fingerprint] = WhatsAppCaptureMessage(
                chat_name=candidate.chat_name,
                sender=candidate.sender,
                text=candidate.text,
                ts_iso=candidate.ts_iso,
                ts_epoch=candidate.ts_epoch,
                fingerprint=candidate.fingerprint,
                attachments=attachments,
            )
            added_this_page += 1
            if len(collected) >= config.message_limit:
                break

        if len(collected) >= config.message_limit or seen_known_fingerprint:
            break

        scroll = _scroll_message_pane(page)
        page.wait_for_timeout(500)
        if scroll.get("before") == scroll.get("after"):
            stalled_pages += 1
        else:
            stalled_pages = 0
        if scroll.get("at_top") or stalled_pages >= 2 or added_this_page == 0:
            break

    ordered = sorted(
        collected.values(),
        key=lambda message: (message.ts_epoch if message.ts_epoch is not None else 10**18, message.ts_iso or "", message.fingerprint),
    )
    return ordered, seen_known_fingerprint


def capture_chat_once(
    page: Page,
    *,
    chat_name: str,
    state: CaptureStateStore,
    client: httpx.Client,
    config: WhatsAppCaptureConfig,
) -> dict[str, Any]:
    _open_chat(page, chat_name)
    visible_rows = _extract_visible_rows(page)
    visible_candidates = [candidate for row in visible_rows if (candidate := _candidate_from_row(chat_name, row)) is not None]

    if config.prime_visible_messages and not state.is_primed(chat_name):
        return _prime_visible_messages(chat_name, state, visible_candidates)

    new_messages, saw_known = _collect_new_messages(page, chat_name, state, config)
    if not new_messages:
        return {
            "chat_name": chat_name,
            "primed": False,
            "captured": 0,
            "visible_messages": len(visible_candidates),
            "seen_known_fingerprint": saw_known,
        }

    result = _post_messages(client, config, new_messages)
    state.remember(chat_name, *[message.fingerprint for message in new_messages])
    state.mark_primed(chat_name)
    state.save()
    append_audit_event(
        "WHATSAPP_WEB_CAPTURE_BATCH",
        None,
        {
            "chat_name": chat_name,
            "captured": len(new_messages),
            "inserted": result.get("inserted"),
            "deduped": result.get("deduped"),
        },
    )
    return {
        "chat_name": chat_name,
        "primed": False,
        "captured": len(new_messages),
        "visible_messages": len(visible_candidates),
        "seen_known_fingerprint": saw_known,
        "api_result": result,
    }


def _launch_context(config: WhatsAppCaptureConfig) -> PlaywrightCaptureRuntime:
    config.user_data_dir.mkdir(parents=True, exist_ok=True)
    config.media_dir.mkdir(parents=True, exist_ok=True)
    playwright = sync_playwright().start()
    try:
        context = playwright.chromium.launch_persistent_context(
            user_data_dir=str(config.user_data_dir),
            channel=config.browser_channel,
            headless=config.headless,
            viewport={"width": 1440, "height": 1600},
            timezone_id="America/New_York",
            accept_downloads=True,
        )
    except Exception:
        playwright.stop()
        raise
    return PlaywrightCaptureRuntime(playwright=playwright, context=context)


def _close_context(runtime: PlaywrightCaptureRuntime) -> None:
    try:
        runtime.context.close()
    finally:
        runtime.playwright.stop()


def _ensure_page(runtime: PlaywrightCaptureRuntime) -> Page:
    if runtime.context.pages:
        return runtime.context.pages[0]
    return runtime.context.new_page()


def run_capture_loop(config: WhatsAppCaptureConfig, *, once: bool = False) -> dict[str, Any]:
    state = CaptureStateStore(config.state_path)
    client = httpx.Client(timeout=20.0)
    runtime = _launch_context(config)
    try:
        page = _ensure_page(runtime)
        page.goto(WHATSAPP_WEB_URL, wait_until="domcontentloaded", timeout=120_000)
        _capture_status(
            config,
            state="starting",
            login_required=False,
            poll_seconds=config.poll_seconds,
            chat_names=list(config.chat_names),
        )
        _ensure_session_ready(page, config, startup=True)
        _log(
            "WhatsApp Web capture running "
            f"headless={config.headless} chats={list(config.chat_names)} poll_seconds={config.poll_seconds}"
        )
        append_audit_event(
            "WHATSAPP_WEB_CAPTURE_STARTED",
            None,
            {
                "headless": config.headless,
                "poll_seconds": config.poll_seconds,
                "chat_names": list(config.chat_names),
                "api_bases": list(config.api_bases),
            },
        )

        last_cycle: dict[str, Any] = {"ok": True, "results": []}
        while True:
            if not _ensure_session_ready(page, config):
                last_cycle = {"ok": False, "login_required": True, "results": []}
                if once:
                    return last_cycle
                time.sleep(config.poll_seconds)
                continue

            cycle_results: list[dict[str, Any]] = []
            cycle_captured = 0
            for chat_name in config.chat_names:
                try:
                    result = capture_chat_once(page, chat_name=chat_name, state=state, client=client, config=config)
                    if result.get("primed"):
                        _log(f'Primed "{chat_name}" with {result.get("visible_messages", 0)} visible messages.')
                    elif result.get("captured"):
                        _log(f'Captured {result["captured"]} new WhatsApp messages from "{chat_name}".')
                        cycle_captured += int(result.get("captured") or 0)
                    cycle_results.append(result)
                except Exception as exc:
                    append_audit_event("WHATSAPP_WEB_CAPTURE_ERROR", None, {"chat_name": chat_name, "error": str(exc)[:500]})
                    cycle_results.append({"chat_name": chat_name, "error": str(exc)})
                    _log(f'WhatsApp Web capture error for "{chat_name}": {exc}')

            last_cycle = {"ok": True, "results": cycle_results}
            status_updates: dict[str, Any] = {
                "state": "ready",
                "login_required": False,
                "last_cycle_at": epoch_to_iso(int(time.time())),
                "last_error": "",
            }
            if cycle_captured:
                status_updates["last_capture_at"] = epoch_to_iso(int(time.time()))
            _capture_status(config, **status_updates)
            if once:
                return last_cycle
            time.sleep(config.poll_seconds)
    finally:
        client.close()
        _close_context(runtime)
