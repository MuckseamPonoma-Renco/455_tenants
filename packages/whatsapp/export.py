from __future__ import annotations

import re
import zipfile
from dataclasses import dataclass
from pathlib import Path

from packages.whatsapp.parser import ParsedMessage, parse_export_text


@dataclass(frozen=True)
class ParsedExport:
    messages: list[ParsedMessage]
    chat_files: list[str]
    is_zip: bool


def chat_name_from_export_filename(name: str, fallback: str = "Tenants WhatsApp") -> str:
    stem = Path(name).stem
    if stem.casefold() == "_chat":
        return fallback
    stem = re.sub(r"^WhatsApp Chat -\s*", "", stem, flags=re.IGNORECASE)
    stem = re.sub(r"[_\s-]*chat$", "", stem, flags=re.IGNORECASE)
    return stem.strip(" _-") or fallback


def _txt_names(archive: zipfile.ZipFile) -> list[str]:
    return sorted(name for name in archive.namelist() if name.casefold().endswith(".txt"))


def _parse_zip_archive(archive: zipfile.ZipFile, *, default_chat_name: str) -> ParsedExport:
    messages: list[ParsedMessage] = []
    chat_files: list[str] = []
    names = _txt_names(archive)
    if not names:
        raise ValueError("ZIP does not contain a .txt chat export")
    for name in names:
        content = archive.read(name).decode("utf-8", errors="replace")
        parsed = parse_export_text(content, chat_name=chat_name_from_export_filename(name, default_chat_name))
        if not parsed:
            continue
        messages.extend(parsed)
        chat_files.append(name)
    return ParsedExport(messages=messages, chat_files=chat_files, is_zip=True)


def parse_export_payload(
    filename: str,
    raw: bytes,
    *,
    default_chat_name: str = "Tenants WhatsApp",
) -> ParsedExport:
    is_zip = raw[:4] == b"PK\x03\x04"
    messages: list[ParsedMessage] = []
    chat_files: list[str] = []

    if is_zip:
        from io import BytesIO

        with zipfile.ZipFile(BytesIO(raw)) as archive:
            return _parse_zip_archive(archive, default_chat_name=default_chat_name)

    content = raw.decode("utf-8", errors="replace")
    parsed = parse_export_text(content, chat_name=chat_name_from_export_filename(filename, default_chat_name))
    return ParsedExport(messages=parsed, chat_files=[filename] if parsed else [], is_zip=False)


def parse_export_path(
    path: str | Path,
    *,
    default_chat_name: str = "Tenants WhatsApp",
    filename: str | None = None,
) -> ParsedExport:
    export_path = Path(path)
    export_filename = filename or export_path.name
    with export_path.open("rb") as handle:
        is_zip = handle.read(4) == b"PK\x03\x04"
    if is_zip:
        with zipfile.ZipFile(export_path) as archive:
            return _parse_zip_archive(archive, default_chat_name=default_chat_name)

    content = export_path.read_text(encoding="utf-8", errors="replace")
    parsed = parse_export_text(content, chat_name=chat_name_from_export_filename(export_filename, default_chat_name))
    return ParsedExport(messages=parsed, chat_files=[export_filename] if parsed else [], is_zip=False)
