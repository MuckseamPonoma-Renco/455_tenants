from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable, Iterator

from packages.audit import compute_message_id
from packages.whatsapp.parser import ParsedMessage


MAX_MESSAGE_ID_LENGTH = 64


@dataclass(frozen=True)
class PhysicalMessageIdentity:
    """Stable identity for one physical message occurrence in an export."""

    message: ParsedMessage
    base_message_id: str
    message_id: str
    occurrence: int


def message_id_for_occurrence(base_message_id: str, occurrence: int) -> str:
    """Keep the legacy ID for occurrence one and suffix later occurrences."""

    if occurrence < 1:
        raise ValueError("occurrence must be at least 1")
    if occurrence == 1:
        return base_message_id

    suffix = f"~{occurrence}"
    if len(suffix) >= MAX_MESSAGE_ID_LENGTH:
        raise ValueError("occurrence suffix is too long for a message ID")
    return f"{base_message_id[: MAX_MESSAGE_ID_LENGTH - len(suffix)]}{suffix}"


def iter_physical_message_identities(
    messages: Iterable[ParsedMessage],
) -> Iterator[PhysicalMessageIdentity]:
    """Yield collision-safe IDs in the export's stable physical order."""

    occurrences: dict[str, int] = defaultdict(int)
    for message in messages:
        base_message_id = compute_message_id(
            message.chat_name,
            message.sender,
            message.ts_iso or "",
            message.text,
        )
        occurrences[base_message_id] += 1
        occurrence = occurrences[base_message_id]
        yield PhysicalMessageIdentity(
            message=message,
            base_message_id=base_message_id,
            message_id=message_id_for_occurrence(base_message_id, occurrence),
            occurrence=occurrence,
        )
