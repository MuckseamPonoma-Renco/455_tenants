from __future__ import annotations


_REPLACEMENT_PHRASES = (
    "full elevator replacement",
    "elevator replacement",
    "replace elevator",
    "replace existing elevator",
    "replacement of elevator",
    "new elevator",
)

_MODERNIZATION_SCOPE_PHRASES = (
    "passenger elevator",
    "electric elevator",
    "elevator modernization",
)


def describes_elevator_replacement_scope(text: str) -> bool:
    normalized = " ".join((text or "").casefold().split())
    if any(phrase in normalized for phrase in _REPLACEMENT_PHRASES):
        return True
    return "modernization" in normalized and any(
        phrase in normalized for phrase in _MODERNIZATION_SCOPE_PHRASES
    )
