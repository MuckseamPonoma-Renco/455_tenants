from __future__ import annotations

import re


NEWS_DATELINE_RE = re.compile(
    r"\b[A-Z][A-Z .]{2,40},\s+[A-Z.]{2,12}\s*\([A-Z0-9]{2,12}\)\s*(?:\u2014|-)\s+",
)
NEWS_URL_RE = re.compile(r"https?://[^\s]+/(?:news|article|morning)/", re.IGNORECASE)
CURRENT_BUILDING_CONDITION_RE = re.compile(
    r"\b(?:our|the|north|south|both)\s+(?:building\s+)?(?:elevators?|lifts?)\b"
    r"[^.!?\n]{0,100}\b(?:out|down|dead|broken|stuck|not\s+working)\b"
    r"|\b(?:out|down|dead|broken|stuck|not\s+working)\b"
    r"[^.!?\n]{0,100}\b(?:our|the|north|south|both)\s+(?:building\s+)?(?:elevators?|lifts?)\b",
    re.IGNORECASE,
)
ADVISORY_INSTRUCTION_RE = re.compile(
    r"\b(?:everyone|residents?|tenants?|people|pet\s+owners?)\s+"
    r"(?:should|shud|must|need\s+to)\b",
    re.IGNORECASE,
)
GENERAL_HAZARD_EXPLANATION_RE = re.compile(
    r"\b(?:acts?\s+like|can\s+be|may\s+be)\b[^.!?\n]{0,100}\b(?:fire|hazard)\b"
    r"|\b(?:a\s+)?(?:stuffed|full)\s+(?:lint\s+)?tray\s*(?:=|means?)\s*(?:a\s+)?fire\s+hazard\b",
    re.IGNORECASE,
)
OBSERVED_CURRENT_HAZARD_RE = re.compile(
    r"\b(?:i|we)\s+(?:found|saw|smelled|noticed)\b"
    r"|\b(?:lint\s+)?tray\s+(?:is|was)\s+(?:stuffed|full|overflowing)\b"
    r"|\b(?:smoke|flames?|fire)\s+(?:is|was|are|were)\b",
    re.IGNORECASE,
)


def nonreporting_content_reason(text: str | None) -> str | None:
    """Identify content that mentions hazards without reporting a building event."""
    clean = (text or "").strip()
    if not clean:
        return None

    dateline = NEWS_DATELINE_RE.search(clean)
    if dateline and NEWS_URL_RE.search(clean):
        introduction = clean[: dateline.start()]
        if not CURRENT_BUILDING_CONDITION_RE.search(introduction):
            return "external_reference"

    if (
        ADVISORY_INSTRUCTION_RE.search(clean)
        and GENERAL_HAZARD_EXPLANATION_RE.search(clean)
        and not OBSERVED_CURRENT_HAZARD_RE.search(clean)
    ):
        return "general_advisory"

    return None
