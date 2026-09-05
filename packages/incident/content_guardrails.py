from __future__ import annotations

import re


NEWS_DATELINE_RE = re.compile(
    r"\b[A-Z][A-Z .]{2,40},\s+[A-Z.]{2,12}\s*\([A-Z0-9]{2,12}\)\s*(?:\u2014|-)\s+",
)
NEWS_URL_RE = re.compile(
    r"https?://[^\s]*(?:gothamist\.com|news12\.com|/news/|/article/|/morning/)",
    re.IGNORECASE,
)
EXTERNAL_STORY_RE = re.compile(
    r"\b(?:article|news\s*story|story\s+in|relatable\s+story|according\s+to)\b",
    re.IGNORECASE,
)
PROPERTY_LISTING_REFERENCE_RE = re.compile(
    r"\b(?:streeteasy|zillow|realtor\.com|apartments\.com|property\s+listing)\b"
    r"[\s\S]{0,180}\b(?:presented|listing|listed|advertis(?:e|ed|ing)|amenit(?:y|ies)|noting)\b"
    r"|\b(?:presented|listing|listed|advertis(?:e|ed|ing)|amenit(?:y|ies)|noting)\b"
    r"[\s\S]{0,180}\b(?:streeteasy|zillow|realtor\.com|apartments\.com|property\s+listing)\b",
    re.IGNORECASE,
)
TRANSIT_ONLY_RE = re.compile(
    r"\b(?:waiting|waited|stuck)\b[^.!?\n]{0,100}\b(?:subway|train|platform|station)\b"
    r"|\b(?:subway|train|platform|station)\b[^.!?\n]{0,100}\b(?:waiting|waited|stuck|delayed)\b",
    re.IGNORECASE,
)
TRANSIT_ADVOCACY_RE = re.compile(
    r"\b(?:f\s*(?:train|service)|subway|train|bike|bicycle)\b[\s\S]{0,320}\b(?:elevators?|lifts?|stairs?)\b"
    r"|\b(?:elevators?|lifts?|stairs?)\b[\s\S]{0,320}\b(?:f\s*(?:train|service)|subway|train|bike|bicycle)\b",
    re.IGNORECASE,
)
REFERENCE_DEFINITION_RE = re.compile(
    r"^\s*[\"\u201c]?\s*(?:the\s+)?(?:code|term|abbreviation)\b"
    r"[\s\S]{0,180}\b(?:stands\s+for|means|is\s+defined\s+as|requires?)\b",
    re.IGNORECASE,
)
HISTORICAL_CONTEXT_RE = re.compile(
    r"\b(?:last\s+summer|last\s+year|years?\s+ago|months?\s+ago|"
    r"a\s+few\s+weeks?\s+ago|shots?\s+from\s+a\s+few\s+weeks?\s+ago|"
    r"pre[-\s][a-z0-9-]+\s+installation|back\s+in\s+20\d{2})\b",
    re.IGNORECASE,
)
CONDITIONAL_BUILDING_SCENARIO_RE = re.compile(
    r"\b(?:if|when)\b[^.!?\n]{0,180}\b(?:elevators?|lifts?)\b"
    r"[^.!?\n]{0,180}\b(?:out|broken|trap(?:s|ped)?|stuck|fire\s+stairs?)\b"
    r"|\b(?:if|when)\b[^.!?\n]{0,180}\b(?:out|broken|trap(?:s|ped)?|stuck)\b"
    r"[^.!?\n]{0,180}\b(?:elevators?|lifts?)\b",
    re.IGNORECASE,
)
PEST_TREATMENT_ADVICE_RE = re.compile(
    r"\b(?:gel|spray|glue\s+traps?|rodent\s+barrier)\b"
    r"[\s\S]{0,180}\b(?:if\s+you\s+need|works?\s+(?:pretty\s+)?quick|usually\s+use|they\s+also\s+have)\b"
    r"|\b(?:if\s+you\s+need|usually\s+use|they\s+also\s+have)\b"
    r"[\s\S]{0,180}\b(?:gel|spray|glue\s+traps?|rodent\s+barrier)\b",
    re.IGNORECASE,
)
POSITIVE_HEAT_COUNTEREXAMPLE_RE = re.compile(
    r"\b(?:never\s+had\s+an?\s+issue\s+with\s+(?:the\s+)?heat|"
    r"heat\s+(?:stays|is)\s+(?:in|on)|as\s+warm\s+as\s+usual)\b",
    re.IGNORECASE,
)
FORWARDED_DOCUMENT_RE = re.compile(
    r"(?:^|\n)\s*(?:from|to|cc|subject|sent):\s*[^\n]+"
    r"|\b(?:dear\s+(?:counsel|management)|attorney[-\s]client|legal\s+demand)\b",
    re.IGNORECASE,
)
THIRD_PARTY_DAMAGE_COMMENTARY_RE = re.compile(
    r"\bthey\s+have\s+(?:so\s+much\s+)?water\s+damage\b"
    r"|\b(?:that|their)\s+building\b[^.!?\n]{0,100}\b(?:water\s+damage|leak(?:s|ing)?)\b",
    re.IGNORECASE,
)
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

    if TRANSIT_ONLY_RE.search(clean) and not CURRENT_BUILDING_CONDITION_RE.search(clean):
        return "external_reference"

    if TRANSIT_ADVOCACY_RE.search(clean) and not CURRENT_BUILDING_CONDITION_RE.search(clean):
        return "general_reference"

    if PROPERTY_LISTING_REFERENCE_RE.search(clean):
        return "external_reference"

    if REFERENCE_DEFINITION_RE.search(clean) and not CURRENT_BUILDING_CONDITION_RE.search(clean):
        return "general_reference"

    if FORWARDED_DOCUMENT_RE.search(clean):
        return "document_context"

    if THIRD_PARTY_DAMAGE_COMMENTARY_RE.search(clean):
        return "external_reference"

    if PEST_TREATMENT_ADVICE_RE.search(clean):
        return "general_advisory"

    if POSITIVE_HEAT_COUNTEREXAMPLE_RE.search(clean):
        return "counterexample"

    if HISTORICAL_CONTEXT_RE.search(clean):
        return "historical_reference"

    if CONDITIONAL_BUILDING_SCENARIO_RE.search(clean):
        return "general_advisory"

    dateline = NEWS_DATELINE_RE.search(clean)
    if dateline and NEWS_URL_RE.search(clean):
        introduction = clean[: dateline.start()]
        if not CURRENT_BUILDING_CONDITION_RE.search(introduction):
            return "external_reference"

    if NEWS_URL_RE.search(clean) and (
        EXTERNAL_STORY_RE.search(clean)
        or re.fullmatch(r"\s*https?://\S+\s*", clean, re.IGNORECASE)
    ):
        return "external_reference"

    if (
        ADVISORY_INSTRUCTION_RE.search(clean)
        and GENERAL_HAZARD_EXPLANATION_RE.search(clean)
        and not OBSERVED_CURRENT_HAZARD_RE.search(clean)
    ):
        return "general_advisory"

    return None
