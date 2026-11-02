from __future__ import annotations

import re
from packages.incident.building_conditions import building_condition_presentation


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
    r"\b(?:if|when(?!\s+(?:i|we|they)\s+(?:left|arrived|came|went|saw|heard|noticed)\b))\b[^.!?\n]{0,180}\b(?:elevators?|lifts?)\b"
    r"[^.!?\n]{0,180}\b(?:out|down|not\s+working|broken|trap(?:s|ped)?|stuck|fire\s+stairs?)\b"
    r"|\b(?:if|when(?!\s+(?:i|we|they)\s+(?:left|arrived|came|went|saw|heard|noticed)\b))\b[^.!?\n]{0,180}\b(?:out|down|not\s+working|broken|trap(?:s|ped)?|stuck)\b"
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
AUTOMATION_INSTRUCTION_RE = re.compile(
    r"\b(?:ignore|disregard|override)\b[^.!?\n]{0,80}\b(?:instructions?|rules?|prompts?|system)\b"
    r"|\b(?:set|return|output)\b[^.!?\n]{0,60}\b(?:is_issue|needs_review|confidence|json)\b",
    re.IGNORECASE,
)
UNCONFIRMED_ELEVATOR_CONDITION_RE = re.compile(
    r"\b(?:will|may|might|could|would)\s+(?:be\s+|get\s+|remain\s+)?"
    r"(?:out\s+of\s+service|down|out|not\s+working|broken|stuck)\b"
    r"|\b(?:scheduled|expected|going)\s+to\s+(?:be\s+|get\s+)?"
    r"(?:out\s+of\s+service|down|out|not\s+working|broken|stuck)\b",
    re.IGNORECASE,
)
NEGATED_ELEVATOR_PROBLEM_RE = re.compile(
    r"\b(?:not|isn['’]t|aren['’]t)\s+(?:broken|down|stuck|out\s+of\s+service)\b"
    r"|\bno\s+(?:elevator|lift)\s+(?:problems?|issues?|malfunctions?)\b", re.I,
)
PURE_SOCIAL_RE = re.compile(r"\s*(?:thanks(?:\s+for\s+(?:the\s+)?update)?|thank\s+you|ok(?:ay)?|hello|hi|good\s+morning|👍)[.!\s]*", re.I)
REFERENCE_QUOTE_RE = re.compile(r"^\s*(?:earlier\s+message|quote|quoted\s+message|forwarded\s+message)\s*:", re.I)
EXTERNAL_ELEVATOR_REPORT_RE = re.compile(
    r"\b(?:elevators?|lifts?)\b[^.!?\n]{0,100}\b(?:another|other|their|that)\s+building\b"
    r"|\b(?:another|other|their|that)\s+building\b[^.!?\n]{0,100}\b(?:elevators?|lifts?)\b", re.I)


def nonreporting_content_reason(text: str | None) -> str | None:
    """Identify content that mentions hazards without reporting a building event."""
    clean = (text or "").strip()
    if not clean:
        return None

    clauses = re.split(r"[.!?\n]+|\bbut\b", clean, flags=re.I)
    independent_current_report = any(
        CURRENT_BUILDING_CONDITION_RE.search(part)
        and not CONDITIONAL_BUILDING_SCENARIO_RE.search(part)
        and not UNCONFIRMED_ELEVATOR_CONDITION_RE.search(part)
        and not AUTOMATION_INSTRUCTION_RE.search(part)
        and not NEGATED_ELEVATOR_PROBLEM_RE.search(part)
        for part in clauses
    )
    if PURE_SOCIAL_RE.fullmatch(clean):
        return "social_message"
    if EXTERNAL_ELEVATOR_REPORT_RE.search(clean):
        local_parts = [part for part in clauses if not EXTERNAL_ELEVATOR_REPORT_RE.search(part)]
        if not any(CURRENT_BUILDING_CONDITION_RE.search(part) for part in local_parts):
            return "external_reference"
    if REFERENCE_QUOTE_RE.search(clean):
        return "quoted_reference"
    if NEGATED_ELEVATOR_PROBLEM_RE.search(clean) and not independent_current_report:
        return "counterexample"
    if re.match(r"^\s*(?:is|are|was|were|does|do|did|could|can|should|has|have)\b", clean, re.I) and clean.endswith("?") and not re.search(r"[.!]\s+", clean):
        return "question"
    if (re.search(r"\b(?:elevators?|lifts?)\b", clean, re.I)
            and UNCONFIRMED_ELEVATOR_CONDITION_RE.search(clean) and not independent_current_report):
        return "unconfirmed_condition"

    if AUTOMATION_INSTRUCTION_RE.search(clean):
        factual_parts = [part for part in re.split(r"[.!?\n]+", clean) if not AUTOMATION_INSTRUCTION_RE.search(part)]
        if not any(CURRENT_BUILDING_CONDITION_RE.search(part) for part in factual_parts):
            return "automation_instruction"

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
        if re.search(r"\b(?:but|now|ever\s+since)\b", clean, re.I) and building_condition_presentation(clean):
            return None
        return "historical_reference"

    if CONDITIONAL_BUILDING_SCENARIO_RE.search(clean) and not independent_current_report:
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
