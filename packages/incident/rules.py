import re

ELEVATOR = re.compile(r"\b(elevator|elevators|lift|lifts)\b", re.I)
ELEVATOR_SIDE_REFERENCE = re.compile(r"\b(?:the\s+)?(?:north|south|left|right)\s+(?:one|side)\b", re.I)
ELEVATOR_ASSET_NORTH = re.compile(r"\bnorth\b", re.I)
ELEVATOR_ASSET_SOUTH = re.compile(r"\bsouth\b", re.I)
ELEVATOR_ASSET_BOTH = re.compile(r"\b(both|two elevators|2 lifts|2 elevators)\b", re.I)
ONLY_SIDE_WORKING = re.compile(
    r"\bonly\s+(?:the\s+)?(?P<side>north|south|left|right)\s+"
    r"(?:elevator|lift|one|side)?\s*(?:is\s+)?"
    r"(?:working|functioning|operational|running|in\s+service)\b",
    re.I,
)
FLOOR_SERVICE_NORMAL = re.compile(
    r"\b(?:not|no\s+longer|without)\b[^.!?\n]{0,80}\b(?:"
    r"floor[- ]by[- ]floor|going\s+down\s+floor\s+by\s+floor|"
    r"stopping\s+(?:(?:at|on)\s+)?(?:each|every|all)\s+floor"
    r")\b",
    re.I,
)

OUT = re.compile(
    r"(out\s+of\s+service|out\s+of\s+order|not\s+working|broken|stuck|"
    r"no\s+(?:the\s+)?(?:north|south|left|right)\s+(?:elevator|lift|one|side)|"
    r"not\s+(?:the\s+)?(?:north|south|left|right)\s+(?:elevator|lift)|"
    r"(?:the\s+)?(?:north|south|left|right)\s+(?:one|side)\s+(?:is\s+|are\s+|was\s+|were\s+|still\s+)?(?:out|down|dead|broken|stuck|not\s+working)|"
    r"(?:elevators?|lifts?|north|south|left|right|they|it)\s+(?:is\s+|are\s+|was\s+|were\s+|still\s+)?(?:out|down)|"
    r"shutdown|shut\s*off|still\s+down|still\s+not\s+working|again\s+down|out\s+again|again\s+out|dead|"
    r"down\s+to\s+1\s+elevator|one\s+elevator\s+again|only\s+1\s+elevator|misbehaving|not\s+arrived|"
    r"not\s+to\s+cool overnight|both\s+elevators\s+are\s+out|both\s+lifts\s+are\s+out)",
    re.I,
)
CONTINUING = re.compile(r"\b(still|again)\b", re.I)
BACK = re.compile(
    r"(back\s+(up|on|in\s+service)|working\s+now|working\s+normal(?:ly)?|operational\s+again|fixed|restored|currently\s+working|currently\s+functioning|2\s+lifts\s+working|both\s+elevators\s+currently\s+functioning|seemed\s+to\s+come\s+at\s+a\s+normal\s+speed|they'?re\s+working\s+now)",
    re.I,
)
IRREGULAR_OPERATION = re.compile(
    r"\b(?:clunk(?:ed|ing)?|bang(?:ed|ing)?|bounce[sd]?|jolt(?:ed|ing)?|shake[sn]?|shook|"
    r"rough\s+ride|door\s+(?:opened|opening|opens)\s+(?:slow(?:ly)?|in\s+slo-?mo)|slow\s+door)\b",
    re.I,
)
CALL_RESPONSE = re.compile(
    r"\b(?:impossible|unable|can't|cannot|couldn['’]?t)\b[^.!?\n]{0,90}\b(?:call|summon|get|bring|request)\b[^.!?\n]{0,90}\b(?:elevator|lift)\b"
    r"|\b(?:elevator|lift)\b[^.!?\n]{0,120}\b(?:not\s+respond(?:ing)?|won['’]?t\s+come|wouldn['’]?t\s+come|doesn['’]?t\s+come|didn['’]?t\s+come|never\s+came|won['’]?t\s+stop|wouldn['’]?t\s+stop)\b"
    r"|\b(?:call|summon|get|bring|request)\b[^.!?\n]{0,90}\b(?:elevator|lift)\b[^.!?\n]{0,90}\b(?:not\s+respond(?:ing)?|won['’]?t|wouldn['’]?t|doesn['’]?t|didn['’]?t|never)\b",
    re.I,
)

HEAT = re.compile(r"\bheat\b|hot\s+water|no\s+hot\s+water|cold\s+water|boiler", re.I)
LEAK = re.compile(r"leak|flood|water\s+damage|ceiling\s+collapsed|mold", re.I)
PESTS = re.compile(r"\b(?:roach(?:es)?|mice|mouse|rats?|bed\s*bugs?|bugs)\b", re.I)
SEC = re.compile(r"lock|door|intercom|camera|security|stair|fire\s+door|handrail", re.I)
APARTMENT_ENTRY = re.compile(
    r"\b(?:apartment|apt|unit)\b[^.!?\n]{0,80}\b(?:entry|enter|entered|access|advise\s+super|without\s+(?:me|anyone)\s+(?:home|there))\b"
    r"|\b(?:entry|enter|entered|access)\b[^.!?\n]{0,80}\b(?:apartment|apt|unit)\b",
    re.I,
)
QUESTION_ONLY = re.compile(r"\?$")
DISCUSSION_QUESTION = re.compile(
    r"\b(?:is|are|does|do|did|has|have|can|could|should|would|when|where|who|what|why|how)\b[^.!?]{0,140}\?",
    re.I,
)
RECORDKEEPING_DISCUSSION = re.compile(
    r"\b(?:form|record|records|court|listing|list|listed|log|logging)\b.*\b(?:hours?|breakages?|called|arrive|come|fixed|repair)\b"
    r"|\b(?:hours?|breakages?|called|arrive|come|fixed|repair)\b.*\b(?:form|record|records|court|listing|list|listed|log|logging)\b",
    re.I,
)

ASSET_AFFECTED_RE = r"(?:out(?:\s+of\s+(?:service|order))?|down|dead|broken|not\s+working|stuck|shutdown|shut\s*off)"
ASSET_WORKING_RE = r"(?:working|functioning|operational|running|in\s+service|restored|back\s+(?:up|on|in\s+service))"


def _side_has_status(text: str, side: str, status_pattern: str) -> bool:
    side_asset = rf"\b(?:the\s+)?{side}\b(?:\s+(?:elevator|lift|one|side))?"
    status = rf"\b{status_pattern}\b"
    return bool(
        re.search(rf"{side_asset}[^.!?\n]{{0,80}}{status}", text, re.I)
        or re.search(rf"{status}[^.!?\n]{{0,80}}{side_asset}", text, re.I)
        or re.search(rf"\bno\s+(?:the\s+)?{side}\s+(?:elevator|lift|one|side)\b", text, re.I)
        or re.search(rf"\bnot\s+(?:the\s+)?{side}\s+(?:elevator|lift)\b", text, re.I)
    )


def _asset_status(text: str, side: str) -> tuple[bool, bool]:
    segments = [
        segment
        for segment in re.split(r"[.;!?\n,]+|\bbut\b|\bwhile\b", text, flags=re.I)
        if segment.strip()
    ] or [text]
    return (
        any(_side_has_status(segment, side, ASSET_AFFECTED_RE) for segment in segments),
        any(_side_has_status(segment, side, ASSET_WORKING_RE) for segment in segments),
    )


def _asset(text: str):
    if ELEVATOR_ASSET_BOTH.search(text):
        return "elevator_both"
    only_working = ONLY_SIDE_WORKING.search(text)
    if only_working:
        side = only_working.group("side").casefold()
        if side == "north":
            return "elevator_south"
        if side == "south":
            return "elevator_north"
    north_affected, north_working = _asset_status(text, "north")
    south_affected, south_working = _asset_status(text, "south")
    if north_affected and south_affected:
        return "elevator_both"
    if north_affected and not south_affected:
        return "elevator_north"
    if south_affected and not north_affected:
        return "elevator_south"
    if north_working and south_affected:
        return "elevator_south"
    if south_working and north_affected:
        return "elevator_north"
    if ELEVATOR_ASSET_NORTH.search(text):
        return "elevator_north"
    if ELEVATOR_ASSET_SOUTH.search(text):
        return "elevator_south"
    return None


def explicit_elevator_asset(text: str):
    return _asset(text or "")


def _has_elevator_reference(text: str) -> bool:
    return bool(ELEVATOR.search(text) or ELEVATOR_SIDE_REFERENCE.search(text))


def text_explicitly_supports_category(text: str, category: str | None) -> bool:
    t = (text or "").strip()
    cat = (category or "").strip()
    if not t or not cat:
        return False
    if cat == "elevator":
        return bool(ELEVATOR.search(t))
    if cat == "heat_hot_water":
        return bool(HEAT.search(t))
    if cat == "leaks_water_damage":
        return bool(LEAK.search(t))
    if cat == "pests":
        return bool(PESTS.search(t))
    if cat == "security_access":
        return bool(SEC.search(t))
    return False


def classify_rules(text: str) -> dict:
    t = (text or "").strip()
    if not t:
        return {"is_issue": False, "category": "other", "asset": None, "severity": 2, "title": "", "summary": "", "kind": "nonissue"}

    if DISCUSSION_QUESTION.search(t) and RECORDKEEPING_DISCUSSION.search(t):
        return {"is_issue": False, "category": "other", "asset": None, "severity": 2, "title": "", "summary": "", "kind": "nonissue"}

    if QUESTION_ONLY.search(t) and not OUT.search(t) and not BACK.search(t):
        return {"is_issue": False, "category": "other", "asset": None, "severity": 2, "title": "", "summary": "", "kind": "nonissue"}

    if _has_elevator_reference(t) and (BACK.search(t) or FLOOR_SERVICE_NORMAL.search(t)) and not ONLY_SIDE_WORKING.search(t):
        asset = _asset(t)
        return {
            "is_issue": True,
            "category": "elevator",
            "asset": asset,
            "severity": 2,
            "title": "Elevator restored",
            "summary": "Tenant reports elevator restored or currently working.",
            "status_hint": "closed",
            "kind": "restore",
        }

    if _has_elevator_reference(t) and CALL_RESPONSE.search(t):
        return {
            "is_issue": True,
            "category": "elevator",
            "asset": _asset(t),
            "event_type": "new_issue",
            "severity": 4,
            "title": "Elevator not responding to floor call",
            "summary": "Tenant reports the elevator did not respond to a floor call.",
            "kind": "issue",
        }

    if _has_elevator_reference(t) and (OUT.search(t) or ONLY_SIDE_WORKING.search(t)):
        asset = _asset(t)
        sev = 5 if asset == "elevator_both" else 4
        return {
            "is_issue": True,
            "category": "elevator",
            "asset": asset,
            "event_type": "still_out" if CONTINUING.search(t) else "outage",
            "severity": sev,
            "title": "Elevator outage",
            "summary": "Tenant reports elevator service reduced or not working.",
            "kind": "outage",
        }

    if _has_elevator_reference(t) and IRREGULAR_OPERATION.search(t):
        return {
            "is_issue": True,
            "category": "elevator",
            "asset": _asset(t),
            "event_type": "new_issue",
            "severity": 4,
            "title": "Elevator operation issue",
            "summary": "Tenant reports unsafe or irregular elevator operation.",
            "kind": "issue",
        }

    if re.search(r"\bdown\s+to\s+1\s+elevator\b|\b1\s+elevator\s+again\b", t, re.I):
        return {
            "is_issue": True,
            "category": "elevator",
            "asset": None,
            "event_type": "still_out" if CONTINUING.search(t) else "outage",
            "severity": 4,
            "title": "Elevator service reduced",
            "summary": "Tenant reports building is down to one working elevator.",
            "kind": "outage",
        }

    if HEAT.search(t) and re.search(r"\b(no|not\s+working|out|cold|brown|discolored|smell|freezing|without)\b", t, re.I):
        return {
            "is_issue": True,
            "category": "heat_hot_water",
            "asset": None,
            "severity": 4,
            "title": "Heat / hot water issue",
            "summary": "Tenant reports heat or hot water problem.",
            "kind": "issue",
        }

    if LEAK.search(t):
        return {
            "is_issue": True,
            "category": "leaks_water_damage",
            "asset": None,
            "severity": 4,
            "title": "Leak / water damage",
            "summary": "Tenant reports leak or water damage.",
            "kind": "issue",
        }

    if PESTS.search(t):
        return {
            "is_issue": True,
            "category": "pests",
            "asset": None,
            "severity": 3,
            "title": "Pest issue",
            "summary": "Tenant reports pests.",
            "kind": "issue",
        }

    if APARTMENT_ENTRY.search(t):
        return {
            "is_issue": True,
            "category": "security_access",
            "asset": None,
            "severity": 3,
            "title": "Apartment entry / access concern",
            "summary": "Tenant reports a concern about apartment entry or access.",
            "kind": "issue",
        }

    if SEC.search(t) and re.search(r"broken|not\s+working|stuck|can't|cannot|unsafe|detaching|jammed|hazard", t, re.I):
        return {
            "is_issue": True,
            "category": "security_access",
            "asset": None,
            "severity": 3,
            "title": "Security / access / safety issue",
            "summary": "Tenant reports door, stair, lock, or access safety problem.",
            "kind": "issue",
        }

    return {"is_issue": False, "category": "other", "asset": None, "severity": 2, "title": "", "summary": "", "kind": "nonissue"}
