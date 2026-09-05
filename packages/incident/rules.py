import re

from packages.incident.content_guardrails import nonreporting_content_reason

ELEVATOR = re.compile(r"\b(elevator|elevators|lift|lifts)\b", re.I)
ELEVATOR_SIDE_REFERENCE = re.compile(r"\b(?:the\s+)?(?:north|south|left|right)\s+(?:one|side)\b", re.I)
ELEVATOR_ASSET_NORTH = re.compile(r"\bnorth\b", re.I)
ELEVATOR_ASSET_SOUTH = re.compile(r"\bsouth\b", re.I)
ELEVATOR_ASSET_BOTH = re.compile(
    r"\b(?:both|two elevators|2 lifts|2 elevators|all (?:elevators?|lifts?)|"
    r"north\s*(?:&|and)\s*south|south\s*(?:&|and)\s*north)\b",
    re.I,
)
ELEVATOR_ASSET_ZERO = re.compile(r"\b(?:zero|no)\s+(?:elevators?|lifts?)\b", re.I)
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
    r"(out\s+of\s+service|out\s+of\s+order|not\s+in\s+service|not\s+working|broken|stuck|"
    r"no\s+(?:the\s+)?(?:north|south|left|right)\s+(?:elevator|lift|one|side)|"
    r"not\s+(?:the\s+)?(?:north|south|left|right)\s+(?:elevator|lift)|"
    r"(?:the\s+)?(?:north|south|left|right)\s+(?:one|side)\s+(?:is\s+|are\s+|was\s+|were\s+|still\s+)?(?:out|down|dead|broken|stuck|not\s+working)|"
    r"(?:elevators?|lifts?|north|south|left|right|they|it)\s+(?:is\s+|are\s+|was\s+|were\s+|still\s+)?(?:out|down)|"
    r"shutdown|shut\s*off|still\s+down|still\s+not\s+working|again\s+down|out\s+again|again\s+out|dead|"
    r"down\s+to\s+1\s+elevator|one\s+elevator\s+again|only\s+1\s+elevator|misbehaving|not\s+arrived|"
    r"not\s+to\s+cool overnight|both\s+elevators\s+are\s+out|both\s+lifts\s+are\s+out)",
    re.I,
)
REDUCED_SERVICE = re.compile(
    r"\b(?:currently\s+)?(?:only\s+)?one\s+(?:elevator|lift)\s+(?:in\s+service|working|running|operational)\b"
    r"|\b(?:one|1)\s+(?:elevator|lift)\s+only\b"
    r"|\bdown\s+to\s+(?:one|1)(?:\s+working)?\s+(?:elevator|lift)\b"
    r"|\b(?:elevator|lift)\s+service\s+(?:is\s+)?reduced\b",
    re.I,
)
CONTINUING = re.compile(r"\b(still|again)\b", re.I)
BACK = re.compile(
    r"(back\s+(up|on|in\s+service)|working\s+now|working\s+normal(?:ly)?|operational\s+again|fixed|restored|currently\s+working|currently\s+functioning|(?:all|2)\s+(?:elevators?|lifts?)\s+(?:are\s+|were\s+|currently\s+)?(?:working|functioning|operational|running)|both\s+(?:elevators?|lifts?)\s+(?:are\s+|were\s+|currently\s+)?(?:working|functioning|operational|running)|both\s+(?:are\s+|were\s+)?(?:working|functioning|operational|running)|both\s+work\s+now|seemed\s+to\s+come\s+at\s+a\s+normal\s+speed|they'?re\s+working\s+now)",
    re.I,
)
CAUTIOUS_RESTORE = re.compile(
    r"\b(?:might|may|seems?|appears?|apparently|possibly|probably)\b"
    r"|[\"“”]\s*(?:working|functioning|operational|running)\s*[\"“”]",
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
VENTILATION = re.compile(r"\b(?:vent|vents|ventilation|airflow|air\s+flow)\b", re.I)
SEC = re.compile(r"lock|door|intercom|camera|security|stair|fire\s+door|handrail", re.I)
APARTMENT_ENTRY = re.compile(
    r"\b(?:apartment|apt|unit)\b[^.!?\n]{0,80}\b(?:entry|enter|entered|access|advise\s+super|without\s+(?:me|anyone)\s+(?:home|there))\b"
    r"|\b(?:entry|enter|entered|access)\b[^.!?\n]{0,80}\b(?:apartment|apt|unit)\b"
    r"|\b(?:try(?:ing|ied)?\s+to\s+)?(?:come|came)\s+(?:in|into)\b[^.!?\n]{0,80}\b(?:apartment|apt|unit)\b"
    r"|\b(?:came|come)\s+to\b[^.!?\n]{0,80}\b(?:apartment|apt|unit)\b[^.!?\n]{0,100}\btry(?:ing|ied)?\s+to\s+enter\b",
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
ELEVATOR_SAFETY_GUIDANCE = re.compile(
    r"\b(?:nyc\.?gov|elevator\s+safety|rules\s+if\s+you\s+get\s+stuck|if\s+you\s+get\s+stuck\s+in\s+(?:an\s+)?elevator|"
    r"ring\s+the\s+alarm|help\s+is\s+on\s+the\s+way)\b",
    re.I,
)
ELEVATOR_CONDITIONAL_SAFETY_DISCUSSION = re.compile(
    r"\bif\s+(?:both\s+)?(?:elevators?|lifts?)\s+(?:are\s+|were\s+)?stuck\b"
    r"|\b(?:indicator\s+floor\s+lights|adjacent\s+shaft|emergency\s+two-way|building\s+code|asme\s+a17\.3)\b",
    re.I,
)
HISTORICAL_ELEVATOR_REFERENCE = re.compile(
    r"\b(?:last\s+year|years?\s+ago|months?\s+ago|back\s+in\s+20\d{2})\b",
    re.I,
)
CURRENT_STATE_REFERENCE = re.compile(r"\b(?:now|today|currently|still|right\s+now|at\s+present)\b", re.I)
ELAPSED_ELEVATOR_EVENT = re.compile(
    r"\bsince\s+(?:the\s+)?(?:stuck|stopped|out|down|dead)\s+(?:elevator|lift)(?:\s+event)?\b",
    re.I,
)
DOOR_ACCESSIBILITY = re.compile(
    r"\b(?:door|entry)\b[\s\S]{0,300}\b(?:wheelchair|accessible|accessibility)\b"
    r"|\b(?:wheelchair|accessible|accessibility)\b[\s\S]{0,300}\b(?:door|entry)\b",
    re.I,
)
PERSONAL_SAFETY_REPORT = re.compile(
    r"\b(?:warn\s+women|"
    r"(?:hang(?:ing)?\s+out|linger(?:ing)?)\b[^.!?\n]{0,100}\b(?:in\s*front\s+of|outside)\s+(?:the\s+)?building|"
    r"walk(?:ed|ing)?\s+(?:up\s+)?(?:from\s+)?behind\b[^.!?\n]{0,100}\b(?:squeez(?:ed|ing)|touch(?:ed|ing)))\b",
    re.I,
)
LAUNDRY = re.compile(
    r"\b(?:laundry|washer|washers|dryer|dryers|washing\s+machines?)\b",
    re.I,
)
LAUNDRY_PROBLEM = re.compile(
    r"\b(?:not\s+working|doesn['’]?t\s+work|error|can['’]?t\s+connect|cannot\s+connect|"
    r"no\s+(?:wi-?fi|internet|service)|app\s+(?:is\s+)?not\s+working|card\s+(?:is\s+)?(?:giving|showing)|"
    r"(?:won['’]?t|doesn['’]?t|isn['’]?t|can['’]?t|cannot)\s+(?:read(?:ing)?|recognize|accept)|"
    r"door\s+(?:isn['’]?t|not)\s+connect(?:ing)?|door\s+(?:isn['’]?t|not)\s+register(?:ing)?|"
    r"(?:stol(?:e|en)|ate|swallowed|kept)\b[^.!?\n]{0,60}\bdetergent|"
    r"detergent\b[^.!?\n]{0,60}\b(?:stol(?:e|en)|not\s+dispens(?:e|ed|ing)))\b",
    re.I,
)
LAUNDRY_NUMBERED_MACHINE = re.compile(
    r"\b(?P<kind>washer|dryer)(?:\s+(?:number|no\.?))?\s*#?\s*(?P<number>\d{1,3})\b",
    re.I,
)
LAUNDRY_RESTORE = re.compile(
    r"\b(?:fixed|repaired|restored|working\s+(?:again|now)|back\s+in\s+service)\b",
    re.I,
)
FIRE_HOSE = re.compile(r"\b(?:fire\s*hoses?|firehouses?)\b", re.I)
FIRE_HOSE_MISSING = re.compile(
    r"\b(?:lack\s+of|missing|removed|not\s+(?:there|present|installed)|no)\b",
    re.I,
)
FIRE_HOSE_PROGRESS = re.compile(
    r"\b(?:replac(?:e|ed|ement|ing)|install(?:ed|ing|ation)?|put\s+back|restor(?:e|ed|ing))\b",
    re.I,
)
ELEVATOR_PERFORMANCE = re.compile(
    r"\b(?:elevator|lift)\b[^.!?\n]{0,80}\b(?:super\s+slow|very\s+slow|moving\s+slow(?:ly)?|running\s+slow(?:ly)?)\b"
    r"|\b(?:super\s+slow|very\s+slow|moving\s+slow(?:ly)?|running\s+slow(?:ly)?)\b[^.!?\n]{0,80}\b(?:elevator|lift)\b",
    re.I,
)
ELECTRICAL = re.compile(r"\b(?:electrical|wiring|wire|wires|outlet|outlets|receptacle|oven)\b", re.I)
ELECTRICAL_PROBLEM = re.compile(
    r"\b(?:painted\s+over|upside\s+down|odd|unsafe|hazard|problem|issue|not\s+working|"
    r"only\s+functioning|comes?\s+out\s+(?:of\s+)?the\s+wall|plug(?:s|ged)?\s+into|"
    r"management\s+hasn['’]?t\s+helped|inspection)\b",
    re.I,
)
FIRST_PERSON_BUILDING_CONDITION = re.compile(
    r"\b(?:my|mine|our|ours|apartment|apt|unit|living\s+room|kitchen)\b",
    re.I,
)
FRONT_DESK_PHONE_WORKS = re.compile(
    r"\bnumber\b[^.!?\n]{0,100}\bfront\s+desk\b[^.!?\n]{0,100}\bworks?\b"
    r"|\bfront\s+desk\b[^.!?\n]{0,100}\bnumber\b[^.!?\n]{0,100}\bworks?\b",
    re.I,
)

ASSET_AFFECTED_RE = r"(?:out(?:\s+of\s+(?:service|order))?|down|dead|broken|not\s+in\s+service|not\s+working|stuck|shutdown|shut\s*off)"
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
    if ELEVATOR_ASSET_BOTH.search(text) or ELEVATOR_ASSET_ZERO.search(text):
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


def explicit_laundry_asset(text: str) -> str | None:
    """Return one explicitly named laundry machine, never a guessed asset."""

    matches = {
        (match.group("kind").casefold(), int(match.group("number")))
        for match in LAUNDRY_NUMBERED_MACHINE.finditer(text or "")
    }
    if len(matches) != 1:
        return None
    kind, number = next(iter(matches))
    return f"{kind}_{number}"


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
    if cat == "laundry":
        return bool(LAUNDRY.search(t))
    if cat == "fire_safety":
        return bool(FIRE_HOSE.search(t))
    return False


def classify_rules(text: str) -> dict:
    t = (text or "").strip()
    if not t:
        return {"is_issue": False, "category": "other", "asset": None, "severity": 2, "title": "", "summary": "", "kind": "nonissue"}

    nonreport_reason = nonreporting_content_reason(t)
    if nonreport_reason:
        return {
            "is_issue": False,
            "category": "other",
            "asset": None,
            "severity": 1,
            "title": "",
            "summary": "",
            "kind": "nonissue",
            "nonreport_reason": nonreport_reason,
        }

    if DISCUSSION_QUESTION.search(t) and RECORDKEEPING_DISCUSSION.search(t):
        return {"is_issue": False, "category": "other", "asset": None, "severity": 2, "title": "", "summary": "", "kind": "nonissue"}

    if ELEVATOR_SAFETY_GUIDANCE.search(t) or ELEVATOR_CONDITIONAL_SAFETY_DISCUSSION.search(t):
        return {"is_issue": False, "category": "other", "asset": None, "severity": 2, "title": "", "summary": "", "kind": "nonissue"}

    if _has_elevator_reference(t) and HISTORICAL_ELEVATOR_REFERENCE.search(t) and not CURRENT_STATE_REFERENCE.search(t):
        return {"is_issue": False, "category": "other", "asset": None, "severity": 2, "title": "", "summary": "", "kind": "nonissue"}

    if _has_elevator_reference(t) and ELAPSED_ELEVATOR_EVENT.search(t):
        return {
            "is_issue": True,
            "category": "elevator",
            "asset": _asset(t),
            "event_type": "status_update",
            "severity": 3,
            "title": "Elevator incident duration update",
            "summary": "Tenant reports elapsed time since a recent elevator incident.",
            "kind": "issue",
        }

    if APARTMENT_ENTRY.search(t):
        return {
            "is_issue": True,
            "category": "security_access",
            "asset": None,
            "event_type": "new_issue",
            "severity": 3,
            "title": "Apartment entry / access concern",
            "summary": "Tenant reports a concern about apartment entry or access.",
            "kind": "issue",
            "preserve_issue": True,
            "preserve_event_type": True,
        }

    if FRONT_DESK_PHONE_WORKS.search(t):
        return {
            "is_issue": True,
            "category": "security_access",
            "asset": None,
            "event_type": "restore",
            "severity": 2,
            "title": "Front desk phone number confirmed working",
            "summary": "Tenant confirms the front desk phone number works.",
            "kind": "restore",
            "preserve_issue": True,
            "preserve_event_type": True,
        }

    if FIRE_HOSE.search(t) and FIRE_HOSE_MISSING.search(t):
        return {
            "is_issue": True,
            "category": "fire_safety",
            "asset": "stairwell_fire_hoses",
            "event_type": "still_out" if re.search(r"\b(?:again|still)\b", t, re.I) else "new_issue",
            "severity": 4,
            "title": "Stairwell fire hoses missing",
            "summary": "Tenant reports stairwell fire hoses missing or removed.",
            "kind": "issue",
            "preserve_issue": True,
            "preserve_event_type": True,
        }

    if FIRE_HOSE.search(t) and FIRE_HOSE_PROGRESS.search(t):
        cautious_progress = bool(re.search(r"\b(?:looks?\s+like|in\s+process|perhaps|maybe|may|might)\b", t, re.I))
        return {
            "is_issue": True,
            "category": "fire_safety",
            "asset": "stairwell_fire_hoses",
            "event_type": "status_update" if cautious_progress else "restore",
            "severity": 3,
            "title": "Stairwell fire-hose replacement update",
            "summary": "Tenant reports progress replacing the stairwell fire hoses.",
            "kind": "issue" if cautious_progress else "restore",
            "preserve_issue": True,
            "preserve_event_type": True,
        }

    if LAUNDRY.search(t) and LAUNDRY_RESTORE.search(t):
        return {
            "is_issue": True,
            "category": "laundry",
            "asset": explicit_laundry_asset(t),
            "event_type": "restore",
            "severity": 2,
            "title": "Laundry machines repaired",
            "summary": "Tenant reports laundry machines repaired and working again.",
            "kind": "restore",
            "preserve_issue": True,
            "preserve_event_type": True,
        }

    if LAUNDRY.search(t) and LAUNDRY_PROBLEM.search(t):
        return {
            "is_issue": True,
            "category": "laundry",
            "asset": explicit_laundry_asset(t),
            "event_type": "new_issue",
            "severity": 3,
            "title": "Laundry facility issue",
            "summary": "Tenant reports a laundry room, machine, card, or connectivity problem.",
            "kind": "issue",
            "preserve_issue": True,
            "preserve_event_type": True,
        }

    if ELEVATOR_PERFORMANCE.search(t):
        return {
            "is_issue": True,
            "category": "elevator",
            "asset": _asset(t),
            "event_type": "status_update",
            "severity": 3,
            "title": "Elevator operating unusually slowly",
            "summary": "Tenant reports an elevator operating unusually slowly.",
            "kind": "issue",
            "preserve_issue": True,
            "preserve_event_type": True,
        }

    if ELECTRICAL.search(t) and ELECTRICAL_PROBLEM.search(t) and FIRST_PERSON_BUILDING_CONDITION.search(t):
        return {
            "is_issue": True,
            "category": "other",
            "asset": None,
            "event_type": "new_issue",
            "severity": 4,
            "title": "Electrical wiring concern",
            "summary": "Tenant reports an electrical wiring or outlet concern in an apartment.",
            "kind": "issue",
            "preserve_issue": True,
            "preserve_event_type": True,
        }

    if QUESTION_ONLY.search(t) and DISCUSSION_QUESTION.search(t):
        return {"is_issue": False, "category": "other", "asset": None, "severity": 2, "title": "", "summary": "", "kind": "nonissue"}

    if QUESTION_ONLY.search(t) and not OUT.search(t) and not BACK.search(t):
        return {"is_issue": False, "category": "other", "asset": None, "severity": 2, "title": "", "summary": "", "kind": "nonissue"}

    if _has_elevator_reference(t) and (BACK.search(t) or FLOOR_SERVICE_NORMAL.search(t)) and not ONLY_SIDE_WORKING.search(t):
        asset = _asset(t)
        if CAUTIOUS_RESTORE.search(t):
            return {
                "is_issue": True,
                "category": "elevator",
                "asset": asset,
                "event_type": "status_update",
                "severity": 3,
                "title": "Elevator working update",
                "summary": "Tenant reports possible elevator operation without confirming a stable restoration.",
                "kind": "issue",
                "preserve_event_type": True,
            }
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

    if _has_elevator_reference(t) and (OUT.search(t) or ONLY_SIDE_WORKING.search(t) or REDUCED_SERVICE.search(t) or ELEVATOR_ASSET_ZERO.search(t)):
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

    if VENTILATION.search(t) and re.search(r"\b(?:issue|problem|inspect(?:ion|or|ed)?|not\s+working|no\s+air)\b", t, re.I):
        return {
            "is_issue": True,
            "category": "other",
            "asset": None,
            "event_type": "new_issue",
            "severity": 2,
            "title": "Ventilation issue",
            "summary": "Tenant reports a building ventilation concern.",
            "kind": "issue",
        }

    if DOOR_ACCESSIBILITY.search(t):
        return {
            "is_issue": True,
            "category": "security_access",
            "asset": None,
            "event_type": "new_issue",
            "severity": 3,
            "title": "Entry door accessibility issue",
            "summary": "Tenant reports an entry door accessibility problem.",
            "kind": "issue",
        }

    if PERSONAL_SAFETY_REPORT.search(t):
        return {
            "is_issue": True,
            "category": "security_access",
            "asset": None,
            "event_type": "new_issue",
            "severity": 3,
            "title": "Building entrance safety concern",
            "summary": "Tenant reports a personal safety concern at the building entrance.",
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
