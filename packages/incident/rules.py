import re

ELEVATOR = re.compile(r"\b(elevator|elevators|lift|lifts)\b", re.I)
ELEVATOR_ASSET_NORTH = re.compile(r"\bnorth\b", re.I)
ELEVATOR_ASSET_SOUTH = re.compile(r"\bsouth\b", re.I)
ELEVATOR_ASSET_BOTH = re.compile(r"\b(both|two elevators|2 lifts|2 elevators)\b", re.I)

OUT = re.compile(
    r"(out\s+of\s+service|not\s+working|broken|stuck|down|shutdown|shut\s*off|still\s+down|still\s+not\s+working|again\s+down|out\s+again|again\s+out|dead|down\s+to\s+1\s+elevator|one\s+elevator\s+again|only\s+1\s+elevator|misbehaving|not\s+arrived|not\s+to\s+cool overnight|both\s+elevators\s+are\s+out|both\s+lifts\s+are\s+out)",
    re.I,
)
CONTINUING = re.compile(r"\b(still|again)\b", re.I)
BACK = re.compile(
    r"(back\s+(up|on|in\s+service)|working\s+now|operational\s+again|fixed|restored|currently\s+working|currently\s+functioning|2\s+lifts\s+working|both\s+elevators\s+currently\s+functioning|seemed\s+to\s+come\s+at\s+a\s+normal\s+speed|they'?re\s+working\s+now)",
    re.I,
)

HEAT = re.compile(r"\bheat\b|hot\s+water|no\s+hot\s+water|cold\s+water|boiler", re.I)
LEAK = re.compile(r"leak|flood|water\s+damage|ceiling\s+collapsed|mold", re.I)
PESTS = re.compile(r"\b(?:roach(?:es)?|mice|mouse|rats?|bed\s*bugs?|bugs)\b", re.I)
SEC = re.compile(r"lock|door|intercom|camera|security|stair|fire\s+door|handrail", re.I)
QUESTION_ONLY = re.compile(r"\?$")


def _asset(text: str):
    if ELEVATOR_ASSET_BOTH.search(text):
        return "elevator_both"
    if ELEVATOR_ASSET_NORTH.search(text):
        return "elevator_north"
    if ELEVATOR_ASSET_SOUTH.search(text):
        return "elevator_south"
    return None


def explicit_elevator_asset(text: str):
    return _asset(text or "")


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

    if QUESTION_ONLY.search(t) and not OUT.search(t) and not BACK.search(t):
        return {"is_issue": False, "category": "other", "asset": None, "severity": 2, "title": "", "summary": "", "kind": "nonissue"}

    if ELEVATOR.search(t) and BACK.search(t):
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

    if ELEVATOR.search(t) and OUT.search(t):
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
