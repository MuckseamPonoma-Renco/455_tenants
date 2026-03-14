import re

PREFILTER = re.compile(
    r"(elevator|lift|stuck|out of service|not working|still down|again|down|broken|shutdown|"
    r"heat|boiler|hot water|cold water|no hot|no heat|radiator|"
    r"leak|flood|water damage|mold|ceiling|"
    r"roach|mice|rat|bed bug|"
    r"lock|door|intercom|camera|security|unsafe|"
    r"gas|smoke|fire|power outage|electric|blackout|)",
    re.IGNORECASE
)

def should_call_llm(text: str, rules_is_issue: bool, rules_kind: str) -> bool:
    if rules_is_issue and rules_kind in ("outage", "restore", "issue"):
        return False
    if PREFILTER.search(text or ""):
        return True
    return False
