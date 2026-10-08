"""Narrow literal elevator observations that older broad outage summaries lost."""
from __future__ import annotations

import re


def _result(label: str, summary: str, *, include: bool = True) -> dict[str, object]:
    return {"label": label, "summary": summary, "include": include}


def elevator_observation_presentation(text: str) -> dict[str, object] | None:
    """Use only explicit text; the caller establishes a linked elevator context."""
    lowered = " ".join((text or "").split()).casefold()
    uncertain = re.compile(r"\b(?:maybe|may|might|possibly|possible|think|seems?|appears?)\b")
    side_names = {side for side in ("north", "south") if re.search(rf"\b{side}\b", lowered)}
    # Questions and silence alone do not establish an outage.
    if re.search(r"\b(?:heard|hear)\s+no\s+elevator\s+moving\b", lowered):
        return _result("", "", include=False)
    if re.search(r"^(?:is|are|could|can|does|do)\b.*\b(?:north|south|both|elevators?)\b", lowered) or (lowered.endswith("?") and re.fullmatch(r"(?:maybe\s+)?(?:both|north|south)(?:\s+elevators?)?\s+(?:are\s+|is\s+)?(?:out|down|dead)(?:\s+again)?\?", lowered)):
        return _result("", "", include=False)
    # Preserve established temporal, entrapment and safety-specific handling.
    if re.search(r"\b(?:earlier|last\s+night|yesterday|previously)\b", lowered) or re.search(r"\b(?:trapped|stuck\s+inside|alarm|rescu\w*|firefighter\w*)\b", lowered):
        return None
    states: dict[str, tuple[str, bool]] = {}
    clauses = re.split(r"[.;!?]|\b(?:and|but|while)\b", lowered)
    for clause in clauses:
        sides = [side for side in side_names if re.search(rf"\b{side}\b", clause)]
        if len(sides) != 1:
            continue
        side = sides[0]
        state = None
        if re.search(r"\b(?:questionable|rattl\w*|erratic\w*|slow\w*|irregular\w*)\b", clause):
            state = "operating slowly" if "slow" in clause else "rattling" if "rattl" in clause else "stopping irregularly" if "stopp" in clause else "unreliable"
        elif re.search(r"\b(?:out|down|dead|broken|not\s+working)\b", clause):
            state = "out"
        elif re.search(r"\b(?:working|operating|running)\b", clause):
            state = "working"
        if state:
            states[side] = (state, bool(uncertain.search(clause)))
    both_out = re.search(r"\bboth(?:\s+(?:elevators?|lifts?|of\s+them))?\s+(?:(?:are|may|might|be|seems?|appears?|to|possibly|probably|currently|still|also|maybe)\s+)*(?:out|down|dead)\b", lowered)
    if uncertain.search(lowered) and both_out:
        explicit = [side for side, (state, tentative) in states.items() if state == "out" and not tentative]
        if len(explicit) == 1:
            side = explicit[0]
            other = "south" if side == "north" else "north"
            return _result(f"{side.title()} elevator out; other car uncertain", f"{side.title()} elevator was reported out; whether the {other} elevator was also out was uncertain.")
        return _result("", "", include=False)
    if uncertain.search(lowered) and re.search(r"\bboth\b.*\bworking\b", lowered):
        detail = " without floor-by-floor service" if re.search(r"not\s+(?:going\s+)?down\s+floor\s+by\s+floor", lowered) else ""
        return _result("Both elevators tentatively reported working", f"A tenant reported that both elevators appeared to be working{detail}.")
    if len(states) == 2 and len(set(states.values())) > 1:
        summary = "; ".join(f"{side.title()} elevator was {'tentatively ' if states[side][1] else ''}reported {states[side][0]}" for side in ("north", "south")) + "."
        return _result("Elevator service: different conditions by car", summary)
    if len(states) == 1 and any(tentative for _state, tentative in states.values()):
        side, (state, _tentative) = next(iter(states.items()))
        return _result(f"{side.title()} elevator: unconfirmed status", f"The {side} elevator was tentatively reported {state}; this was not confirmed.")
    floor_failure = re.search(r"\b(?:doesn['’]?t|won['’]?t|wouldn['’]?t|never)\s+(?:stop|come|arriv\w*)\b", lowered) and re.search(r"\b(?:floor|doorman|send|sent)\b", lowered)
    staff_sent = re.search(r"\bhad\s+to\s+call\b.*\b(?:sent|send)\s+(?:it\s+)?up\b", lowered)
    if floor_failure or staff_sent:
        side = next(iter(side_names)).title() + " elevator" if len(side_names) == 1 else "An elevator"
        summary = f"{side} was reported failing to respond to a floor call."
        if staff_sent or re.search(r"\b(?:staff|doorman)\b.{0,45}\b(?:send|sent|help)\b", lowered):
            summary += " Staff assistance was needed."
        if re.search(r"\b(?:very\s+)?slow\b", lowered):
            summary += " A subsequent ride was reported unusually slow."
        return _result("Elevator floor-call problem", summary)
    if re.search(r"\bno\s+mechanics?\b|\bmechanics?\b.{0,30}\b(?:not|hasn['’]?t|haven['’]?t)\b.{0,20}\b(?:here|arrived|onsite|on\s+site|in\s+the\s+building)\b", lowered):
        tentative = any(uncertain.search(clause) and re.search(r"\bmechanics?\b", clause) for clause in clauses)
        summary = "A tenant thought no elevator mechanic was on site; this was not confirmed." if tentative else "No elevator mechanic was reported on site."
        if re.search(r"\b(?:staff|doorman)\b.*\b(?:said|says)\b.*\b(?:will|would)\b.*\bcall\b", lowered):
            summary += " Staff said they would call for service."
        return _result("Elevator mechanic presence unconfirmed" if tentative else "Elevator mechanic not on site", summary)
    if re.search(r"\bmechanics?\b.{0,30}\b(?:arrived|here|on\s+site|in\s+(?:the\s+)?building)\b", lowered) and not re.search(r"\b(?:will|would|expected|should|tomorrow|not|never|no|hasn['’]?t)\b", lowered):
        out_side = [side for side, (state, _tentative) in states.items() if state == "out"]
        prefix = f"{out_side[0].title()} elevator remained out; " if len(out_side) == 1 else ""
        arrival = "Elevator mechanics were reported on site." if re.search(r"\bmechanics\b", lowered) else "Elevator mechanic was reported on site."
        return _result("Elevator repair visit", prefix + ("a mechanic was reported on site." if prefix else arrival))
    if len(side_names) == 1 and re.search(r"\b(?:i|we)\b.*\brode\b", lowered) and re.search(r"\bfine\b", lowered):
        side = next(iter(side_names))
        return _result(f"{side.title()} elevator ride reported", f"A tenant reported a successful ride in the {side} elevator.")
    short_out = re.fullmatch(r"(north|south)(?:\s+elevator)?(?:\s+is|\s+was)?\s+(still\s+)?(?:out|down|not\s+working)(\s+again)?[.!]?", lowered)
    if short_out:
        side, still, again = short_out.groups()
        return _result(f"{side.title()} elevator", f"{side.title()} elevator was reported as {'still out' if still else 'out'}{' again' if again else ''}.")
    if re.search(r"\bboth\s+(?:are\s+)?out\b", lowered) and re.search(r"\b(?:staff|doorman)\b.*\bsaid\b|\bsaid\b.*\bboth\b", lowered):
        return _result("Both elevators", "Both elevators were reported out. This update was attributed to building staff.")
    return None
