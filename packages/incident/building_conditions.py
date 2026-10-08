"""Narrow, firsthand ventilation and electrical reports shared by intake and display."""
from __future__ import annotations

import re


def building_condition_presentation(text: str) -> dict[str, str] | None:
    """Recognize a stated condition, never infer code violations or health effects."""
    clean = " ".join((text or "").split())
    if not re.search(r"\b(?:my|mine|our|ours|we|i)\b", clean, re.I):
        return None
    if re.search(r"\b(?:elevators?|lifts?|trapped|fire|entered|entry)\b", clean, re.I):
        return None
    vent_context = bool(re.search(r"\b(?:vents?|ventilation)\b", clean, re.I))
    # An opening question must not erase a separate firsthand statement.
    # Context may resolve 'mine' within this one message, never across tenants.
    clauses = re.split(r"[.!?]", clean)
    observed = [clause for clause in clauses if re.search(r"\b(?:my|mine|our|ours|we|i)\b", clause, re.I) and re.search(r"\b(?:grime|dust|dirty|clotted|odou?r|smell\w*|painted\s+over|blocked|covered|outlets?|wiring)\b", clause, re.I)]
    if observed:
        clean = observed[-1]
    # A conservative fallback, not a general classifier. Advice, questions about
    # other people, negation, resolved conditions and competing hazards defer.
    if re.search(r"\b(?:if|hypothetically|should|would|whether\s+anyone|does\s+anyone|did\s+anyone|elevators?|trapped|fire|entered|entry)\b", clean, re.I):
        return None
    if re.search(r"\b(?:no\s+(?:more\s+)?(?:dust|grime|odou?r|smell)|(?:do|does|did)\s+not|don['’]?t|doesn['’]?t|didn['’]?t|not\s+(?:dirty|blocked|covered|painted)|anymore|no\s+longer|resolved|fixed|are\s+fine|is\s+fine|were\s+fine|was\s+fine)\b", clean, re.I):
        return None
    if re.search(r"\b(?:used\s+to|last\s+year|years?\s+ago|previously)\b", clean, re.I):
        return None
    personal_observation = re.search(r"\b(?:my|our)\b.{0,70}\b(?:vents?|ventilation|outlets?|wiring|wires?)\b|\b(?:mine|ours)\b.{0,15}\b(?:are|is|have)\b|\b(?:i|we)\b.{0,45}\b(?:have|had|noticed|see|saw|smell|covered|getting)\b", clean, re.I)
    if vent_context and personal_observation:
        if re.search(r"\b(?:odor|odour|smell\w*|scented)\b", clean, re.I):
            summary = "A tenant reported unwanted odors coming through apartment vents."
        elif re.search(r"\bpainted\s+over\b", clean, re.I):
            summary = "A tenant reported painted-over apartment vent covers."
        elif re.search(r"\b(?:dust|grime|dirty|clotted)\b", clean, re.I):
            summary = "A tenant reported grime or dust buildup on apartment vents."
        elif re.search(r"\b(?:blocked|covered)\b", clean, re.I):
            summary = "A tenant reported covered or blocked apartment vents."
        else:
            return None
        return {"kind": "ventilation", "label": "Apartment ventilation concern", "summary": summary}
    electrical_request = bool(re.search(r"\b(?:i|we)\b.{0,35}\b(?:asked|requested)\b.{0,90}\b(?:check|checked|inspect|inspected|inspection)\b", clean, re.I))
    electrical_condition = re.search(r"\b(?:painted\s+over|not\s+working|only\s+functioning|spark\w*|burn\w*|exposed)\b", clean, re.I)
    if re.search(r"\b(?:outlets?|wiring|wires?|electrical)\b", clean, re.I) and ((personal_observation and electrical_condition) or electrical_request):
        if re.search(r"\bpainted\s+over\b", clean, re.I):
            summary = "A tenant reported painted-over electrical outlets."
        elif electrical_request:
            return {"kind": "electrical", "label": "Electrical inspection request", "summary": "A tenant reported requesting an inspection of apartment outlets or wiring.", "event_type": "status_update"}
        else:
            summary = "A tenant reported an apartment outlet or wiring concern."
        return {"kind": "electrical", "label": "Electrical outlet / wiring concern", "summary": summary}
    return None
