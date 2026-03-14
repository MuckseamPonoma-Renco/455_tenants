from __future__ import annotations
import os
from packages.llm.openai_client import call_openai_json, OpenAIError

CATEGORIES = [
    "elevator",
    "heat_hot_water",
    "leaks_water_damage",
    "pests",
    "security_access",
    "other",
]

def _env(name: str, default: str) -> str:
    v = os.environ.get(name)
    return v if v is not None and v != "" else default

def _int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except Exception:
        return default

def _build_prompt(message_text: str, open_incidents: list[dict], recent_related: list[dict]) -> str:
    return f"""You are classifying messages from a tenants WhatsApp group about building issues.

Return ONLY valid JSON with this schema:
{{
  "is_issue": boolean,
  "signal_type": "report" | "discussion",
  "category": one of {CATEGORIES},
  "asset": one of ["elevator_north","elevator_south","elevator_both", null],
  "event_type": "outage" | "restore" | "still_out" | "new_issue" | "status_update" | "non_issue",
  "severity": 1-5,
  "confidence": 0-100,
  "title": string (<=60 chars),
  "summary": string (<=200 chars),
  "refers_to_open_incident": boolean,
  "close_incident": boolean,
  "needs_review": boolean
}}

Rules:
- "report" asserts a real condition/state (e.g., 'elevator is down', 'still not working', 'back in service').
- "discussion" asks/talks without asserting state ('any update?', 'who called?').
- close_incident=true only if clearly resolved/restored.
- Prefer high recall, but set needs_review=true when ambiguous.
- If elevators mentioned but no north/south: asset=null. If both elevators: asset="elevator_both".
- If not a building issue: is_issue=false, category="other", event_type="non_issue".

Open incidents (current state):
{open_incidents}

Recent possibly-related messages:
{recent_related}

New message:
{message_text}
"""


def llm_classify_message(message_text: str, open_incidents: list[dict] | None = None, recent_related: list[dict] | None = None) -> dict:
    open_incidents = open_incidents or []
    recent_related = recent_related or []

    if not (os.environ.get("OPENAI_API_KEY") or os.environ.get("LLM_API_KEY")):
        return {"is_issue": False, "category": "other", "asset": None, "severity": 2, "title": "", "summary": "", "kind":"nonissue"}

    model = _env("OPENAI_MODEL", _env("LLM_MODEL", "gpt-4.1-mini"))
    escalate_model = _env("OPENAI_ESCALATE_MODEL", "gpt-5-mini")
    min_conf = _int_env("LLM_MIN_CONFIDENCE", 65)
    max_out = _int_env("LLM_MAX_OUTPUT_TOKENS", 220)

    prompt = _build_prompt(message_text, open_incidents, recent_related)

    try:
        out = call_openai_json(prompt, model=model, max_output_tokens=max_out)
    except OpenAIError:
        return {"is_issue": False, "category": "other", "asset": None, "severity": 2, "title": "", "summary": "",
                "kind":"nonissue", "needs_review": True, "confidence": 0}

    # defaults
    out.setdefault("is_issue", False)
    out.setdefault("signal_type", "discussion")
    out.setdefault("category", "other")
    out.setdefault("asset", None)
    out.setdefault("event_type", "non_issue")
    out.setdefault("severity", 2)
    out.setdefault("confidence", 50)
    out.setdefault("title", "")
    out.setdefault("summary", "")
    out.setdefault("refers_to_open_incident", False)
    out.setdefault("close_incident", False)
    out.setdefault("needs_review", False)

    if (out.get("is_issue") or out.get("refers_to_open_incident")) and int(out.get("confidence", 0)) < min_conf:
        try:
            out2 = call_openai_json(prompt, model=escalate_model, max_output_tokens=max_out)
            if isinstance(out2, dict):
                out = out2
        except Exception:
            out["needs_review"] = True

    return out
