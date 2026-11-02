"""One schema for API constraints and local validation of model-owned fields."""
from packages.llm.openai_client import OpenAIError

CATEGORIES = ["elevator", "heat_hot_water", "leaks_water_damage", "pests", "security_access", "fire_safety", "laundry", "other"]
PROPERTIES = {
    "is_issue": {"type": "boolean"},
    "signal_type": {"type": "string", "enum": ["report", "discussion"]},
    "category": {"type": "string", "enum": CATEGORIES},
    "asset": {"type": ["string", "null"], "enum": ["elevator_north", "elevator_south", "elevator_both", None]},
    "event_type": {"type": "string", "enum": ["outage", "restore", "still_out", "new_issue", "status_update", "non_issue"]},
    "severity": {"type": "integer", "minimum": 1, "maximum": 5},
    "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
    "title": {"type": "string", "maxLength": 60},
    "summary": {"type": "string", "maxLength": 200},
    "refers_to_open_incident": {"type": "boolean"},
    "close_incident": {"type": "boolean"},
    "needs_review": {"type": "boolean"},
}
SCHEMA = {"type": "object", "properties": PROPERTIES, "required": list(PROPERTIES), "additionalProperties": False}


def validate(value: object) -> dict:
    if not isinstance(value, dict) or set(value) != set(PROPERTIES):
        raise OpenAIError("invalid_classification_schema")
    for key, spec in PROPERTIES.items():
        field = value[key]; kind = spec["type"]
        valid = ((kind == "boolean" and type(field) is bool)
                 or (kind == "integer" and type(field) is int)
                 or (kind == "string" and isinstance(field, str))
                 or (isinstance(kind, list) and (field is None or isinstance(field, str))))
        if not valid or ("enum" in spec and field not in spec["enum"]):
            raise OpenAIError("invalid_classification_schema")
        if "minimum" in spec and not spec["minimum"] <= field <= spec["maximum"]:
            raise OpenAIError("invalid_classification_schema")
        if "maxLength" in spec and len(field) > spec["maxLength"]:
            raise OpenAIError("invalid_classification_schema")
    if (not value["is_issue"] and (value["signal_type"] != "discussion" or value["event_type"] != "non_issue"
                                   or value["category"] != "other" or value["asset"] is not None or value["close_incident"])):
        raise OpenAIError("inconsistent_classification")
    if value["is_issue"] and (value["signal_type"] != "report" or value["event_type"] == "non_issue"):
        raise OpenAIError("inconsistent_classification")
    if value["close_incident"] != (value["event_type"] == "restore"):
        raise OpenAIError("inconsistent_classification")
    return dict(value)
