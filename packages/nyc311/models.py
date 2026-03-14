from __future__ import annotations
import json
from dataclasses import dataclass
from typing import Any


@dataclass
class FilingDraft:
    complaint_type: str
    form_target: str
    title: str
    description: str
    category: str
    incident_id: str
    payload: dict[str, Any]

    def payload_json(self) -> str:
        return json.dumps(self.payload, ensure_ascii=False, sort_keys=True)
