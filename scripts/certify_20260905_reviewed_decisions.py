#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.local_env import load_local_env_file

load_local_env_file(ROOT / ".env")

from packages.audit import append_audit_event, daily_hash_chain  # noqa: E402
from packages.db import Incident, MessageDecision, RawMessage, get_session  # noqa: E402


REVIEWED_DECISIONS: dict[str, tuple[bool, str, str]] = {
    "aed1f305b2a28df618bdcf90a5c75a03dbacdd5d675e086aca25a69c4818f45f": (True, "elevator", "restore"),
    "0102c058c53a5e88031ab2dbf4f706e4334c9643cdb3e4e9e35763704781a56a": (True, "elevator", "outage"),
    "e96b94e723975eaec6c9142b0a32a85d5a6ee01c5cf52693dc2f1531c6cae83f": (True, "elevator", "outage"),
    "068a04cabbf90edcf71bbe3bee9a45d4574e1272042129cf00906f69e1ccdf25": (True, "elevator", "restore"),
    "5c2a2ed585cfc485bb922cdc8f9e9998a289e57f7c79710cec2383e6ce0ac0e8": (True, "laundry", "new_issue"),
    "15a25908c6463c13b39916d0d72984c360f7a7824fd419a027432c6ab38bbd9f": (True, "fire_safety", "new_issue"),
    "6d2b0a02b88d17dfbffc77e75d0cd346f4e74e6a9d6c6408bffbe1ff943f5a76": (True, "fire_safety", "status_update"),
    "397f6057898c1f730002108b46377452f5f0055a72e7e2de9f186ed78abbfd7a": (True, "fire_safety", "status_update"),
    "836b5e46e6bd07a88a5eb3a6b03b022e6563a0b861d426ace5ceace9b750fe45": (True, "laundry", "restore"),
    "92a1d837c08522dcb3dc098972f92196033d1381616d7585a7960acda980ed53": (True, "fire_safety", "still_out"),
    "9125ff678cd0e9ff3c7768319371c651c7ea41092acab1fd608517274612cd60": (True, "elevator", "status_update"),
    "04496593dd52942a5278ffd782d9f57dc5b78f9587f86d4b4657de49a94c8d80": (True, "elevator", "status_update"),
    "80a5be7816ff8de14adfb3b6e40cf353d17c444826f33f9a6b5ce1ddd71d029b": (True, "security_access", "new_issue"),
    "5e366948f3903d8171331528c1b745a3636ec107d8a5e6fa56de0f42d2bd91e6": (True, "security_access", "status_update"),
    "8897aa1d8dfd2d2245bff83e50046841280f22f12440ef356e86a11646e09edc": (False, "", ""),
    "1f4bdbbe02e8572fb8047e44d4a2788049bc9db3412469c755d81c7693300d0b": (False, "", ""),
}

REVIEWED_BY = "codex:2026-09-05-455-live-audit"


def _json_object(value: str | None) -> dict[str, object]:
    try:
        parsed = json.loads(value or "{}")
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def certify(*, apply: bool) -> dict[str, object]:
    errors: list[str] = []
    already_certified: list[str] = []
    to_certify: list[str] = []
    touched_incident_ids: set[str] = set()
    reviewed_at = dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")

    with get_session() as session:
        for message_id, (expected_is_issue, expected_category, expected_event_type) in REVIEWED_DECISIONS.items():
            raw = session.get(RawMessage, message_id)
            decision = session.get(MessageDecision, message_id)
            if raw is None:
                errors.append(f"{message_id}: raw message missing")
                continue
            if decision is None:
                errors.append(f"{message_id}: decision missing")
                continue
            actual = (bool(decision.is_issue), decision.category or "", decision.event_type or "")
            expected = (expected_is_issue, expected_category, expected_event_type)
            incident_state_invalid = bool(
                (expected_is_issue and not decision.incident_id)
                or (not expected_is_issue and decision.incident_id)
            )
            if actual != expected or incident_state_invalid:
                incident_note = " with incident" if expected_is_issue else " without issue incident"
                errors.append(f"{message_id}: expected {expected}{incident_note}, got {actual}")
                continue
            final = _json_object(decision.final_json)
            if (
                str(decision.chosen_source or "") == "review_codex_semantic_audit"
                and final.get("review_status") == "completed"
                and final.get("reviewed_by") == REVIEWED_BY
                and not decision.needs_review
            ):
                already_certified.append(message_id)
                continue
            to_certify.append(message_id)
            if apply:
                final.update(
                    {
                        "review_status": "completed",
                        "review_kind": "codex_semantic_audit",
                        "reviewed_at": reviewed_at,
                        "reviewed_by": REVIEWED_BY,
                        "needs_review": False,
                    }
                )
                decision.final_json = json.dumps(final, sort_keys=True)
                decision.chosen_source = "review_codex_semantic_audit"
                decision.needs_review = False
                if decision.incident_id:
                    touched_incident_ids.add(decision.incident_id)

        if apply and not errors:
            session.flush()
            for incident_id in sorted(touched_incident_ids):
                incident = session.get(Incident, incident_id)
                if incident is None:
                    errors.append(f"{incident_id}: reviewed incident missing")
                    continue
                linked = (
                    session.query(MessageDecision)
                    .filter(
                        MessageDecision.incident_id == incident_id,
                        MessageDecision.is_issue.is_(True),
                    )
                    .all()
                )
                incident.needs_review = any(bool(row.needs_review) for row in linked)

        if errors or not apply:
            session.rollback()
        else:
            session.commit()

    result: dict[str, object] = {
        "apply": apply,
        "reviewed_by": REVIEWED_BY,
        "expected": len(REVIEWED_DECISIONS),
        "to_certify": to_certify,
        "already_certified": already_certified,
        "errors": errors,
        "applied": bool(apply and not errors),
    }
    if apply and not errors and to_certify:
        append_audit_event(
            "CODEX_SEMANTIC_REVIEW_COMPLETED",
            None,
            {
                "reviewed_by": REVIEWED_BY,
                "reviewed_at": reviewed_at,
                "message_ids": to_certify,
                "expected_decisions": {
                    message_id: {"is_issue": is_issue, "category": category, "event_type": event_type}
                    for message_id, (is_issue, category, event_type) in REVIEWED_DECISIONS.items()
                    if message_id in to_certify
                },
            },
        )
        daily_hash_chain()
    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Certify the exact decisions manually reviewed during the 2026-09-05 live audit."
    )
    parser.add_argument("--apply", action="store_true", help="Apply review provenance; default is read-only.")
    args = parser.parse_args()
    result = certify(apply=args.apply)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 1 if result["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
