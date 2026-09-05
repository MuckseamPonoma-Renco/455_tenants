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
from packages.db import MessageDecision, RawMessage, get_session  # noqa: E402


REVIEWED_DECISIONS: dict[str, tuple[str, str]] = {
    "aed1f305b2a28df618bdcf90a5c75a03dbacdd5d675e086aca25a69c4818f45f": ("elevator", "restore"),
    "0102c058c53a5e88031ab2dbf4f706e4334c9643cdb3e4e9e35763704781a56a": ("elevator", "outage"),
    "e96b94e723975eaec6c9142b0a32a85d5a6ee01c5cf52693dc2f1531c6cae83f": ("elevator", "outage"),
    "068a04cabbf90edcf71bbe3bee9a45d4574e1272042129cf00906f69e1ccdf25": ("elevator", "restore"),
    "5c2a2ed585cfc485bb922cdc8f9e9998a289e57f7c79710cec2383e6ce0ac0e8": ("laundry", "new_issue"),
    "15a25908c6463c13b39916d0d72984c360f7a7824fd419a027432c6ab38bbd9f": ("fire_safety", "new_issue"),
    "6d2b0a02b88d17dfbffc77e75d0cd346f4e74e6a9d6c6408bffbe1ff943f5a76": ("fire_safety", "status_update"),
    "397f6057898c1f730002108b46377452f5f0055a72e7e2de9f186ed78abbfd7a": ("fire_safety", "status_update"),
    "836b5e46e6bd07a88a5eb3a6b03b022e6563a0b861d426ace5ceace9b750fe45": ("laundry", "restore"),
    "92a1d837c08522dcb3dc098972f92196033d1381616d7585a7960acda980ed53": ("fire_safety", "still_out"),
    "9125ff678cd0e9ff3c7768319371c651c7ea41092acab1fd608517274612cd60": ("elevator", "status_update"),
    "04496593dd52942a5278ffd782d9f57dc5b78f9587f86d4b4657de49a94c8d80": ("elevator", "status_update"),
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
    reviewed_at = dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")

    with get_session() as session:
        for message_id, (expected_category, expected_event_type) in REVIEWED_DECISIONS.items():
            raw = session.get(RawMessage, message_id)
            decision = session.get(MessageDecision, message_id)
            if raw is None:
                errors.append(f"{message_id}: raw message missing")
                continue
            if decision is None:
                errors.append(f"{message_id}: decision missing")
                continue
            actual = (bool(decision.is_issue), decision.category or "", decision.event_type or "")
            expected = (True, expected_category, expected_event_type)
            if actual != expected or not decision.incident_id or decision.needs_review:
                errors.append(f"{message_id}: expected {expected} with incident/no review, got {actual}")
                continue
            final = _json_object(decision.final_json)
            if (
                str(decision.chosen_source or "") == "review_codex_semantic_audit"
                and final.get("review_status") == "completed"
                and final.get("reviewed_by") == REVIEWED_BY
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
                    }
                )
                decision.final_json = json.dumps(final, sort_keys=True)
                decision.chosen_source = "review_codex_semantic_audit"

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
                    message_id: {"category": category, "event_type": event_type}
                    for message_id, (category, event_type) in REVIEWED_DECISIONS.items()
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
