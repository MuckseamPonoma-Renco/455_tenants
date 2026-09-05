#!/usr/bin/env python3
from __future__ import annotations

import argparse
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
from packages.incident import extractor  # noqa: E402


TARGET_MESSAGE_ID = "5e366948f3903d8171331528c1b745a3636ec107d8a5e6fa56de0f42d2bd91e6"
TARGET_INCIDENT_ID = "f9621fad6b23ca099c9d57e803846d0e"
EXPECTED_TEXT = "1 lift, 1 functional fire stair."
EXPECTED_DECISION = (True, "security_access", "status_update", TARGET_INCIDENT_ID)
INITIAL_MESSAGE_ID = "80a5be7816ff8de14adfb3b6e40cf353d17c444826f33f9a6b5ce1ddd71d029b"
CORROBORATING_MESSAGE_ID = "15df7a123c772342b9065b515eeb9dda2732812653e929faa5ad15e43e5ab3dd"
EXPECTED_INCIDENT_MESSAGE_IDS = {INITIAL_MESSAGE_ID, CORROBORATING_MESSAGE_ID, TARGET_MESSAGE_ID}


def _decision_state(decision: MessageDecision | None) -> tuple[bool, str, str, str]:
    if decision is None:
        return False, "", "", ""
    return (
        bool(decision.is_issue),
        str(decision.category or ""),
        str(decision.event_type or ""),
        str(decision.incident_id or ""),
    )


def repair(*, apply: bool) -> dict[str, object]:
    errors: list[str] = []
    changed = False
    before: tuple[bool, str, str, str]
    after: tuple[bool, str, str, str]

    with get_session() as session:
        raw = session.get(RawMessage, TARGET_MESSAGE_ID)
        decision = session.get(MessageDecision, TARGET_MESSAGE_ID)
        incident = session.get(Incident, TARGET_INCIDENT_ID)
        before = _decision_state(decision)

        if raw is None:
            errors.append("target raw message is missing")
        elif (raw.text or "").strip() != EXPECTED_TEXT:
            errors.append("target raw message text does not match the audited export")
        if incident is None:
            errors.append("audited fire-stair access incident is missing")
        elif incident.category != "security_access":
            errors.append("audited target incident is not a security/access incident")

        initial = session.get(MessageDecision, INITIAL_MESSAGE_ID)
        corroborating = session.get(MessageDecision, CORROBORATING_MESSAGE_ID)
        expected_initial = (True, "security_access", "new_issue", TARGET_INCIDENT_ID)
        if _decision_state(initial) != expected_initial:
            errors.append("initial stuck-door report no longer matches the audited decision")
        if _decision_state(corroborating) != expected_initial:
            errors.append("corroborating fire-stair report no longer matches the audited decision")

        contextual_choice = extractor._contextual_topic_followup_choice(session, raw) if raw is not None else None
        if not isinstance(contextual_choice, dict):
            errors.append("the target message is not recognized as a contextual follow-up")
        elif str(contextual_choice.get("target_incident_id") or "") != TARGET_INCIDENT_ID:
            errors.append("contextual follow-up does not resolve to the audited fire-stair incident")

        if not errors and apply:
            if before != EXPECTED_DECISION:
                original_llm_mode = extractor.LLM_MODE
                extractor.LLM_MODE = "off"
                try:
                    extractor.classify_and_upsert_incident(session, raw, allow_filing_job=False)
                finally:
                    extractor.LLM_MODE = original_llm_mode
                session.flush()
                changed = True

            linked = (
                session.query(MessageDecision)
                .filter(MessageDecision.incident_id == TARGET_INCIDENT_ID)
                .all()
            )
            linked_ids = {row.message_id for row in linked}
            unexpected = sorted(linked_ids - EXPECTED_INCIDENT_MESSAGE_IDS)
            missing_linked = sorted(EXPECTED_INCIDENT_MESSAGE_IDS - linked_ids)
            if unexpected:
                errors.append(f"unexpected decisions are linked to the audited incident: {unexpected}")
            if missing_linked:
                errors.append(f"audited incident decisions are missing: {missing_linked}")
            if not errors and incident is not None:
                incident.report_count = len([row for row in linked if row.is_issue])
                if incident.report_count != 3:
                    errors.append(f"expected three fire-stair reports, got {incident.report_count}")

        decision = session.get(MessageDecision, TARGET_MESSAGE_ID)
        after = _decision_state(decision)
        if not errors and (apply or before == EXPECTED_DECISION) and after != EXPECTED_DECISION:
            errors.append(f"expected {EXPECTED_DECISION}, got {after}")

        if errors or not apply:
            session.rollback()
        else:
            session.commit()

    result: dict[str, object] = {
        "apply": apply,
        "target_message_id": TARGET_MESSAGE_ID,
        "target_incident_id": TARGET_INCIDENT_ID,
        "before": before,
        "after": after,
        "changed": changed,
        "already_correct": before == EXPECTED_DECISION,
        "errors": errors,
        "applied": bool(apply and not errors),
    }
    if apply and changed and not errors:
        append_audit_event(
            "FIRE_STAIR_CONTEXT_REPAIR",
            TARGET_INCIDENT_ID,
            {
                "message_id": TARGET_MESSAGE_ID,
                "before": before,
                "after": after,
                "reason": "attach limited fire-stair egress follow-up to the preceding stuck-door incident",
            },
        )
        daily_hash_chain()
    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Repair the audited July 23 limited fire-stair follow-up classification."
    )
    parser.add_argument("--apply", action="store_true", help="Apply the repair; default is read-only.")
    args = parser.parse_args()
    result = repair(apply=args.apply)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 1 if result["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
