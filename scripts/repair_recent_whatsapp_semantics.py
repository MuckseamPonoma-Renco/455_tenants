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
from packages.db import FilingJob, Incident, MessageDecision, RawMessage, ServiceRequestCase, get_session  # noqa: E402
from packages.incident import extractor  # noqa: E402
from scripts.reconcile_cross_source_duplicates import run_reconciliation  # noqa: E402


TARGET_MESSAGE_IDS = (
    "5c2a2ed585cfc485bb922cdc8f9e9998a289e57f7c79710cec2383e6ce0ac0e8",
    "15a25908c6463c13b39916d0d72984c360f7a7824fd419a027432c6ab38bbd9f",
    "6d2b0a02b88d17dfbffc77e75d0cd346f4e74e6a9d6c6408bffbe1ff943f5a76",
    "34b79d693c59b1d123c04cfa7e60b0038db7522a1dc9b1f2fb99aeae878e67eb",
    "397f6057898c1f730002108b46377452f5f0055a72e7e2de9f186ed78abbfd7a",
    "836b5e46e6bd07a88a5eb3a6b03b022e6563a0b861d426ace5ceace9b750fe45",
    "92a1d837c08522dcb3dc098972f92196033d1381616d7585a7960acda980ed53",
    "9125ff678cd0e9ff3c7768319371c651c7ea41092acab1fd608517274612cd60",
    "232fd16cec6b7f9ee5b7ff7af3ce2b30f59545b6538352d0bcf4ac684cfdbe44",
    "04496593dd52942a5278ffd782d9f57dc5b78f9587f86d4b4657de49a94c8d80",
)

REPAIRABLE_OLD_INCIDENT_IDS = (
    "cf8a6befbb0891c0f758b90e8fa7a926",
    "cdfdb82a79a74379f8ca7554e9ef105e",
    "c1a05308d69796dcd03135c8b3576fa3",
    "6c996dc1aa0bf2a08dbf5cc995bb64d6",
)

NORTH_SLOW_INCIDENT_ID = "8040453a06f8e7da4083145091b2533e"

EXPECTED_CATEGORY_EVENT = {
    TARGET_MESSAGE_IDS[0]: ("laundry", "new_issue"),
    TARGET_MESSAGE_IDS[1]: ("fire_safety", "new_issue"),
    TARGET_MESSAGE_IDS[2]: ("fire_safety", "status_update"),
    TARGET_MESSAGE_IDS[3]: ("fire_safety", "status_update"),
    TARGET_MESSAGE_IDS[4]: ("fire_safety", "status_update"),
    TARGET_MESSAGE_IDS[5]: ("laundry", "restore"),
    TARGET_MESSAGE_IDS[6]: ("fire_safety", "still_out"),
    TARGET_MESSAGE_IDS[7]: ("elevator", "status_update"),
    TARGET_MESSAGE_IDS[8]: ("elevator", "status_update"),
    TARGET_MESSAGE_IDS[9]: ("elevator", "status_update"),
}


def _protected_references(session, incident_ids: set[str]) -> list[dict[str, object]]:
    protected: list[dict[str, object]] = []
    for incident_id in sorted(incident_ids):
        cases = session.query(ServiceRequestCase).filter(ServiceRequestCase.incident_id == incident_id).all()
        jobs = session.query(FilingJob).filter(FilingJob.incident_id == incident_id).all()
        if cases or any(job.state in {"claimed", "submitted"} for job in jobs):
            protected.append(
                {
                    "incident_id": incident_id,
                    "service_requests": [case.service_request_number for case in cases],
                    "protected_jobs": [job.job_id for job in jobs if job.state in {"claimed", "submitted"}],
                }
            )
    return protected


def repair(*, apply: bool, reconcile: bool = True) -> dict[str, object]:
    with get_session() as session:
        raws = session.query(RawMessage).filter(RawMessage.message_id.in_(TARGET_MESSAGE_IDS)).all()
        raw_by_id = {row.message_id: row for row in raws}
        missing = [message_id for message_id in TARGET_MESSAGE_IDS if message_id not in raw_by_id]
        decisions = session.query(MessageDecision).filter(MessageDecision.message_id.in_(TARGET_MESSAGE_IDS)).all()
        before = {
            row.message_id: {
                "category": row.category,
                "event_type": row.event_type,
                "incident_id": row.incident_id,
                "is_issue": bool(row.is_issue),
            }
            for row in decisions
        }
        touched_incident_ids = {row.incident_id for row in decisions if row.incident_id}
        protected = _protected_references(session, set(REPAIRABLE_OLD_INCIDENT_IDS))
        plan: dict[str, object] = {
            "apply": apply,
            "target_messages": len(TARGET_MESSAGE_IDS),
            "found_messages": len(raw_by_id),
            "missing_messages": missing,
            "before": before,
            "repairable_old_incidents": list(REPAIRABLE_OLD_INCIDENT_IDS),
            "touched_incidents": sorted(touched_incident_ids),
            "protected_references": protected,
        }
        if missing or protected or not apply:
            return plan

        for decision in decisions:
            decision.incident_id = None
            decision.auto_file_candidate = False
        session.flush()

        for incident_id in REPAIRABLE_OLD_INCIDENT_IDS:
            extractor._prune_incident_if_unreferenced(session, incident_id)

        original_llm_mode = extractor.LLM_MODE
        extractor.LLM_MODE = "off"
        try:
            for raw in sorted(raws, key=lambda row: (int(row.ts_epoch or 0), row.message_id)):
                extractor.classify_and_upsert_incident(session, raw, allow_filing_job=False)
        finally:
            extractor.LLM_MODE = original_llm_mode

        session.flush()
        after_rows = session.query(MessageDecision).filter(MessageDecision.message_id.in_(TARGET_MESSAGE_IDS)).all()
        after = {
            row.message_id: {
                "category": row.category,
                "event_type": row.event_type,
                "incident_id": row.incident_id,
                "is_issue": bool(row.is_issue),
            }
            for row in after_rows
        }

        errors: list[str] = []
        for message_id, expected in EXPECTED_CATEGORY_EVENT.items():
            actual = after.get(message_id) or {}
            if (actual.get("category"), actual.get("event_type")) != expected or not actual.get("is_issue"):
                errors.append(
                    f"{message_id}: expected issue {expected[0]}/{expected[1]}, got "
                    f"{actual.get('is_issue')} {actual.get('category')}/{actual.get('event_type')}"
                )

        fire_ids = {
            after[message_id]["incident_id"]
            for message_id in TARGET_MESSAGE_IDS[1:5] + (TARGET_MESSAGE_IDS[6],)
            if message_id in after
        }
        laundry_ids = {
            after[message_id]["incident_id"]
            for message_id in (TARGET_MESSAGE_IDS[0], TARGET_MESSAGE_IDS[5])
            if message_id in after
        }
        slow_ids = {
            after[message_id]["incident_id"]
            for message_id in TARGET_MESSAGE_IDS[7:]
            if message_id in after
        }
        if len(fire_ids) != 1:
            errors.append(f"fire-safety updates did not consolidate: {sorted(fire_ids)}")
        if len(laundry_ids) != 1:
            errors.append(f"laundry updates did not consolidate: {sorted(laundry_ids)}")
        if slow_ids != {NORTH_SLOW_INCIDENT_ID}:
            errors.append(f"north slow updates did not attach to {NORTH_SLOW_INCIDENT_ID}: {sorted(slow_ids)}")

        fire_incident = session.get(Incident, next(iter(fire_ids))) if len(fire_ids) == 1 else None
        laundry_incident = session.get(Incident, next(iter(laundry_ids))) if len(laundry_ids) == 1 else None
        if fire_incident is None or fire_incident.status != "open":
            errors.append("consolidated fire-safety incident is not open after the reported recurrence")
        if laundry_incident is None or laundry_incident.status != "closed":
            errors.append("consolidated laundry incident is not closed after the repair update")
        remaining_old = [incident_id for incident_id in REPAIRABLE_OLD_INCIDENT_IDS if session.get(Incident, incident_id)]
        if remaining_old:
            errors.append(f"obsolete incidents remain: {remaining_old}")
        if errors:
            session.rollback()
            return {**plan, "applied": False, "validation_errors": errors, "after": after}

        session.commit()

    append_audit_event(
        "RECENT_WHATSAPP_SEMANTIC_REPAIR",
        None,
        {
            "message_ids": list(TARGET_MESSAGE_IDS),
            "old_incident_ids": list(REPAIRABLE_OLD_INCIDENT_IDS),
            "before": before,
            "after": after,
        },
    )
    daily_hash_chain()
    result: dict[str, object] = {**plan, "applied": True, "validation_errors": [], "after": after}
    if reconcile:
        result["cross_source_reconciliation"] = run_reconciliation()
    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Repair the audited Sep 2-3 laundry, fire-hose, and slow-elevator classifications."
    )
    parser.add_argument("--apply", action="store_true", help="Apply the repair; default is a read-only plan.")
    parser.add_argument("--no-reconcile", action="store_true", help="Skip exact live/export alias reconciliation.")
    args = parser.parse_args()
    result = repair(apply=args.apply, reconcile=not args.no_reconcile)
    print(json.dumps(result, indent=2, sort_keys=True))
    if result.get("missing_messages") or result.get("protected_references") or result.get("validation_errors"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
