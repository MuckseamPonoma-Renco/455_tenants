#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.local_env import load_local_env_file

load_local_env_file(ROOT / ".env")

from packages.audit import append_audit_event, daily_hash_chain  # noqa: E402
from packages.db import (  # noqa: E402
    FilingJob,
    Incident,
    MessageDecision,
    RawMessage,
    ServiceRequestCase,
    WatchdogAction,
    get_session,
)
from packages.incident.rules import classify_rules  # noqa: E402


REPAIR_ID = "2026-09-05-live-washer-15-v1"
INCIDENT_ID = "13dff0ba3dd4e54c41ed35477c988e11"
ASSET = "washer_15"
MESSAGE_SPECS = {
    "116e13ef0a5356a49cb9be091cf030a78d07523302a6946dd80cb8b5efee897a": {
        "text_sha256": "a73e9c88c15aa069ec2dd729ffb1d8b99640080cad345f5dfed7fbdc3bd0677e",
        "ts_epoch": 1788629460,
        "event_type": "new_issue",
        "title": "Washer #15 detergent dispenser problem",
        "summary": "Washer #15 failed to dispense detergent; the tenant said they could contact Hercules.",
    },
    "26d0635ef3918e7e92233ce85d088f5600546e94fa9acc9fb2d790d1618786be": {
        "text_sha256": "31d33d6b0c28043b8ad69248e396d15eb59b63f68bd32e05fbfd6c7157294d91",
        "ts_epoch": 1788636600,
        "event_type": "still_out",
        "title": "Washer #15 detergent problem recurred after repair",
        "summary": (
            "The tenant confirmed that Washer #15 had the same detergent problem again after "
            "it was reported fixed on Thursday."
        ),
    },
}
DESIRED_TITLE = "Washer #15 detergent dispenser problem"
DESIRED_SUMMARY = (
    "Washer #15 was reported failing to dispense detergent; a follow-up said the problem "
    "recurred after it was reported fixed on Thursday."
)


def _sha256(text: str | None) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()


def _safe_json(value: str | None) -> dict[str, object]:
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _decision_snapshot(row: MessageDecision) -> dict[str, object]:
    final = _safe_json(row.final_json)
    return {
        "incident_id": row.incident_id,
        "chosen_source": row.chosen_source,
        "is_issue": bool(row.is_issue),
        "category": row.category,
        "asset": final.get("asset"),
        "event_type": row.event_type,
        "confidence": int(row.confidence or 0),
        "needs_review": bool(row.needs_review),
        "auto_file_candidate": bool(row.auto_file_candidate),
        "review_status": final.get("review_status"),
        "review_outcome": final.get("review_outcome"),
    }


def _is_desired(message_id: str, row: MessageDecision) -> bool:
    expected = MESSAGE_SPECS[message_id]
    final = _safe_json(row.final_json)
    return bool(
        row.incident_id == INCIDENT_ID
        and row.chosen_source == f"review:{REPAIR_ID}"
        and row.is_issue
        and row.category == "laundry"
        and row.event_type == expected["event_type"]
        and int(row.confidence or 0) >= 90
        and not row.needs_review
        and not row.auto_file_candidate
        and final.get("asset") == ASSET
        and final.get("title") == expected["title"]
        and final.get("summary") == expected["summary"]
        and final.get("review_status") == "completed"
        and final.get("review_outcome") == "corrected"
    )


def repair(*, apply: bool) -> dict[str, object]:
    message_ids = tuple(MESSAGE_SPECS)
    with get_session() as session:
        raws = {
            row.message_id: row
            for row in (
                session.query(RawMessage)
                .filter(RawMessage.message_id.in_(message_ids))
                .with_for_update()
                .all()
            )
        }
        decisions = {
            row.message_id: row
            for row in (
                session.query(MessageDecision)
                .filter(MessageDecision.message_id.in_(message_ids))
                .with_for_update()
                .all()
            )
        }
        incident = (
            session.query(Incident)
            .filter(Incident.incident_id == INCIDENT_ID)
            .with_for_update()
            .one_or_none()
        )
        errors: list[str] = []
        for message_id, spec in MESSAGE_SPECS.items():
            raw = raws.get(message_id)
            decision = decisions.get(message_id)
            if raw is None:
                errors.append(f"missing raw message: {message_id}")
                continue
            if _sha256(raw.text) != spec["text_sha256"] or int(raw.ts_epoch or 0) != spec["ts_epoch"]:
                errors.append(f"raw message hash/timestamp mismatch: {message_id}")
            if raw.source != "whatsapp_web":
                errors.append(f"unexpected source for {message_id}: {raw.source}")
            if decision is None:
                errors.append(f"missing decision: {message_id}")
                continue
            if not _is_desired(message_id, decision) and not (
                decision.incident_id == INCIDENT_ID
                and decision.is_issue
                and decision.category == "laundry"
                and decision.event_type == spec["event_type"]
                and not decision.needs_review
                and not decision.auto_file_candidate
                and _safe_json(decision.final_json).get("asset") in {None, ASSET}
            ):
                errors.append(f"unexpected decision state: {message_id}")

        if incident is None:
            errors.append(f"missing incident: {INCIDENT_ID}")
        elif not (
            incident.category == "laundry"
            and incident.status == "open"
            and incident.asset in {None, ASSET}
            and set(message_ids).issubset(set((incident.proof_refs or "").split(",")))
        ):
            errors.append(f"unexpected incident state: {INCIDENT_ID}")

        protected = {
            "filing_jobs": session.query(FilingJob).filter(FilingJob.incident_id == INCIDENT_ID).count(),
            "service_requests": session.query(ServiceRequestCase).filter(ServiceRequestCase.incident_id == INCIDENT_ID).count(),
            "watchdog_actions": session.query(WatchdogAction).filter(WatchdogAction.related_incident_id == INCIDENT_ID).count(),
        }
        if any(protected.values()):
            errors.append(f"protected downstream references exist: {protected}")

        before = {message_id: _decision_snapshot(row) for message_id, row in decisions.items()}
        already_repaired = bool(
            not errors
            and incident is not None
            and incident.asset == ASSET
            and incident.title == DESIRED_TITLE
            and incident.summary == DESIRED_SUMMARY
            and all(_is_desired(message_id, decisions[message_id]) for message_id in message_ids)
        )
        result: dict[str, object] = {
            "repair_id": REPAIR_ID,
            "apply": apply,
            "target_message_count": len(message_ids),
            "incident_id": INCIDENT_ID,
            "protected_references": protected,
            "already_repaired": already_repaired,
            "would_change": not already_repaired and not errors,
            "errors": errors,
            "before": before,
        }
        if errors or not apply or already_repaired:
            session.rollback()
            result["applied"] = False
            return result

        reviewed_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        for message_id, decision in decisions.items():
            spec = MESSAGE_SPECS[message_id]
            raw = raws[message_id]
            final = _safe_json(decision.final_json)
            final.update(
                {
                    "is_issue": True,
                    "signal_type": "report",
                    "category": "laundry",
                    "asset": ASSET,
                    "event_type": spec["event_type"],
                    "title": spec["title"],
                    "summary": spec["summary"],
                    "confidence": max(90, int(decision.confidence or 0)),
                    "needs_review": False,
                    "review_status": "completed",
                    "review_outcome": "corrected",
                    "reviewed_by": REPAIR_ID,
                    "reviewed_at": reviewed_at,
                }
            )
            decision.incident_id = INCIDENT_ID
            decision.chosen_source = f"review:{REPAIR_ID}"
            decision.is_issue = True
            decision.category = "laundry"
            decision.event_type = str(spec["event_type"])
            decision.confidence = max(90, int(decision.confidence or 0))
            decision.needs_review = False
            decision.auto_file_candidate = False
            if message_id == next(iter(MESSAGE_SPECS)):
                decision.rules_json = json.dumps(classify_rules(raw.text or ""), ensure_ascii=False)
            decision.final_json = json.dumps(final, ensure_ascii=False)

        assert incident is not None
        incident.asset = ASSET
        incident.title = DESIRED_TITLE
        incident.summary = DESIRED_SUMMARY
        incident.confidence = max(90, int(incident.confidence or 0))
        incident.needs_review = False
        incident.updated_at = reviewed_at
        session.flush()

        after = {message_id: _decision_snapshot(decisions[message_id]) for message_id in message_ids}
        validation_errors = [
            f"postcondition failed: {message_id}"
            for message_id in message_ids
            if not _is_desired(message_id, decisions[message_id])
        ]
        if incident.asset != ASSET or incident.title != DESIRED_TITLE or incident.summary != DESIRED_SUMMARY:
            validation_errors.append("incident postcondition failed")
        after_protected = {
            "filing_jobs": session.query(FilingJob).filter(FilingJob.incident_id == INCIDENT_ID).count(),
            "service_requests": session.query(ServiceRequestCase).filter(ServiceRequestCase.incident_id == INCIDENT_ID).count(),
            "watchdog_actions": session.query(WatchdogAction).filter(WatchdogAction.related_incident_id == INCIDENT_ID).count(),
        }
        if after_protected != protected:
            validation_errors.append("downstream reference counts changed")
        if validation_errors:
            session.rollback()
            return {**result, "applied": False, "validation_errors": validation_errors, "after": after}
        session.commit()

    append_audit_event(
        "LIVE_WHATSAPP_SEMANTIC_REPAIR",
        None,
        {"repair_id": REPAIR_ID, "message_ids": list(message_ids), "before": before, "after": after},
    )
    daily_hash_chain()
    return {
        **result,
        "applied": True,
        "would_change": False,
        "validation_errors": [],
        "after": after,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Repair the exact Sep 5 Washer #15 live-capture semantics.")
    parser.add_argument("--apply", action="store_true", help="Apply one fail-closed transaction; default is read-only.")
    args = parser.parse_args()
    result = repair(apply=args.apply)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 1 if result.get("errors") or result.get("validation_errors") else 0


if __name__ == "__main__":
    raise SystemExit(main())
