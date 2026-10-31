"""Bounded filing of reviewed, current elevator conditions after a cloud audit.

The recovery runner owns the upload/audit/opt-in gate. This module owns the
per-incident evidence scope and checks it again immediately before final submit.
It never replays the archive through the automatic filing path.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Callable
from datetime import datetime, timezone

from sqlalchemy import func, or_, select

from packages.db import FilingJob, Incident, MessageDecision, RawMessage, ServiceRequestCase, get_session
from packages.incident.rules import classify_rules
from packages.nyc311.planner import (
    ACTIONABLE_ELEVATOR_EVENTS,
    REFRESHABLE_JOB_STATES,
    ensure_filing_job_for_incident,
    incident_is_auto_eligible,
)
from packages.nyc311.portal_worker import run_portal_filing_once
from packages.whatsapp.parser import is_media_placeholder_text

MAX_EVIDENCE_AGE_SECONDS = 24 * 60 * 60
ASSETS = frozenset({"elevator_north", "elevator_south", "elevator_both"})
SAFE_SOURCES = frozenset({"rules", "hybrid", "hybrid_rule_state", "llm", "llm_rule_state", "review", "review_rule_state"})


def _json_object(value: str | None) -> dict:
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _confidence(value: object) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _current_epoch(value: object, now: float) -> bool:
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and value > 0
        and 0 <= now - value <= MAX_EVIDENCE_AGE_SECONDS
    )


def _actionable_choice(choice: dict, asset: str) -> bool:
    # Missing booleans and unrecognized provenance are not affirmative proof.
    return (
        choice.get("is_issue") is True
        and choice.get("signal_type") == "report"
        and choice.get("category") == "elevator"
        and choice.get("asset") == asset
        and choice.get("event_type") in ACTIONABLE_ELEVATOR_EVENTS
        and choice.get("close_incident") is False
        and choice.get("needs_review") is False
        and _confidence(choice.get("confidence")) >= 80
        and not any(choice.get(key) for key in ("review_error", "error", "nonreport_reason"))
    )


def _review_completed(decision: MessageDecision) -> bool:
    reviewed = _json_object(decision.final_json) if decision.chosen_source in {"review", "review_rule_state"} else _json_object(decision.llm_json)
    return reviewed.get("review_status") == "completed" and not reviewed.get("review_error")


def _target_chats() -> set[str]:
    value = next((os.environ[key] for key in (
        "PUBLIC_UPDATES_CHAT_NAMES", "TENANT_PUBLIC_CHAT_NAMES", "WHATSAPP_CAPTURE_CHAT_NAMES",
    ) if os.environ.get(key)), "")
    return {" ".join(name.split()).casefold() for name in re.split(r"[,;\n]", value) if name.strip()}


def reviewed_condition_snapshot(
    session, incident: Incident, *, now: float | None = None, lock_evidence: bool = False,
) -> str | None:
    """Return a hash bound to current reviewed source evidence, or fail closed.

    This does not invoke a model or change records. Locks protect the evidence
    while the final-submit transaction validates and commits its durable fence.
    """
    now = datetime.now(timezone.utc).timestamp() if now is None else now
    if (
        incident.category != "elevator"
        or incident.status != "open"
        or incident.asset not in ASSETS
        or incident.needs_review is not False
        or incident.end_ts is not None
        or incident.end_ts_epoch is not None
        or not _current_epoch(incident.last_ts_epoch, now)
        or not incident_is_auto_eligible(incident)
    ):
        return None
    refs = {ref.strip() for ref in (incident.proof_refs or "").split(",") if ref.strip()}
    if not refs:
        return None
    query = (select(RawMessage, MessageDecision)
        .join(MessageDecision, MessageDecision.message_id == RawMessage.message_id)
        .where(or_(RawMessage.message_id.in_(refs), MessageDecision.incident_id == incident.incident_id))
        .order_by(RawMessage.ts_epoch.desc(), RawMessage.message_id)
        .execution_options(populate_existing=True)
    )
    if lock_evidence:
        query = query.with_for_update(of=(RawMessage, MessageDecision))
    rows = session.execute(query).all()
    if not rows or not refs.issubset({raw.message_id for raw, _decision in rows}):
        return None
    raw, decision = rows[0]
    final = _json_object(decision.final_json)
    model = final if decision.chosen_source in {"review", "review_rule_state"} else _json_object(decision.llm_json)
    rules = classify_rules(raw.text or "")
    if (
        raw.ts_epoch != incident.last_ts_epoch
        or not _current_epoch(raw.ts_epoch, now)
        or not raw.text
        or " ".join((raw.chat_name or "").split()).casefold() not in _target_chats()
        or decision.incident_id != incident.incident_id
        or decision.chosen_source not in SAFE_SOURCES
        or decision.is_issue is not True
        or decision.category != "elevator"
        or decision.event_type not in ACTIONABLE_ELEVATOR_EVENTS
        or decision.needs_review is not False
        or _confidence(decision.confidence) < 80
        or not _actionable_choice(final, incident.asset)
        or model.get("review_status") != "completed"
        or not _actionable_choice(model, incident.asset)
        or rules.get("is_issue") is not True
        or rules.get("category") != "elevator"
        or rules.get("asset") != incident.asset
        or rules.get("event_type") not in ACTIONABLE_ELEVATOR_EVENTS
        or rules.get("nonreport_reason")
    ):
        return None
    # New messages awaiting decisions may include a restoration. A later
    # contradictory same-car decision also blocks a stale open incident.
    later_query = (
        select(RawMessage)
        .where(
            or_(RawMessage.chat_name == raw.chat_name, func.lower(func.trim(RawMessage.chat_name)).in_(_target_chats())),
            or_(RawMessage.ts_epoch >= raw.ts_epoch, RawMessage.ts_epoch.is_(None)),
        )
        .order_by(RawMessage.ts_epoch, RawMessage.message_id)
        .execution_options(populate_existing=True)
    )
    if lock_evidence:
        later_query = later_query.with_for_update()
    later = session.scalars(later_query).all()
    decisions_query = select(MessageDecision).where(MessageDecision.message_id.in_([row.message_id for row in later]))
    if lock_evidence:
        decisions_query = decisions_query.with_for_update()
    later_decisions = {row.message_id: row for row in session.scalars(decisions_query.execution_options(populate_existing=True))}
    later_evidence = []
    for newer_raw in later:
        if newer_raw.message_id == raw.message_id:
            continue
        if newer_raw.sender == "SYSTEM" or is_media_placeholder_text(newer_raw.text):
            continue
        newer_decision = later_decisions.get(newer_raw.message_id)
        if (
            not _current_epoch(newer_raw.ts_epoch, now)
            or newer_decision is None
            or not _review_completed(newer_decision)
            or newer_decision.needs_review is not False
        ):
            return None
        newer_final = _json_object(newer_decision.final_json)
        overlaps = newer_final.get("asset") in {incident.asset, "elevator_both"} or (
            incident.asset == "elevator_both" and newer_final.get("asset") in ASSETS
        )
        if newer_decision.category == "elevator" and overlaps:
            if (
                newer_decision.needs_review is not False
                or newer_decision.event_type in {"restore", "status_update"}
                or newer_final.get("close_incident") is not False
            ):
                return None
        later_evidence.append((newer_raw.message_id, newer_raw.text, newer_decision.final_json, newer_decision.llm_json))
    evidence = {
        "incident": {name: getattr(incident, name) for name in (
            "incident_id", "category", "asset", "status", "needs_review", "start_ts", "start_ts_epoch",
            "last_ts_epoch", "end_ts", "end_ts_epoch", "title", "summary", "proof_refs", "confidence",
            "severity", "report_count", "witness_count",
        )},
        "sources": [
            (item.message_id, item.ts_epoch, item.text, item.chat_name, choice.incident_id,
             choice.chosen_source, choice.is_issue, choice.category, choice.event_type,
             choice.needs_review, choice.confidence, choice.rules_json, choice.llm_json, choice.final_json)
            for item, choice in rows
        ],
        "later": later_evidence,
    }
    return hashlib.sha256(json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def run_current_filings(*, max_jobs: int = 2, before_submit: Callable[[], bool] | None = None) -> dict[str, object]:
    """Prepare and file at most two scoped jobs; return no tenant/job payloads."""
    summary = {"ok": True, "submitted": 0, "lookup_verified": 0, "empty": 0, "blocked": 0}
    previous = os.environ.get("AUTO_FILE_ENABLED")
    os.environ["AUTO_FILE_ENABLED"] = "1"
    blocked_ids: set[str] = set()
    snapshots: dict[str, str] = {}
    try:
        cutoff = int(datetime.now(timezone.utc).timestamp()) - MAX_EVIDENCE_AGE_SECONDS
        with get_session() as session:
            incidents = session.scalars(
                select(Incident).where(
                    Incident.category == "elevator", Incident.status == "open",
                    or_(Incident.last_ts_epoch >= cutoff, Incident.last_ts_epoch.is_(None)),
                ).order_by(Incident.last_ts_epoch.desc().nullslast())
            ).all()
            for incident in incidents:
                if session.scalar(select(ServiceRequestCase.id).where(ServiceRequestCase.incident_id == incident.incident_id)) is not None:
                    continue
                snapshot = reviewed_condition_snapshot(session, incident)
                if snapshot is None:
                    blocked_ids.add(incident.incident_id)
                    continue
                # Do not reconcile or retire unrelated incidents/jobs here.
                job = ensure_filing_job_for_incident(session, incident)
                if job is None or job.state == "skipped":
                    continue
                if job.state not in REFRESHABLE_JOB_STATES:
                    blocked_ids.add(incident.incident_id)
                    continue
                try:
                    attempt_limit = max(1, int(os.environ.get("AUTO_FILE_MAX_PORTAL_ATTEMPTS", "3")))
                except ValueError:
                    attempt_limit = 3
                if int(job.attempts or 0) >= attempt_limit:
                    blocked_ids.add(incident.incident_id)
                    continue
                snapshots[incident.incident_id] = snapshot
            session.commit()

        def current(session, incident: Incident) -> bool:
            expected = snapshots.get(incident.incident_id)
            valid = expected is not None and reviewed_condition_snapshot(session, incident, lock_evidence=True) == expected
            if not valid:
                blocked_ids.add(incident.incident_id)
            return valid

        if not snapshots:
            summary["empty"] = 1
        else:
            for _ in range(max(1, min(2, max_jobs))):
                result = run_portal_filing_once(
                    incident_ids=frozenset(snapshots), incident_validator=current,
                    before_submit=before_submit, headless=True, verify_lookup=True,
                )
                if result.get("ok") is True and result.get("service_request_number"):
                    summary["submitted"] += 1
                    summary["lookup_verified"] += int(result.get("lookup_verified") is True)
                elif result.get("ok") is True and result.get("job") is None:
                    summary["empty"] += 1
                    break
                else:
                    # Never consume another attempt for a failed/requeued or
                    # uncertain submission during this same recovery cycle.
                    job = result.get("job")
                    if isinstance(job, dict) and job.get("incident_id"):
                        blocked_ids.add(str(job["incident_id"]))
                    else:
                        summary["blocked"] += 1
                    summary["ok"] = False
                    break
    except Exception:
        # Exceptions can contain private source/portal details. The persistent
        # worker fence/receipt remains the authority for any external outcome.
        summary["blocked"] += 1
        summary["ok"] = False
    finally:
        if previous is None:
            os.environ.pop("AUTO_FILE_ENABLED", None)
        else:
            os.environ["AUTO_FILE_ENABLED"] = previous
    summary["blocked"] += len(blocked_ids)
    summary["ok"] = summary["ok"] and not summary["blocked"]
    return summary
