import datetime
import json
import os
from sqlalchemy import func
from packages.audit import compute_message_id
from packages.db import Incident, IncidentWitness, MessageDecision, RawMessage
from packages.incident.rules import classify_rules
from packages.llm.classifier import llm_classify_message
from packages.llm.triage import should_call_llm
from packages.nyc311.planner import ensure_filing_job_for_incident, incident_is_auto_eligible
from packages.nyc311.tracker import attach_manual_cases_from_text

OTHER_WINDOW_SECONDS = int(os.environ.get("OTHER_WINDOW_SECONDS", "21600"))
ELEVATOR_SILENCE_GAP_SECONDS = int(os.environ.get("ELEVATOR_SILENCE_GAP_SECONDS", "7200"))
LLM_MODE = os.environ.get("LLM_MODE", "uncertain").lower().strip()


def _now_iso():
    return datetime.datetime.now(datetime.UTC).isoformat().replace('+00:00', 'Z')


def _inc_id(cat: str, asset: str | None, ts_iso: str | None, title: str) -> str:
    return compute_message_id(cat, asset or "", ts_iso or "", title)[:32]


def _open_incidents_context(session) -> list[dict]:
    rows = session.query(Incident).filter(Incident.status != "closed").order_by(Incident.last_ts_epoch.desc().nullslast()).limit(8).all()
    return [
        {
            "incident_id": row.incident_id,
            "category": row.category,
            "asset": row.asset,
            "status": row.status,
            "severity": row.severity,
            "start_ts": row.start_ts,
            "summary": (row.summary or "")[:180],
        }
        for row in rows
    ]


def _recent_related_context(session, rm: RawMessage) -> list[dict]:
    txt = (rm.text or "").lower()
    key_tokens = ["elevator", "lift", "heat", "hot water", "cold", "leak", "roach", "mice", "rat", "door", "lock", "intercom", "security", "mold", "boiler"]
    hits = [token for token in key_tokens if token in txt]
    if not hits:
        return []
    query = session.query(RawMessage).filter(RawMessage.ts_epoch.isnot(None)).order_by(RawMessage.ts_epoch.desc()).limit(40).all()
    out = []
    for msg in query:
        lower = (msg.text or "").lower()
        if any(token in lower for token in hits):
            out.append({"ts": msg.ts_iso, "sender": msg.sender, "text": (msg.text or "")[:140]})
        if len(out) >= 6:
            break
    return out


def _upsert_witness(session, incident_id: str, sender_hash: str):
    if not sender_hash:
        return
    with session.no_autoflush:
        exists = any(
            isinstance(row, IncidentWitness) and row.incident_id == incident_id and row.sender_hash == sender_hash
            for row in session.new
        )
        if not exists:
            exists = session.query(IncidentWitness).filter(
                IncidentWitness.incident_id == incident_id,
                IncidentWitness.sender_hash == sender_hash,
            ).first()
    if not exists:
        session.add(IncidentWitness(incident_id=incident_id, sender_hash=sender_hash))


def _recompute_witness_count(session, incident_id: str) -> int:
    count = session.query(func.count(IncidentWitness.id)).filter(IncidentWitness.incident_id == incident_id).scalar() or 0
    incident = session.get(Incident, incident_id)
    if incident:
        incident.witness_count = int(count)
    return int(count)


def _attach_proof(inc: Incident, message_id: str):
    refs = [ref for ref in (inc.proof_refs or "").split(",") if ref.strip()]
    if message_id not in refs:
        refs.append(message_id)
    inc.proof_refs = ",".join(refs[:3])


def _find_incident_by_id(session, incident_id: str) -> Incident | None:
    with session.no_autoflush:
        incident = session.get(Incident, incident_id)
        if incident:
            return incident
        for row in session.new:
            if isinstance(row, Incident) and row.incident_id == incident_id:
                return row
    return None


def _create_incident(session, cat: str, asset: str | None, rm: RawMessage, title: str, summary: str, severity: int, status: str, confidence: int, needs_review: bool) -> Incident:
    incident_id = _inc_id(cat, asset, rm.ts_iso, title)
    existing = _find_incident_by_id(session, incident_id)
    if existing:
        _attach_proof(existing, rm.message_id)
        _upsert_witness(session, incident_id, rm.sender_hash)
        _recompute_witness_count(session, incident_id)
        existing.updated_at = _now_iso()
        existing.confidence = max(int(existing.confidence or 0), confidence)
        existing.needs_review = bool(existing.needs_review or needs_review)
        if status == "closed":
            existing.status = "closed"
            existing.end_ts = existing.end_ts or rm.ts_iso
            existing.end_ts_epoch = existing.end_ts_epoch or rm.ts_epoch
        return existing

    inc = Incident(
        incident_id=incident_id,
        category=cat,
        asset=asset,
        severity=severity,
        status=status,
        start_ts=rm.ts_iso,
        start_ts_epoch=rm.ts_epoch,
        end_ts=None,
        end_ts_epoch=None,
        last_ts_epoch=rm.ts_epoch,
        title=title[:240],
        summary=summary[:2000],
        proof_refs=rm.message_id,
        report_count=1,
        witness_count=0,
        confidence=confidence,
        needs_review=needs_review,
        updated_at=_now_iso(),
    )
    session.add(inc)
    _upsert_witness(session, incident_id, rm.sender_hash)
    _recompute_witness_count(session, incident_id)
    return inc


def _update_incident(session, inc: Incident, rm: RawMessage, summary: str, severity: int, confidence: int, needs_review: bool):
    _attach_proof(inc, rm.message_id)
    if rm.ts_epoch is not None:
        if inc.start_ts_epoch is None or int(rm.ts_epoch) < int(inc.start_ts_epoch):
            inc.start_ts_epoch = rm.ts_epoch
            inc.start_ts = rm.ts_iso or inc.start_ts
        if inc.last_ts_epoch is None or int(rm.ts_epoch) > int(inc.last_ts_epoch):
            inc.last_ts_epoch = rm.ts_epoch
    inc.updated_at = _now_iso()
    inc.needs_review = inc.needs_review or needs_review
    inc.severity = max(int(inc.severity or 2), severity)
    inc.report_count = int(inc.report_count or 0) + 1
    inc.confidence = max(int(inc.confidence or 0), confidence)
    if summary and summary not in (inc.summary or ""):
        inc.summary = (inc.summary + " | " + summary)[:2000]
    _upsert_witness(session, inc.incident_id, rm.sender_hash)
    _recompute_witness_count(session, inc.incident_id)


def _rule_choice(rules: dict | None) -> dict | None:
    rules = rules or {}
    if not rules.get("is_issue"):
        return None
    return {
        "is_issue": True,
        "signal_type": "report",
        "category": rules.get("category"),
        "asset": rules.get("asset"),
        "event_type": "restore" if rules.get("kind") == "restore" else "outage" if rules.get("kind") == "outage" else "new_issue",
        "severity": int(rules.get("severity", 2)),
        "confidence": 85 if rules.get("kind") in {"outage", "restore"} else 75,
        "title": rules.get("title") or "Issue",
        "summary": rules.get("summary") or "",
        "close_incident": rules.get("kind") == "restore",
        "needs_review": False,
    }


def _normalized_llm_choice(llm: dict | None) -> dict | None:
    if not isinstance(llm, dict):
        return None
    out = dict(llm)
    out.setdefault("is_issue", False)
    out.setdefault("signal_type", "discussion")
    out.setdefault("category", "other")
    out.setdefault("asset", None)
    out.setdefault("event_type", "non_issue")
    out.setdefault("severity", 2)
    out.setdefault("confidence", 50)
    out.setdefault("title", "")
    out.setdefault("summary", "")
    out.setdefault("close_incident", False)
    out.setdefault("needs_review", False)
    return out


def _should_use_llm(text: str, rules: dict) -> bool:
    mode = (LLM_MODE or "uncertain").lower().strip()
    if mode in {"", "off", "false", "0"}:
        return False
    if mode in {"all", "supervised"}:
        return bool((text or "").strip())
    if mode == "assist":
        return bool(rules.get("is_issue")) or should_call_llm(text or "", rules.get("is_issue", False), rules.get("kind", "nonissue"))
    return should_call_llm(text or "", rules.get("is_issue", False), rules.get("kind", "nonissue"))


def _merge_choices(rule_choice: dict | None, llm_choice: dict | None) -> tuple[dict | None, str]:
    if rule_choice and llm_choice and llm_choice.get("is_issue"):
        if rule_choice.get("category") == llm_choice.get("category"):
            merged = dict(rule_choice)
            merged["asset"] = llm_choice.get("asset") or rule_choice.get("asset")
            merged["event_type"] = llm_choice.get("event_type") or rule_choice.get("event_type")
            merged["severity"] = max(int(rule_choice.get("severity", 2)), int(llm_choice.get("severity", 2)))
            merged["confidence"] = max(int(rule_choice.get("confidence", 0)), int(llm_choice.get("confidence", 0)))
            merged["title"] = llm_choice.get("title") or rule_choice.get("title")
            merged["summary"] = llm_choice.get("summary") or rule_choice.get("summary")
            merged["close_incident"] = bool(rule_choice.get("close_incident") or llm_choice.get("close_incident"))
            merged["needs_review"] = bool(rule_choice.get("needs_review") or llm_choice.get("needs_review"))
            return merged, "hybrid"

        preferred = llm_choice if int(llm_choice.get("confidence", 0)) >= 90 else rule_choice
        preferred = dict(preferred)
        preferred["needs_review"] = True
        return preferred, "hybrid_disagreement"

    if rule_choice and llm_choice and not llm_choice.get("is_issue"):
        chosen = dict(rule_choice)
        chosen["needs_review"] = True
        return chosen, "rules_with_llm_disagreement"

    if llm_choice and llm_choice.get("is_issue"):
        chosen = dict(llm_choice)
        chosen["needs_review"] = bool(chosen.get("needs_review", False) or int(chosen.get("confidence", 0)) < 80)
        return chosen, "llm"

    if rule_choice:
        return dict(rule_choice), "rules"

    return None, "none"


def _pick_decision(session, rm: RawMessage) -> tuple[dict | None, dict, dict | None, str]:
    rules = classify_rules(rm.text)
    llm = None
    if _should_use_llm(rm.text or "", rules):
        llm = llm_classify_message(rm.text or "", open_incidents=_open_incidents_context(session), recent_related=_recent_related_context(session, rm))
    llm_choice = _normalized_llm_choice(llm)
    chosen, chosen_source = _merge_choices(_rule_choice(rules), llm_choice)
    return chosen, rules, llm_choice, chosen_source


def _record_decision(session, rm: RawMessage, rules: dict, llm_choice: dict | None, chosen: dict | None, chosen_source: str, incident_id: str | None):
    row = session.get(MessageDecision, rm.message_id) or MessageDecision(message_id=rm.message_id)
    row.incident_id = incident_id
    row.created_at = _now_iso()
    row.chosen_source = chosen_source
    row.is_issue = bool(chosen and chosen.get("is_issue"))
    row.category = chosen.get("category") if chosen else None
    row.event_type = chosen.get("event_type") if chosen else None
    row.confidence = int((chosen or {}).get("confidence", 0) or 0)
    row.needs_review = bool((chosen or {}).get("needs_review", False))
    row.rules_json = json.dumps(rules or {}, ensure_ascii=False)
    row.llm_json = json.dumps(llm_choice or {}, ensure_ascii=False)
    row.final_json = json.dumps(chosen or {}, ensure_ascii=False)
    row.auto_file_candidate = bool(incident_id and session.get(Incident, incident_id) and incident_is_auto_eligible(session.get(Incident, incident_id)))
    session.merge(row)


def classify_and_upsert_incident(session, rm: RawMessage) -> str:
    chosen, rules, llm_choice, chosen_source = _pick_decision(session, rm)
    incident = None

    if chosen and chosen.get("is_issue") and chosen.get("signal_type") == "report":
        cat = chosen.get("category") or "other"
        asset = chosen.get("asset")
        event_type = chosen.get("event_type") or "new_issue"
        severity = int(chosen.get("severity", 2))
        confidence = int(chosen.get("confidence", 70))
        title = (chosen.get("title") or "Issue")[:240]
        summary = (chosen.get("summary") or "")[:2000]
        needs_review = bool(chosen.get("needs_review", False))
        close_incident = bool(chosen.get("close_incident")) or event_type == "restore"

        if cat == "elevator":
            if close_incident:
                query = session.query(Incident).filter(Incident.category == "elevator", Incident.status != "closed")
                if asset and asset != "elevator_both":
                    query = query.filter((Incident.asset == asset) | (Incident.asset == "elevator_both") | (Incident.asset.is_(None)))
                candidate = None
                for row in query.order_by(Incident.last_ts_epoch.desc().nullslast()).all():
                    if rm.ts_epoch is None or row.last_ts_epoch is None or int(row.last_ts_epoch) <= int(rm.ts_epoch):
                        candidate = row
                        break
                if not candidate:
                    incident = _create_incident(session, cat, asset, rm, title, summary, 2, "closed", max(confidence, 60), True)
                    incident.end_ts = rm.ts_iso
                    incident.end_ts_epoch = rm.ts_epoch
                else:
                    _update_incident(session, candidate, rm, summary, 2, confidence, needs_review)
                    candidate.status = "closed"
                    candidate.end_ts = rm.ts_iso
                    candidate.end_ts_epoch = rm.ts_epoch
                    incident = candidate
            else:
                query = session.query(Incident).filter(Incident.category == "elevator", Incident.status != "closed")
                if asset and asset != "elevator_both":
                    query = query.filter((Incident.asset == asset) | (Incident.asset == "elevator_both") | (Incident.asset.is_(None)))
                last_open = None
                for row in query.order_by(Incident.last_ts_epoch.desc().nullslast()).all():
                    if rm.ts_epoch is None or row.last_ts_epoch is None:
                        last_open = row
                        break
                    delta = int(rm.ts_epoch) - int(row.last_ts_epoch)
                    if 0 <= delta <= ELEVATOR_SILENCE_GAP_SECONDS:
                        last_open = row
                        break
                    if delta > ELEVATOR_SILENCE_GAP_SECONDS:
                        break
                if last_open:
                    _update_incident(session, last_open, rm, summary, severity, confidence, needs_review)
                    incident = last_open
                else:
                    incident = _create_incident(session, cat, asset, rm, title, summary, severity, "open", max(confidence, 80), needs_review)
        else:
            best = None
            rows = session.query(Incident).filter(Incident.category == cat, Incident.status != "closed").order_by(Incident.last_ts_epoch.desc().nullslast()).all()
            for candidate in rows:
                if asset and candidate.asset and candidate.asset != asset:
                    continue
                if rm.ts_epoch is None or candidate.last_ts_epoch is None:
                    best = candidate
                    break
                delta = int(rm.ts_epoch) - int(candidate.last_ts_epoch)
                if 0 <= delta <= OTHER_WINDOW_SECONDS:
                    best = candidate
                    break
                if delta > OTHER_WINDOW_SECONDS:
                    break
            if best:
                _update_incident(session, best, rm, summary, severity, confidence, needs_review)
                incident = best
            else:
                incident = _create_incident(session, cat, asset, rm, title, summary, severity, "open", confidence, needs_review)

    attach_manual_cases_from_text(session, text=rm.text or "", incident=incident)
    if incident:
        ensure_filing_job_for_incident(session, incident)
    _record_decision(session, rm, rules, llm_choice, chosen, chosen_source, incident.incident_id if incident else None)
    return incident.incident_id if incident else ""
