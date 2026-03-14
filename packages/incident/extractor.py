import datetime
import os
from sqlalchemy import func
from packages.audit import compute_message_id
from packages.db import Incident, IncidentWitness, RawMessage
from packages.incident.rules import classify_rules
from packages.llm.classifier import llm_classify_message
from packages.llm.triage import should_call_llm
from packages.nyc311.planner import ensure_filing_job_for_incident
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
    exists = session.query(IncidentWitness).filter(IncidentWitness.incident_id == incident_id, IncidentWitness.sender_hash == sender_hash).first()
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


def _create_incident(session, cat: str, asset: str | None, rm: RawMessage, title: str, summary: str, severity: int, status: str, confidence: int, needs_review: bool) -> Incident:
    incident_id = _inc_id(cat, asset, rm.ts_iso, title)
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
    inc.last_ts_epoch = rm.ts_epoch or inc.last_ts_epoch
    inc.updated_at = _now_iso()
    inc.needs_review = inc.needs_review or needs_review
    inc.severity = max(int(inc.severity or 2), severity)
    inc.report_count = int(inc.report_count or 0) + 1
    inc.confidence = max(int(inc.confidence or 0), confidence)
    if summary and summary not in (inc.summary or ""):
        inc.summary = (inc.summary + " | " + summary)[:2000]
    _upsert_witness(session, inc.incident_id, rm.sender_hash)
    _recompute_witness_count(session, inc.incident_id)


def _pick_chosen(session, rm: RawMessage) -> dict | None:
    rules = classify_rules(rm.text)
    use_llm = False
    if LLM_MODE == "all":
        use_llm = True
    elif LLM_MODE == "uncertain":
        use_llm = should_call_llm(rm.text or "", rules.get("is_issue", False), rules.get("kind", "nonissue"))

    llm = None
    if use_llm:
        llm = llm_classify_message(rm.text or "", open_incidents=_open_incidents_context(session), recent_related=_recent_related_context(session, rm))

    needs_review = False
    chosen = None
    if rules.get("is_issue"):
        chosen = {
            "is_issue": True,
            "signal_type": "report",
            "category": rules.get("category"),
            "asset": rules.get("asset"),
            "event_type": "restore" if rules.get("kind") == "restore" else "outage" if rules.get("kind") == "outage" else "new_issue",
            "severity": int(rules.get("severity", 2)),
            "confidence": 85,
            "title": rules.get("title") or "Issue",
            "summary": rules.get("summary") or "",
            "close_incident": rules.get("kind") == "restore",
            "needs_review": False,
        }
        if llm and isinstance(llm, dict) and llm.get("is_issue") is False:
            needs_review = True
    elif llm and isinstance(llm, dict):
        chosen = llm
        needs_review = bool(chosen.get("needs_review", False))

    if not chosen:
        return None
    chosen["needs_review"] = bool(chosen.get("needs_review", False) or needs_review)
    return chosen


def classify_and_upsert_incident(session, rm: RawMessage) -> str:
    chosen = _pick_chosen(session, rm)
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
                candidate = query.order_by(Incident.last_ts_epoch.desc().nullslast()).first()
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
                last_open = query.order_by(Incident.last_ts_epoch.desc().nullslast()).first()
                if last_open and rm.ts_epoch and last_open.last_ts_epoch and (rm.ts_epoch - last_open.last_ts_epoch) <= ELEVATOR_SILENCE_GAP_SECONDS:
                    _update_incident(session, last_open, rm, summary, severity, confidence, needs_review)
                    incident = last_open
                else:
                    incident = _create_incident(session, cat, asset, rm, title, summary, severity, "open", max(confidence, 80), needs_review)
        else:
            best = None
            for candidate in session.query(Incident).filter(Incident.category == cat, Incident.status != "closed").all():
                if asset and candidate.asset and candidate.asset != asset:
                    continue
                if rm.ts_epoch and candidate.last_ts_epoch and abs(rm.ts_epoch - candidate.last_ts_epoch) <= OTHER_WINDOW_SECONDS:
                    best = candidate
                    break
            if best:
                _update_incident(session, best, rm, summary, severity, confidence, needs_review)
                incident = best
            else:
                incident = _create_incident(session, cat, asset, rm, title, summary, severity, "open", confidence, needs_review)

    attach_manual_cases_from_text(session, text=rm.text or "", incident=incident)
    if incident:
        ensure_filing_job_for_incident(session, incident)
        return incident.incident_id
    return ""
