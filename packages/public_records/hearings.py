"""Join current OATH decisions to DOB summonses without implying repair status."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterable

from packages.public_records.source_queries import normalize_ticket_reference
from packages.timeutil import normalize_timestamp, parse_ts_to_epoch


def _raw(record: Any) -> dict[str, Any]:
    try:
        payload = json.loads(record.raw_json or "{}")
    except (TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _date(value: Any) -> str:
    return (normalize_timestamp(value) or "")[:10]


def current_oath_hearing_outcome(
    summons: Any, records: Iterable[Any], *, now: datetime | None = None
) -> dict[str, Any] | None:
    """A previous hearing's decision cannot settle a newer pending hearing.

    Only present, verified OATH observations can satisfy the tenant request.
    Select the latest hearing/snapshot before inspecting its result so an older
    completed hearing cannot win over a newer pending or adjourned proceeding.
    """
    if summons.record_type != "dob_ecb_violation":
        return None
    if summons.needs_human_verification or summons.machine_verification_status == "official_conflict":
        return None
    raw_summons = _raw(summons)
    ticket = normalize_ticket_reference(raw_summons.get("ecb_violation_number") or summons.record_key)
    if not ticket:
        return None
    candidates = []
    for record in records:
        if (
            record.source_system != "oath_hearings"
            or record.source_presence_status != "present"
            or not record.visible_public
            or record.needs_human_verification
            or record.machine_verification_status == "official_conflict"
            or not (record.machine_verified_at or record.human_verified_at)
        ):
            continue
        raw = _raw(record)
        agency = str(raw.get("issuing_agency") or "").upper().replace(".", "").strip()
        if agency not in {"DEPT OF BUILDINGS", "DEPARTMENT OF BUILDINGS"}:
            continue
        if normalize_ticket_reference(raw.get("ticket_number") or record.record_key) != ticket:
            continue
        candidates.append((record, raw))
    if not candidates:
        return None
    latest, raw = max(candidates, key=lambda candidate: (
        _date(candidate[1].get("hearing_date")) or _date(candidate[1].get("decision_date")),
        parse_ts_to_epoch(candidate[0].source_checked_at) or 0,
        parse_ts_to_epoch(candidate[0].last_seen_at) or 0,
        _date(candidate[1].get("decision_date")),
    ))
    result = str(raw.get("hearing_result") or "").strip()
    decision_date = _date(raw.get("decision_date"))
    hearing_date = _date(raw.get("hearing_date"))
    minimum_date = max(_date(raw_summons.get("hearing_date")), hearing_date, _date(summons.filed_at))
    today = (now or datetime.now(timezone.utc)).date().isoformat()
    if (
        not result or result.casefold() in {"unknown", "n/a", "none"}
        or any(term in result.casefold() for term in ("pending", "scheduled", "not heard", "adjourned"))
        or not decision_date or decision_date < minimum_date or decision_date > today
    ):
        return None
    return {
        "ticket_number": ticket,
        "hearing_result": result,
        "decision_date": decision_date,
        "hearing_date": hearing_date,
        "source_record_id": latest.id,
        "source_url": latest.source_url,
    }
