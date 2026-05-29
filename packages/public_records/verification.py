from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Iterable

from sqlalchemy import select

from packages.db import PublicRecordWatch, WatchdogAction
from packages.project_watch.rules import now_iso
from packages.public_records.config import building_address, building_bbl_compact, building_bin, source_configs


AUTO_VERIFIER = "auto:official_open_data"
DEFAULT_MIN_CONFIDENCE = 80


@dataclass(frozen=True)
class VerificationResult:
    status: str
    confidence: int
    summary: str
    corroborating_records: list[dict[str, object]]
    machine_verified: bool


def auto_verify_min_confidence() -> int:
    raw = (os.environ.get("PUBLIC_RECORD_AUTO_VERIFY_MIN_CONFIDENCE") or "").strip()
    if not raw:
        return DEFAULT_MIN_CONFIDENCE
    try:
        return max(0, min(100, int(raw)))
    except ValueError:
        return DEFAULT_MIN_CONFIDENCE


def _normalize_text(value: object | None) -> str:
    text = str(value or "").upper()
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _digits(value: object | None) -> str:
    text = str(value or "").strip()
    if re.fullmatch(r"\d+(\.0+)?", text):
        return text.split(".", 1)[0]
    return re.sub(r"\D+", "", text)


def _raw(record: PublicRecordWatch) -> dict:
    try:
        payload = json.loads(record.raw_json or "{}")
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _bbl_parts() -> tuple[str, str]:
    bbl = _digits(building_bbl_compact())
    if len(bbl) >= 10:
        return str(int(bbl[1:6])), str(int(bbl[6:10]))
    return "5390", "74"


def _address_matches(record: PublicRecordWatch, raw: dict) -> bool:
    expected = _normalize_text(building_address())
    candidates = [
        record.address,
        raw.get("incident_address"),
        raw.get("physical_address"),
        raw.get("street"),
        raw.get("street_name"),
        raw.get("house_street"),
        raw.get("streetname"),
        raw.get("violation_location_street_name"),
    ]
    house_fields = (
        raw.get("house_number")
        or raw.get("house_no")
        or raw.get("housenumber")
        or raw.get("violation_location_house")
    )
    street_fields = (
        raw.get("street_name")
        or raw.get("house_street")
        or raw.get("street")
        or raw.get("streetname")
        or raw.get("violation_location_street_name")
    )
    if house_fields and street_fields:
        candidates.append(f"{house_fields} {street_fields}")
    normalized_candidates = {_normalize_text(candidate) for candidate in candidates if candidate}
    return expected in normalized_candidates


def _block_lot_matches(raw: dict) -> bool:
    expected_block, expected_lot = _bbl_parts()
    block = _digits(raw.get("block") or raw.get("violation_location_block_no"))
    lot = _digits(raw.get("lot") or raw.get("violation_location_lot_no"))
    return bool(block and lot and str(int(block)) == expected_block and str(int(lot)) == expected_lot)


def _explicit_identity_conflict(record: PublicRecordWatch, raw: dict) -> bool:
    bbl = _digits(record.bbl)
    bin_value = _digits(record.bin)
    if bbl and bbl != _digits(building_bbl_compact()):
        return True
    if bin_value and building_bin() and bin_value != _digits(building_bin()):
        return True
    if _block_lot_matches(raw):
        return False
    expected_block, expected_lot = _bbl_parts()
    block = _digits(raw.get("block") or raw.get("violation_location_block_no"))
    lot = _digits(raw.get("lot") or raw.get("violation_location_lot_no"))
    return bool(block and lot and (str(int(block)) != expected_block or str(int(lot)) != expected_lot))


def _direct_identity_score(record: PublicRecordWatch, raw: dict) -> tuple[int, str | None]:
    if _digits(record.bbl) == _digits(building_bbl_compact()):
        return 45, "exact BBL match"
    if building_bin() and _digits(record.bin) == _digits(building_bin()):
        return 45, "exact BIN match"
    if _block_lot_matches(raw):
        return 40, "exact block/lot match"
    if _address_matches(record, raw):
        return 40, "exact address match"
    return 0, None


def _source_map():
    return {source.key: source for source in source_configs()}


def _elevator_signal(record: PublicRecordWatch, raw: dict) -> tuple[bool, str | None]:
    source = _source_map().get(record.source_system)
    if source and source.elevator_specific:
        return True, "elevator-specific official dataset"
    if record.device_number:
        return True, "elevator device number present"
    haystack = " ".join(
        _normalize_text(value)
        for value in (
            record.source_system,
            record.record_type,
            record.filing_type,
            record.status_detail,
            raw.get("unit"),
            raw.get("complaint_type"),
            raw.get("descriptor"),
            raw.get("violation_type"),
            raw.get("violation_type_code"),
            raw.get("violation_description"),
            raw.get("violation_details"),
            raw.get("novdescription"),
            raw.get("description"),
        )
        if value
    )
    if "ELEVR" in haystack or "ELEVATOR" in haystack or "ELEVATORS" in haystack:
        return True, "elevator text/category signal"
    if _normalize_text(raw.get("violation_type_code")) == "E":
        return True, "DOB elevator violation type code"
    return False, None


def _record_refs(record: PublicRecordWatch, raw: dict | None = None) -> set[str]:
    raw = raw or _raw(record)
    refs: set[str] = set()
    fields = {
        "job": [record.job_number, raw.get("job_filing_number"), raw.get("job_number"), raw.get("job__")],
        "permit": [record.permit_number, raw.get("work_permit"), raw.get("permit_number")],
        "device": [record.device_number, raw.get("device_id"), raw.get("bis_nyc_device_id"), raw.get("device_number")],
        "ticket": [record.record_key, raw.get("ticket_number"), raw.get("ecb_violation_number"), raw.get("ecb_number")],
    }
    for prefix, values in fields.items():
        for value in values:
            clean = _normalize_text(value)
            if clean:
                refs.add(f"{prefix}:{clean}")
    return refs


def _corroboration_index(records: Iterable[PublicRecordWatch]) -> dict[str, list[PublicRecordWatch]]:
    index: dict[str, list[PublicRecordWatch]] = {}
    for record in records:
        for ref in _record_refs(record):
            index.setdefault(ref, []).append(record)
    return index


def _corroborating_records(
    record: PublicRecordWatch,
    records_by_ref: dict[str, list[PublicRecordWatch]],
) -> list[PublicRecordWatch]:
    found: dict[int, PublicRecordWatch] = {}
    for ref in _record_refs(record):
        for candidate in records_by_ref.get(ref, []):
            if candidate.id and candidate.id != record.id:
                found[candidate.id] = candidate
    return list(found.values())


def evaluate_machine_verification(
    record: PublicRecordWatch,
    *,
    records_by_ref: dict[str, list[PublicRecordWatch]],
    min_confidence: int | None = None,
) -> VerificationResult:
    threshold = min_confidence if min_confidence is not None else auto_verify_min_confidence()
    raw = _raw(record)
    reasons: list[str] = []
    if _explicit_identity_conflict(record, raw):
        return VerificationResult(
            status="official_conflict",
            confidence=20,
            summary="Explicit BBL, BIN, block/lot, or address data conflicts with 455 Ocean Parkway.",
            corroborating_records=[],
            machine_verified=False,
        )

    confidence = 0
    if record.source_system in _source_map():
        confidence += 20
        reasons.append("official configured NYC/DOB/Open Data source")

    identity_score, identity_reason = _direct_identity_score(record, raw)
    corroborating = _corroborating_records(record, records_by_ref)
    direct_corroborating = [
        candidate
        for candidate in corroborating
        if _direct_identity_score(candidate, _raw(candidate))[0] > 0 and not _explicit_identity_conflict(candidate, _raw(candidate))
    ]
    if identity_score:
        confidence += identity_score
        reasons.append(identity_reason or "building identity match")
    elif direct_corroborating:
        confidence += 35
        reasons.append("linked by official job/device/ticket to a directly matched building record")

    elevator_specific, elevator_reason = _elevator_signal(record, raw)
    if elevator_specific:
        confidence += 20
        reasons.append(elevator_reason or "elevator-specific signal")
    else:
        confidence += 5
        reasons.append("building-context record, not elevator-specific")

    if record.record_key:
        confidence += 10
        reasons.append("stable public record key")

    if corroborating:
        confidence += 15
        reasons.append("cross-source or cross-record corroboration")

    confidence = min(100, confidence)
    machine_verified = confidence >= threshold
    if machine_verified and corroborating:
        status = "official_corroborated"
    elif machine_verified and elevator_specific:
        status = "official_elevator_match"
    elif machine_verified:
        status = "official_building_match"
    else:
        status = "needs_review"

    corroboration_payload = [
        {
            "id": candidate.id,
            "source_system": candidate.source_system,
            "record_key": candidate.record_key,
        }
        for candidate in sorted(corroborating, key=lambda item: (item.source_system, item.record_key))
    ][:10]
    summary = "; ".join(reasons) or "No official-source verification signals found."
    return VerificationResult(status, confidence, summary, corroboration_payload, machine_verified)


def apply_machine_verification(session) -> dict[str, int]:
    records = list(session.scalars(select(PublicRecordWatch)).all())
    records_by_ref = _corroboration_index(records)
    ts = now_iso()
    counts = {
        "machine_verified": 0,
        "machine_needs_review": 0,
        "machine_conflicts": 0,
        "machine_updated": 0,
        "verification_actions_auto_closed": 0,
    }
    for record in records:
        result = evaluate_machine_verification(record, records_by_ref=records_by_ref)
        counts["machine_verified" if result.machine_verified else "machine_needs_review"] += 1
        if result.status == "official_conflict":
            counts["machine_conflicts"] += 1

        next_verified_at = record.machine_verified_at
        next_verified_by = record.machine_verified_by
        if result.machine_verified and not next_verified_at:
            next_verified_at = ts
            next_verified_by = AUTO_VERIFIER
        if not result.machine_verified:
            next_verified_at = None
            next_verified_by = None

        next_needs_human = False if (record.human_verified_at or result.machine_verified) else True
        changed = (
            record.machine_verification_status != result.status
            or int(record.machine_confidence or 0) != result.confidence
            or record.machine_verified_at != next_verified_at
            or record.machine_verified_by != next_verified_by
            or record.machine_verification_summary != result.summary
            or (record.corroborating_records_json or None) != (json.dumps(result.corroborating_records, sort_keys=True) if result.corroborating_records else None)
            or bool(record.needs_human_verification) != bool(next_needs_human)
        )
        if changed:
            record.machine_verification_status = result.status
            record.machine_confidence = result.confidence
            record.machine_verified_at = next_verified_at
            record.machine_verified_by = next_verified_by
            record.machine_verification_summary = result.summary
            record.corroborating_records_json = json.dumps(result.corroborating_records, sort_keys=True) if result.corroborating_records else None
            record.needs_human_verification = next_needs_human
            counts["machine_updated"] += 1

        if result.machine_verified or result.status != "official_conflict":
            session.flush()
            actions = session.scalars(
                select(WatchdogAction).where(
                    WatchdogAction.source_record_id == record.id,
                    WatchdogAction.action_type.in_(["new_record_needs_verification", "changed_public_record"]),
                    WatchdogAction.status == "open",
                )
            ).all()
            for action in actions:
                action.status = "auto_verified" if result.machine_verified else "auto_reviewed"
                action.completed_at = ts
                action.updated_at = ts
                counts["verification_actions_auto_closed"] += 1
        else:
            actions = session.scalars(
                select(WatchdogAction).where(
                    WatchdogAction.source_record_id == record.id,
                    WatchdogAction.action_type == "changed_public_record",
                    WatchdogAction.status == "open",
                )
            ).all()
            for action in actions:
                action.title = "Official record needs human source check"
                action.detail = (
                    f"Official record {record.record_key} did not reach the automatic confidence threshold. "
                    "Check this one source only if it changes the public elevator-watch answer."
                )
                action.owner_role = "operator"
                action.draft_message = "Please check this one weak official-source match only if it affects the public elevator-watch answer."
                action.updated_at = ts
    return counts
