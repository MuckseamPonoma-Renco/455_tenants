from __future__ import annotations

import hashlib
import json
from typing import Any
from urllib.parse import urlencode

from packages.public_records.config import DATA_CITY_BASE, SourceConfig
from packages.timeutil import normalize_timestamp


def raw_hash(row: dict[str, Any]) -> str:
    payload = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def raw_json(row: dict[str, Any]) -> str:
    return json.dumps(row, ensure_ascii=False, sort_keys=True)


def record_key(source: SourceConfig, row: dict[str, Any]) -> str:
    if source.key == "dob_now_elevator_applications":
        return str(row.get("job_filing_number") or "").strip()
    if source.key == "dob_now_elevator_device_details":
        job = str(row.get("job_filing_number") or "").strip()
        device = str(row.get("device_id") or row.get("bis_nyc_device_id") or "").strip()
        return f"{job}:{device}".strip(":")
    if source.key == "dob_now_elevator_safety_compliance":
        return str(row.get("device_number") or "").strip()
    if source.key == "dob_complaints":
        return str(row.get("complaint_number") or "").strip()
    if source.key == "dob_violations":
        return str(row.get("isn_dob_bis_viol") or row.get("violation_number") or "").strip()
    if source.key == "dob_ecb_violations":
        return str(row.get("ecb_violation_number") or row.get("isn_dob_bis_extract") or "").strip()
    if source.key == "oath_hearings":
        return str(row.get("ticket_number") or "").strip()
    if source.key == "nyc_311":
        return str(row.get("unique_key") or "").strip()
    if source.key == "hpd_building":
        return str(row.get("buildingid") or "").strip()
    if source.key == "hpd_violations":
        return str(row.get("violationid") or "").strip()
    for field in ("id", "record_id", "unique_key"):
        value = str(row.get(field) or "").strip()
        if value:
            return value
    return raw_hash(row)


def _first(row: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = row.get(key)
        if value is not None and str(value).strip() != "":
            return str(value).strip()
    return None


def _date(row: dict[str, Any], *keys: str) -> str | None:
    value = _first(row, *keys)
    return normalize_timestamp(value) if value else None


def _address(row: dict[str, Any]) -> str | None:
    house = _first(row, "house_number", "house_no", "house__", "housenumber", "violation_location_house")
    street = _first(row, "street_name", "house_street", "street", "streetname", "incident_address", "violation_location_street_name")
    if street and house and house not in street:
        return f"{house} {street}".strip()
    return street or _first(row, "physical_address", "incident_address")


def _source_url(source: SourceConfig, params: dict[str, str] | None = None) -> str:
    if not params:
        return source.url
    return f"{DATA_CITY_BASE}/resource/{source.dataset_id}.json?{urlencode(params)}"


def normalize_record(source: SourceConfig, row: dict[str, Any], *, source_url: str | None = None) -> dict[str, Any]:
    key = record_key(source, row)
    if not key:
        raise ValueError(f"{source.key}: cannot determine record key")

    status = _first(row, "filing_status", "device_status", "status", "ecb_violation_status", "hearing_status", "violation_category", "currentstatus", "recordstatus")
    status_detail = _first(
        row,
        "filingstatus_or_filingincludes",
        "descriptionofwork",
        "violation_description",
        "violation_details",
        "description",
        "resolution_description",
        "novdescription",
        "disposition_comments",
        "device_job_description",
    )
    job_number = _first(row, "job_filing_number", "job_number", "job__", "dob_violation_number")
    permit_number = _first(row, "work_permit", "permit_number", "tracking_number")
    device_number = _first(row, "device_number", "device_id", "bis_nyc_device_id")

    return {
        "source_system": source.key,
        "record_type": source.record_type,
        "record_key": key,
        "bbl": _first(row, "bbl"),
        "bin": _first(row, "bin", "bin__"),
        "address": _address(row),
        "job_number": job_number,
        "permit_number": permit_number,
        "device_number": device_number,
        "filing_type": _first(row, "filing_type", "filing_reason", "job_type", "violation_type", "complaint_type"),
        "status": status,
        "status_detail": status_detail,
        "filed_at": _date(row, "filing_date", "date_entered", "created_date", "issue_date", "violation_date", "inspectiondate"),
        "approved_at": _date(row, "approved_date", "approveddate", "permit_entire_date"),
        "permit_issued_at": _date(row, "issued_date", "permit_entire_date"),
        "inspection_date": _date(row, "periodic_latest_inspection", "inspection_date", "inspectiondate"),
        "expires_at": _date(row, "permit_expiration_date", "expired_date", "expiration_date"),
        "source_url": source_url or _source_url(source),
        "raw_json": raw_json(row),
        "raw_hash": raw_hash(row),
        "visible_public": True,
        "needs_human_verification": True,
        "notes": source.notes or None,
    }
