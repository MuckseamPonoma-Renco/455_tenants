"""Building-scoped queries and explicit joins between official NYC sources."""
from __future__ import annotations

import re
from collections.abc import Iterable

from packages.public_records.config import (
    building_address_aliases,
    building_bbl_compact,
    building_bin,
    building_borough,
)


def _quoted(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _in(field: str, values: Iterable[str]) -> str:
    return f"{field} in ({', '.join(_quoted(value) for value in sorted(set(values)))})"


def normalize_ticket_reference(value: object) -> str:
    """ECB 39201971R and OATH 039201971R identify the same summons."""
    compact = re.sub(r"[^A-Z0-9]", "", str(value or "").upper())
    match = re.fullmatch(r"0*(\d+)([A-Z]?)", compact)
    if not match:
        return compact
    return f"{int(match.group(1))}{match.group(2)}"


def oath_ticket_query_specs(tickets: Iterable[str]) -> list[dict[str, str]]:
    variants: set[str] = set()
    for ticket in tickets:
        normalized = normalize_ticket_reference(ticket)
        if not normalized or not re.fullmatch(r"\d+[A-Z]?", normalized):
            continue
        variants.update((normalized, normalized.zfill(10)))
    # Keep SoQL URLs bounded even when many historical ECB records are present.
    ordered = sorted(variants)
    return [
        {"issuing_agency": "DEPT. OF BUILDINGS", "$where": "issuing_agency = 'DEPT. OF BUILDINGS' AND " + _in("ticket_number", ordered[start:start + 80])}
        for start in range(0, len(ordered), 80)
    ]


def electrical_device_references(row: dict) -> set[str]:
    description = str(row.get("job_description") or "").upper()
    # NYC passenger elevator device IDs (e.g. 3P6189). Avoid treating job IDs as devices.
    return set(re.findall(r"\b[1-5]P\d{3,7}\b", description))


def electrical_is_elevator_related(row: dict) -> bool:
    scope = " ".join(str(row.get(field) or "") for field in ("category_work_list", "job_description"))
    return bool(re.search(r"\belevators?\b", scope, re.IGNORECASE) or electrical_device_references(row))


def query_specs() -> list[tuple[str, dict[str, str]]]:
    bbl = building_bbl_compact()
    bin_value = building_bin()
    borough = building_borough().upper()
    oath_scope = f"issuing_agency = 'DEPT. OF BUILDINGS' AND violation_location_borough = {_quoted(borough)}"
    specs = [
        ("dob_now_elevator_applications", {"bbl": bbl}),
        ("dob_now_electrical_applications", {"bin": bin_value}),
        ("dob_now_elevator_safety_compliance", {"bbl": bbl}),
        ("dob_complaints", {"bin": bin_value, "unit": "ELEVR"}),
        ("dob_violations", {"bin": bin_value}),
        ("dob_ecb_violations", {"bin": bin_value}),
    ]
    if re.fullmatch(r"\d{10}", bbl):
        block, lot = bbl[1:6], bbl[6:10]
        specs.append(("oath_hearings", {
            "issuing_agency": "DEPT. OF BUILDINGS",
            "violation_location_borough": borough,
            "$where": (
                oath_scope + " AND " + _in("violation_location_block_no", (block, str(int(block))))
                + " AND " + _in("violation_location_lot_no", (lot, str(int(lot))))
            ),
        }))
    addresses = []
    for address in building_address_aliases():
        house, separator, street = address.strip().partition(" ")
        if separator:
            addresses.append(
                f"(violation_location_house = {_quoted(house)} AND "
                f"violation_location_street_name = {_quoted(street.upper())})"
            )
    if addresses:
        specs.append(("oath_hearings", {
            "issuing_agency": "DEPT. OF BUILDINGS",
            "violation_location_borough": borough,
            "$where": oath_scope + " AND (" + " OR ".join(addresses) + ")",
        }))
    specs.extend([
        ("nyc_311", {"bbl": bbl, "agency": "DOB", "complaint_type": "Elevator"}),
        ("hpd_building", {"bin": bin_value}),
        # Keep closed history in the refresh so an official correction replaces
        # the prior open status instead of merely disappearing from the query.
        ("hpd_violations", {"bbl": bbl}),
    ])
    return specs
