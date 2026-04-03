from __future__ import annotations

import os
import re
from typing import Any

from packages.local_env import load_local_env_file

WHITESPACE_RE = re.compile(r"\s+")
PKWY_RE = re.compile(r"\bPKWY\b", flags=re.IGNORECASE)
STATE_ZIP_RE = re.compile(r"^(?P<state>[A-Za-z]{2})(?:\s+(?P<zip>\d{5}(?:-\d{4})?))?$")


def _env(name: str, default: str = "") -> str:
    value = os.environ.get(name)
    return value if value is not None and value != "" else default


def _clean(value: Any) -> str:
    return WHITESPACE_RE.sub(" ", str(value or "").replace("\n", " ")).strip()


def _upper(value: Any) -> str:
    return _clean(value).upper()


def _normalize_street(value: Any) -> str:
    clean = _clean(value)
    if not clean:
        return ""
    return PKWY_RE.sub("PARKWAY", clean).upper()


def _split_full_address(value: str) -> tuple[str, str, str, str]:
    parts = [part.strip() for part in _clean(value).split(",") if part.strip()]
    street = parts[0] if parts else ""
    city = parts[1] if len(parts) >= 2 else ""
    state = ""
    zip_code = ""
    if len(parts) >= 3:
        match = STATE_ZIP_RE.match(parts[2])
        if match:
            state = match.group("state") or ""
            zip_code = match.group("zip") or ""
        else:
            state = parts[2]
    if len(parts) >= 4 and not zip_code:
        zip_code = _clean(parts[3])
    return street, city, state, zip_code


def build_full_address(street: Any, city: Any, state: Any, zip_code: Any) -> str:
    street_text = _normalize_street(street)
    city_text = _upper(city)
    state_text = _upper(state)
    zip_text = _clean(zip_code)
    parts = [part for part in (street_text, city_text, state_text, zip_text) if part]
    return ", ".join(parts)


def canonicalize_building_address(building: dict[str, Any] | None = None) -> dict[str, str]:
    load_local_env_file()
    building = building or {}
    raw_full_address = _clean(building.get("full_address"))
    raw_street = _clean(building.get("street_address"))
    full_address = raw_full_address or _clean(_env("BUILDING_FULL_ADDRESS"))
    street = raw_street or _clean(_env("BUILDING_STREET_ADDRESS"))
    city = _clean(building.get("city") or _env("BUILDING_CITY"))
    state = _clean(building.get("state") or _env("BUILDING_STATE"))
    zip_code = _clean(building.get("zip") or _env("BUILDING_ZIP"))
    borough = _clean(building.get("borough") or _env("BUILDING_BOROUGH"))

    if raw_street and "," in raw_street and not raw_full_address:
        full_address = raw_street
        street = ""
    elif not full_address and "," in street:
        full_address = street
        street = ""

    parsed_street = parsed_city = parsed_state = parsed_zip = ""
    if full_address:
        parsed_street, parsed_city, parsed_state, parsed_zip = _split_full_address(full_address)

    street = _normalize_street(street or parsed_street)
    city = _upper(city or parsed_city or "Brooklyn")
    state = _upper(state or parsed_state or "NY")
    zip_code = _clean(zip_code or parsed_zip)
    borough = _upper(borough or city or "Brooklyn")
    full_address = build_full_address(street, city, state, zip_code)

    return {
        "street_address": street,
        "city": city,
        "state": state,
        "zip": zip_code,
        "borough": borough,
        "full_address": full_address,
    }
