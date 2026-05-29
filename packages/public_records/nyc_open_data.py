from __future__ import annotations

from urllib.parse import urlencode

import httpx

from packages.public_records.config import DATA_CITY_BASE, SourceConfig


class SocrataError(RuntimeError):
    pass


def query_url(source: SourceConfig, params: dict[str, str]) -> str:
    return f"{DATA_CITY_BASE}/resource/{source.dataset_id}.json?{urlencode(params)}"


def fetch_rows(source: SourceConfig, params: dict[str, str], *, limit: int = 500, timeout: float = 30.0) -> list[dict]:
    query = {"$limit": str(limit), **params}
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        response = client.get(f"{DATA_CITY_BASE}/resource/{source.dataset_id}.json", params=query)
        response.raise_for_status()
        payload = response.json()
    if isinstance(payload, dict) and payload.get("error"):
        raise SocrataError(f"{source.key} query failed: {payload.get('message')}")
    if not isinstance(payload, list):
        raise SocrataError(f"{source.key} query returned non-list payload")
    return [row for row in payload if isinstance(row, dict)]
