from __future__ import annotations

import os
import time
from urllib.parse import urlencode

import httpx

from packages.public_records.config import DATA_CITY_BASE, SourceConfig


class SocrataError(RuntimeError):
    pass


def query_url(source: SourceConfig, params: dict[str, str]) -> str:
    return f"{DATA_CITY_BASE}/resource/{source.dataset_id}.json?{urlencode(params)}"


def _retry_count() -> int:
    try:
        return max(1, int(os.environ.get("NYC_OPEN_DATA_RETRIES", "3")))
    except ValueError:
        return 3


def fetch_rows(source: SourceConfig, params: dict[str, str], *, limit: int = 500, timeout: float = 30.0) -> list[dict]:
    query = {"$limit": str(limit), **params}
    attempts = _retry_count()
    last_exc: Exception | None = None
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        for attempt in range(1, attempts + 1):
            try:
                response = client.get(f"{DATA_CITY_BASE}/resource/{source.dataset_id}.json", params=query)
                response.raise_for_status()
                payload = response.json()
                break
            except (httpx.TimeoutException, httpx.TransportError, httpx.HTTPStatusError) as exc:
                last_exc = exc
                status = getattr(getattr(exc, "response", None), "status_code", None)
                retryable_status = status is not None and (status >= 500 or status == 429)
                if attempt >= attempts or (status is not None and not retryable_status):
                    raise
                time.sleep(min(2.0, 0.25 * attempt))
        else:
            raise last_exc or SocrataError(f"{source.key} query failed")
    if isinstance(payload, dict) and payload.get("error"):
        raise SocrataError(f"{source.key} query failed: {payload.get('message')}")
    if not isinstance(payload, list):
        raise SocrataError(f"{source.key} query returned non-list payload")
    return [row for row in payload if isinstance(row, dict)]
