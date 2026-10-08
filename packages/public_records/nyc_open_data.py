from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlencode

import httpx

from packages.public_records.config import DATA_CITY_BASE, SourceConfig


_REQUEST_LOCK = threading.Lock()
_LAST_REQUEST_AT = 0.0


class SocrataError(RuntimeError):
    pass


class SocrataIncompleteError(SocrataError):
    """A query did not establish that all matching source rows were returned."""


def query_url(source: SourceConfig, params: dict[str, str]) -> str:
    return f"{DATA_CITY_BASE}/resource/{source.dataset_id}.json?{urlencode(params)}"


def _retry_count() -> int:
    try:
        return max(1, int(os.environ.get("NYC_OPEN_DATA_RETRIES", "3")))
    except ValueError:
        return 3


def _request_interval() -> float:
    default = 0.0 if os.environ.get("NYC_OPEN_DATA_APP_TOKEN") or os.environ.get("SOCRATA_APP_TOKEN") else 2.0
    try:
        return max(0.0, min(30.0, float(os.environ.get("NYC_OPEN_DATA_MIN_INTERVAL_SECONDS", str(default)))))
    except ValueError:
        return default


def _pace_request() -> None:
    """Share burst pacing across every source and concurrent fetch in this process."""
    global _LAST_REQUEST_AT
    interval = _request_interval()
    if not interval:
        return
    with _REQUEST_LOCK:
        delay = _LAST_REQUEST_AT + interval - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        _LAST_REQUEST_AT = time.monotonic()


def _retry_delay(response: httpx.Response | None, attempt: int) -> float:
    delay = min(8.0, 2.0 ** (attempt - 1))
    retry_after = response.headers.get("Retry-After", "") if response is not None else ""
    if retry_after:
        try:
            delay = max(delay, float(retry_after))
        except ValueError:
            try:
                until = parsedate_to_datetime(retry_after)
                if until.tzinfo is None:
                    until = until.replace(tzinfo=timezone.utc)
                delay = max(delay, (until - datetime.now(timezone.utc)).total_seconds())
            except (TypeError, ValueError, OverflowError):
                pass
    return min(30.0, max(0.0, delay))


def _get_json(client: httpx.Client, source: SourceConfig, url: str, params: dict[str, str] | None = None):
    attempts = _retry_count()
    for attempt in range(1, attempts + 1):
        try:
            _pace_request()
            response = client.get(url, params=params)
            response.raise_for_status()
            payload = response.json()
            break
        except ValueError as exc:
            raise SocrataError(f"{source.key} returned invalid JSON") from exc
        except (httpx.TimeoutException, httpx.TransportError, httpx.HTTPStatusError) as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            retryable_status = status is not None and (status >= 500 or status == 429)
            if attempt >= attempts or (status is not None and not retryable_status):
                raise SocrataError(f"{source.key} request failed: {type(exc).__name__}" + (f" HTTP {status}" if status else "")) from exc
            time.sleep(_retry_delay(getattr(exc, "response", None), attempt))
    if isinstance(payload, dict) and payload.get("error"):
        raise SocrataError(f"{source.key} query failed: {payload.get('message')}")
    return payload


def _count(client: httpx.Client, source: SourceConfig, params: dict[str, str]) -> int:
    payload = _get_json(client, source, source.endpoint, {**params, "$select": "count(*) as total"})
    try:
        if not isinstance(payload, list) or len(payload) != 1:
            raise ValueError("invalid count payload")
        value = payload[0]["total"]
        if isinstance(value, bool) or not str(value).isdigit():
            raise ValueError("invalid count")
        return int(value)
    except (KeyError, TypeError, ValueError) as exc:
        raise SocrataError(f"{source.key} returned invalid row count") from exc


def _check_schema(client: httpx.Client, source: SourceConfig) -> None:
    payload = _get_json(client, source, f"{DATA_CITY_BASE}/api/views/{source.dataset_id}.json")
    if not isinstance(payload, dict) or not isinstance(payload.get("columns"), list):
        raise SocrataError(f"{source.key} returned invalid schema metadata")
    fields = {column.get("fieldName") for column in payload["columns"] if isinstance(column, dict)}
    missing = set(source.required_fields) - fields
    if missing:
        raise SocrataError(f"{source.key} schema is missing required field(s): {', '.join(sorted(missing))}")


def fetch_rows(source: SourceConfig, params: dict[str, str], *, limit: int = 500, timeout: float = 30.0) -> list[dict]:
    """Fetch the entire query or raise, never return a partial successful snapshot.

    ``limit`` is the page size. Count checks and stable Socrata row IDs detect
    server truncation, repeated/overlapping pages, and changes in result size
    while paging. An actual empty result is a successful empty list.
    """
    if limit < 1:
        raise ValueError("page size must be positive")
    unsupported = {"$limit", "$offset", "$select", "$order"}.intersection(params)
    if unsupported:
        raise ValueError(f"fetch_rows controls pagination: {', '.join(sorted(unsupported))}")
    try:
        max_rows = max(1, int(os.environ.get("NYC_OPEN_DATA_MAX_ROWS", "100000")))
    except ValueError:
        max_rows = 100000
    rows: list[dict] = []
    row_ids: set[str] = set()
    app_token = os.environ.get("NYC_OPEN_DATA_APP_TOKEN") or os.environ.get("SOCRATA_APP_TOKEN")
    headers = {"X-App-Token": app_token} if app_token else {}
    with httpx.Client(timeout=timeout, follow_redirects=True, headers=headers) as client:
        expected_count = _count(client, source, params)
        if expected_count > max_rows:
            raise SocrataIncompleteError(f"{source.key} has {expected_count} matching rows, exceeding safety limit {max_rows}")
        # Metadata also validates genuinely empty queries: an empty response
        # alone cannot establish that the expected source schema still exists.
        _check_schema(client, source)
        while len(rows) < expected_count:
            page_size = min(limit, expected_count - len(rows))
            query = {
                **params,
                "$select": "*, :id as __watchdog_row_id",
                "$order": ":id",
                "$limit": str(page_size),
                "$offset": str(len(rows)),
            }
            payload = _get_json(client, source, source.endpoint, query)
            if not isinstance(payload, list) or any(not isinstance(row, dict) for row in payload):
                raise SocrataError(f"{source.key} query returned invalid row payload")
            if len(payload) != page_size:
                raise SocrataIncompleteError(f"{source.key} returned {len(payload)} of {page_size} expected rows at offset {len(rows)}")
            for row in payload:
                row = dict(row)
                row_id = row.pop("__watchdog_row_id", None)
                if not isinstance(row_id, str) or not row_id or row_id in row_ids:
                    raise SocrataIncompleteError(f"{source.key} returned missing or repeated pagination row ID")
                if source.required_fields and not str(row.get(source.required_fields[0]) or "").strip():
                    raise SocrataError(f"{source.key} row has no required record identifier {source.required_fields[0]}")
                row_ids.add(row_id)
                rows.append(row)
        final_count = _count(client, source, params)
        if final_count != expected_count:
            raise SocrataIncompleteError(f"{source.key} changed during fetch: {expected_count} rows became {final_count}; retry a complete snapshot")
    return rows
