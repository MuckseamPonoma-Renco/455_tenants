"""Cached source/readback quality, separate from process liveness. No DB reads."""

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def _path():
    return Path(
        os.environ.get(
            "SOURCE_QUALITY_STATUS_PATH",
            str(Path.home() / ".local/state/tenant-issue-os/source-quality.json"),
        )
    )


def save(result):
    truth = result.get("source_truth") or {}
    counts = {
        key: len(truth.get(key) or [])
        for key in (
            "missing_reports",
            "unsupported_claims",
            "unreviewed_sources",
            "unmapped_public_rows",
        )
    }
    counts.update(
        {
            key: int(truth.get(key) or 0)
            for key in (
                "media_pending_review",
                "media_unavailable",
                "private_reports_for_review",
                "ambiguous_reports",
            )
        }
    )
    state = (
        "unverified"
        if not truth
        else "review_required"
        if any(counts.values())
        else "verified"
        if result.get("ok")
        else "mismatch"
    )
    payload = {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "state": state,
        "days_checked": result.get("days"),
        "source_truth_ok": result.get("source_truth_ok"),
        "sheet_readback_ok": not bool(result.get("live_read_error"))
        and bool(result.get("full_tenant_log_ok")),
        **counts,
    }
    path = _path()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".source-quality-")
    try:
        with os.fdopen(fd, "w") as h:
            json.dump(payload, h)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def public_status():
    from packages.incident.source_observation import required

    try:
        result = json.loads(_path().read_text())
        age = (
            datetime.now(timezone.utc) - datetime.fromisoformat(result["checked_at"])
        ).total_seconds()
        if (
            not 0
            <= age
            <= max(3600, int(os.environ.get("SOURCE_QUALITY_MAX_AGE_SECONDS", "10800")))
        ):
            result["state"] = "stale"
    except (OSError, ValueError, TypeError, KeyError):
        result = {"state": "unverified"}
    if not required():
        result["state"] = "legacy_unverified"
    return {**result, "source_observations_required": required()}
