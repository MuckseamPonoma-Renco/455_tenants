#!/usr/bin/env python3
"""Certify the exact unchanged and deferred decisions from the full archive audit.

The committed ledger is a closed-world record of decisions which were
individually reviewed on 2026-09-05.  The default mode is a read-only plan.
``--apply`` first verifies every raw-text digest and every stored decision,
then updates review provenance for the complete ledger in one transaction.

This script does not classify messages, change incident links or incident
materialization, queue or submit filings, or write to Google Sheets.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import hmac
import json
import re
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from sqlalchemy import select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.local_env import load_local_env_file

load_local_env_file(ROOT / ".env")

from packages.audit import append_audit_event, daily_hash_chain  # noqa: E402
from packages.db import MessageDecision, RawMessage, get_session  # noqa: E402


CERTIFICATION_ID = "2026-09-05-full-archive-manual-review-v1"
REVIEWED_BY = "codex:2026-09-05-full-archive-manual-audit"
REVIEW_KIND = "codex_full_archive_manual_audit"
CHOSEN_SOURCE = "review_codex_full_archive_manual_audit"
LEDGER_PATH = Path(__file__).with_name("full_archive_review_ledger_20260905.json")
EXPECTED_OUTCOME_COUNTS = {
    "unchanged_correct": 199,
    "deferred_missing_evidence": 7,
}

_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_MESSAGE_ID = re.compile(r"^(?:[0-9a-f]+)(?:~[2-9][0-9]*)?$")
_ROOT_FIELDS = frozenset(
    {"schema_version", "certification_id", "reviewed_by", "expected_counts", "reviews"}
)
_COUNT_FIELDS = frozenset({"total", *EXPECTED_OUTCOME_COUNTS})
_ENTRY_FIELDS = frozenset(
    {
        "export_ordinal",
        "message_id",
        "raw_text_sha256",
        "review_outcome",
        "expected_decision",
        "audit_rationale",
    }
)
_DECISION_FIELDS = frozenset(
    {"is_issue", "category", "asset", "event_type", "incident_id"}
)


class ReviewLedgerError(RuntimeError):
    """The committed manual-review ledger is malformed or incomplete."""


@dataclass(frozen=True, slots=True)
class ExpectedDecision:
    is_issue: bool
    category: str | None
    asset: str | None
    event_type: str | None
    incident_id: str | None


@dataclass(frozen=True, slots=True)
class ReviewEntry:
    export_ordinal: int
    message_id: str
    raw_text_sha256: str
    review_outcome: str
    expected_decision: ExpectedDecision
    audit_rationale: str


@dataclass(frozen=True, slots=True)
class ReviewLedger:
    certification_id: str
    reviewed_by: str
    sha256: str
    entries: tuple[ReviewEntry, ...]


def _now_iso() -> str:
    return dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")


def _require_exact_fields(
    payload: Mapping[str, object], expected: frozenset[str], *, location: str
) -> None:
    actual = frozenset(payload)
    if actual == expected:
        return
    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected)
    raise ReviewLedgerError(
        f"invalid fields at {location}: missing={missing}, unexpected={unexpected}"
    )


def _optional_text(value: object, *, location: str) -> str | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ReviewLedgerError(f"{location} must be a string or null")
    return value


def _parse_expected_decision(payload: object, *, index: int) -> ExpectedDecision:
    location = f"reviews[{index}].expected_decision"
    if not isinstance(payload, dict):
        raise ReviewLedgerError(f"{location} must be an object")
    _require_exact_fields(payload, _DECISION_FIELDS, location=location)
    if not isinstance(payload["is_issue"], bool):
        raise ReviewLedgerError(f"{location}.is_issue must be a boolean")

    expected = ExpectedDecision(
        is_issue=payload["is_issue"],
        category=_optional_text(payload["category"], location=f"{location}.category"),
        asset=_optional_text(payload["asset"], location=f"{location}.asset"),
        event_type=_optional_text(payload["event_type"], location=f"{location}.event_type"),
        incident_id=_optional_text(payload["incident_id"], location=f"{location}.incident_id"),
    )
    if expected.is_issue:
        if expected.category is None or expected.event_type is None or expected.incident_id is None:
            raise ReviewLedgerError(
                f"{location} issue decisions require category, event_type, and exact incident_id"
            )
    elif any(
        value is not None
        for value in (
            expected.category,
            expected.asset,
            expected.event_type,
            expected.incident_id,
        )
    ):
        raise ReviewLedgerError(
            f"{location} nonissue decisions require null category, asset, event_type, and incident_id"
        )
    return expected


def _parse_entry(payload: object, *, index: int) -> ReviewEntry:
    location = f"reviews[{index}]"
    if not isinstance(payload, dict):
        raise ReviewLedgerError(f"{location} must be an object")
    _require_exact_fields(payload, _ENTRY_FIELDS, location=location)

    ordinal = payload["export_ordinal"]
    message_id = payload["message_id"]
    digest = payload["raw_text_sha256"]
    outcome = payload["review_outcome"]
    rationale = payload["audit_rationale"]
    if type(ordinal) is not int or ordinal < 1:
        raise ReviewLedgerError(f"{location}.export_ordinal must be a positive integer")
    if (
        not isinstance(message_id, str)
        or len(message_id) != 64
        or not _MESSAGE_ID.fullmatch(message_id)
    ):
        raise ReviewLedgerError(f"{location}.message_id is not a supported exact message ID")
    if not isinstance(digest, str) or not _HEX_64.fullmatch(digest):
        raise ReviewLedgerError(f"{location}.raw_text_sha256 must be 64 lowercase hex characters")
    if outcome not in EXPECTED_OUTCOME_COUNTS:
        raise ReviewLedgerError(f"{location}.review_outcome is unsupported")
    if not isinstance(rationale, str) or not rationale.strip():
        raise ReviewLedgerError(f"{location}.audit_rationale must not be blank")

    return ReviewEntry(
        export_ordinal=ordinal,
        message_id=message_id,
        raw_text_sha256=digest,
        review_outcome=outcome,
        expected_decision=_parse_expected_decision(payload["expected_decision"], index=index),
        audit_rationale=rationale.strip(),
    )


def load_review_ledger(path: str | Path | None = None) -> ReviewLedger:
    selected = Path(path) if path is not None else LEDGER_PATH
    try:
        raw_bytes = selected.read_bytes()
        payload = json.loads(raw_bytes.decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ReviewLedgerError(f"unable to load review ledger from {selected}") from exc
    if not isinstance(payload, dict):
        raise ReviewLedgerError("review ledger root must be an object")
    _require_exact_fields(payload, _ROOT_FIELDS, location="root")
    if type(payload["schema_version"]) is not int or payload["schema_version"] != 1:
        raise ReviewLedgerError("unsupported review ledger schema_version")
    if payload["certification_id"] != CERTIFICATION_ID:
        raise ReviewLedgerError("review ledger certification_id does not match this certifier")
    if payload["reviewed_by"] != REVIEWED_BY:
        raise ReviewLedgerError("review ledger reviewed_by does not match this certifier")

    counts = payload["expected_counts"]
    if not isinstance(counts, dict):
        raise ReviewLedgerError("root.expected_counts must be an object")
    _require_exact_fields(counts, _COUNT_FIELDS, location="root.expected_counts")
    if counts != {"total": sum(EXPECTED_OUTCOME_COUNTS.values()), **EXPECTED_OUTCOME_COUNTS}:
        raise ReviewLedgerError("review ledger expected_counts does not match the closed-world scope")

    rows = payload["reviews"]
    if not isinstance(rows, list):
        raise ReviewLedgerError("root.reviews must be an array")
    entries = tuple(_parse_entry(row, index=index) for index, row in enumerate(rows))
    message_ids = [entry.message_id for entry in entries]
    ordinals = [entry.export_ordinal for entry in entries]
    if len(message_ids) != len(set(message_ids)):
        raise ReviewLedgerError("review ledger contains duplicate message_id values")
    if len(ordinals) != len(set(ordinals)):
        raise ReviewLedgerError("review ledger contains duplicate export_ordinal values")
    actual_counts = Counter(entry.review_outcome for entry in entries)
    if actual_counts != Counter(EXPECTED_OUTCOME_COUNTS):
        raise ReviewLedgerError(
            f"review ledger outcome counts do not match scope: {dict(actual_counts)}"
        )
    if list(ordinals) != sorted(ordinals):
        raise ReviewLedgerError("review ledger rows must be sorted by export_ordinal")

    return ReviewLedger(
        certification_id=payload["certification_id"],
        reviewed_by=payload["reviewed_by"],
        sha256=hashlib.sha256(raw_bytes).hexdigest(),
        entries=entries,
    )


def _raw_text_sha256(raw: RawMessage) -> str:
    return hashlib.sha256((raw.text or "").encode("utf-8")).hexdigest()


def _parse_final_json(
    decision: MessageDecision, *, message_id: str, errors: list[str]
) -> dict[str, object] | None:
    try:
        parsed = json.loads(decision.final_json or "{}")
    except (json.JSONDecodeError, TypeError):
        errors.append(f"{message_id}: final_json is not valid JSON")
        return None
    if not isinstance(parsed, dict):
        errors.append(f"{message_id}: final_json must be a JSON object")
        return None
    return parsed


def _normalize_db_text(value: object, *, location: str) -> str | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ReviewLedgerError(f"{location} must be a string or null")
    return value


def _decision_state(
    decision: MessageDecision, final: Mapping[str, object]
) -> ExpectedDecision:
    return ExpectedDecision(
        is_issue=bool(decision.is_issue),
        category=_normalize_db_text(decision.category, location="decision.category"),
        asset=_normalize_db_text(final.get("asset"), location="final_json.asset"),
        event_type=_normalize_db_text(decision.event_type, location="decision.event_type"),
        incident_id=_normalize_db_text(decision.incident_id, location="decision.incident_id"),
    )


def _desired_needs_review(entry: ReviewEntry) -> bool:
    return entry.review_outcome == "deferred_missing_evidence"


def _has_certification(
    decision: MessageDecision,
    final: Mapping[str, object],
    *,
    entry: ReviewEntry,
    ledger: ReviewLedger,
) -> bool:
    return bool(
        decision.chosen_source == CHOSEN_SOURCE
        and decision.needs_review is _desired_needs_review(entry)
        and final.get("review_status") == "completed"
        and final.get("review_kind") == REVIEW_KIND
        and final.get("review_outcome") == entry.review_outcome
        and final.get("review_id") == ledger.certification_id
        and final.get("reviewed_by") == ledger.reviewed_by
        and final.get("review_ledger_sha256") == ledger.sha256
        and final.get("review_rationale") == entry.audit_rationale
        and final.get("needs_review") is _desired_needs_review(entry)
        and isinstance(final.get("reviewed_at"), str)
        and bool(str(final.get("reviewed_at") or "").strip())
    )


def _preflight(
    session, ledger: ReviewLedger
) -> tuple[
    list[str],
    dict[str, MessageDecision],
    dict[str, dict[str, object]],
    list[str],
    list[str],
]:
    errors: list[str] = []
    message_ids = [entry.message_id for entry in ledger.entries]
    raws = session.scalars(select(RawMessage).where(RawMessage.message_id.in_(message_ids))).all()
    decisions = session.scalars(
        select(MessageDecision).where(MessageDecision.message_id.in_(message_ids))
    ).all()
    raw_by_id = {row.message_id: row for row in raws}
    decision_by_id = {row.message_id: row for row in decisions}
    final_by_id: dict[str, dict[str, object]] = {}
    to_certify: list[str] = []
    already_certified: list[str] = []

    for entry in ledger.entries:
        raw = raw_by_id.get(entry.message_id)
        decision = decision_by_id.get(entry.message_id)
        if raw is None:
            errors.append(f"{entry.message_id}: raw message missing")
        else:
            actual_digest = _raw_text_sha256(raw)
            if not hmac.compare_digest(actual_digest, entry.raw_text_sha256):
                errors.append(
                    f"{entry.message_id}: raw-text SHA256 mismatch "
                    f"(expected {entry.raw_text_sha256}, got {actual_digest})"
                )
        if decision is None:
            errors.append(f"{entry.message_id}: decision missing")
            continue
        final = _parse_final_json(decision, message_id=entry.message_id, errors=errors)
        if final is None:
            continue
        final_by_id[entry.message_id] = final
        try:
            actual = _decision_state(decision, final)
        except ReviewLedgerError as exc:
            errors.append(f"{entry.message_id}: invalid decision state: {exc}")
            continue
        if actual != entry.expected_decision:
            errors.append(
                f"{entry.message_id}: decision state drift; "
                f"expected {entry.expected_decision}, got {actual}"
            )
            continue
        if _has_certification(decision, final, entry=entry, ledger=ledger):
            already_certified.append(entry.message_id)
        else:
            to_certify.append(entry.message_id)

    return errors, decision_by_id, final_by_id, to_certify, already_certified


def _apply_provenance(
    decision: MessageDecision,
    final: dict[str, object],
    *,
    entry: ReviewEntry,
    ledger: ReviewLedger,
    reviewed_at: str,
) -> None:
    needs_review = _desired_needs_review(entry)
    # Copy before update so every unrelated nested/scalar final_json field is
    # preserved exactly at the semantic JSON level.
    updated = dict(final)
    updated.update(
        {
            "review_status": "completed",
            "review_kind": REVIEW_KIND,
            "review_outcome": entry.review_outcome,
            "review_id": ledger.certification_id,
            "reviewed_at": reviewed_at,
            "reviewed_by": ledger.reviewed_by,
            "review_ledger_sha256": ledger.sha256,
            "review_rationale": entry.audit_rationale,
            "needs_review": needs_review,
        }
    )
    decision.final_json = json.dumps(updated, ensure_ascii=False, sort_keys=True)
    decision.chosen_source = CHOSEN_SOURCE
    decision.needs_review = needs_review


def certify(*, apply: bool, ledger_path: str | Path | None = None) -> dict[str, object]:
    try:
        ledger = load_review_ledger(ledger_path)
    except ReviewLedgerError as exc:
        return {
            "apply": apply,
            "certification_id": CERTIFICATION_ID,
            "target_message_count": sum(EXPECTED_OUTCOME_COUNTS.values()),
            "outcome_counts": dict(EXPECTED_OUTCOME_COUNTS),
            "to_certify": [],
            "already_certified": [],
            "errors": [str(exc)],
            "applied": False,
            "changed": False,
        }

    reviewed_at = _now_iso()
    errors: list[str]
    to_certify: list[str]
    already_certified: list[str]
    with get_session() as session:
        errors, decision_by_id, final_by_id, to_certify, already_certified = _preflight(
            session, ledger
        )
        plan: dict[str, object] = {
            "apply": apply,
            "certification_id": ledger.certification_id,
            "reviewed_by": ledger.reviewed_by,
            "ledger_sha256": ledger.sha256,
            "target_message_count": len(ledger.entries),
            "outcome_counts": dict(Counter(entry.review_outcome for entry in ledger.entries)),
            "to_certify": to_certify,
            "already_certified": already_certified,
            "errors": errors,
            "applied": False,
            "changed": False,
        }
        if errors or not apply:
            session.rollback()
            return plan

        try:
            entries_by_id = {entry.message_id: entry for entry in ledger.entries}
            for message_id in to_certify:
                _apply_provenance(
                    decision_by_id[message_id],
                    final_by_id[message_id],
                    entry=entries_by_id[message_id],
                    ledger=ledger,
                    reviewed_at=reviewed_at,
                )
            session.flush()
            session.commit()
        except Exception as exc:
            session.rollback()
            return {
                **plan,
                "errors": [f"certification transaction failed: {type(exc).__name__}: {exc}"],
                "applied": False,
                "changed": False,
            }

    changed = bool(to_certify)
    result = {**plan, "applied": True, "changed": changed}
    if changed:
        changed_ids = set(to_certify)
        append_audit_event(
            "FULL_ARCHIVE_MANUAL_REVIEW_CERTIFIED",
            None,
            {
                "certification_id": ledger.certification_id,
                "reviewed_at": reviewed_at,
                "reviewed_by": ledger.reviewed_by,
                "ledger_sha256": ledger.sha256,
                "message_ids": to_certify,
                "outcome_counts": dict(
                    Counter(
                        entry.review_outcome
                        for entry in ledger.entries
                        if entry.message_id in changed_ids
                    )
                ),
            },
        )
        daily_hash_chain()
    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Certify the exact unchanged and deferred decisions individually reviewed "
            "during the 2026-09-05 full WhatsApp archive audit."
        )
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply one all-or-nothing provenance transaction; default is read-only.",
    )
    args = parser.parse_args()
    result = certify(apply=args.apply)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 1 if result["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
