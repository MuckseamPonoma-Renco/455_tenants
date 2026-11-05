"""Check public claims against independently stored source observations.

This module never calls the public renderer or its inclusion predicates. A
renderer which drops an expected message or changes its meaning must fail here.
"""

from __future__ import annotations
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from packages.incident import source_observation as observations
from packages.whatsapp.parser import is_media_placeholder_text
from packages.timeutil import parse_ts_to_epoch
from zoneinfo import ZoneInfo


def _normal(value) -> str:
    return " ".join(str(value or "").split()).casefold()


def _media_permission_errors(raw, decision, row) -> list[str]:
    urls = set(re.findall(r'https?://[^"\s)]+', str(row[4]) + ' ' + str(row[5])))
    if not urls:
        return []
    try:
        review = json.loads(decision.final_json or '{}').get('public_evidence_review') or {}
    except (AttributeError, ValueError, TypeError):
        review = {}
    valid = (review.get('allow')
             and review.get('source_sha256') == observations.source_hash(raw.text or '')
             and review.get('attachments_sha256') == observations.source_hash(raw.attachments or '')
             and urls.issubset(set(review.get('allowed_urls') or [])))
    return [] if valid else ['media_url_not_approved_for_source']


def _reviewed_non_issue_capture(raw, capture: dict) -> bool:
    """A retained full UI copy can resolve a fragment only as a non-report.

    It never supplies evidence for publication or filing. Keep both the original
    raw record and the reviewed full text so the exclusion can be audited again.
    """
    full = capture.get("full_text")
    review = capture.get("full_observation")
    if not isinstance(full, str) or not isinstance(review, dict):
        return False
    bound = observations.bind(review, full)
    return bool(
        capture.get("source_sha256") == observations.source_hash(raw.text or "")
        and capture.get("full_source_sha256") == observations.source_hash(full)
        and raw.text and raw.text in full
        and capture.get("evidence_method") == "verified_accessibility_expansion"
        and re.fullmatch(r"[0-9a-f]{64}", str(capture.get("evidence_sha256", "")))
        and review.get("manual_review") is True
        and bound["review_status"] == "completed"
        and not bound["include"] and not bound["needs_review"]
    )


def claim_errors(observation: dict, label: str, summary: str) -> list[str]:
    """Independent, conservative constraints on the literal published claim."""
    issues = []
    combined = _normal(label + " " + summary)
    label_text = _normal(label)
    if observation["asset"] is None and re.search(
        r"\b(?:north|south)\s+(?:elevator|lift)", label_text
    ):
        issues.append("invented_car")
    if observation["asset"] in {"elevator_north", "elevator_south"}:
        other = "south" if observation["asset"] == "elevator_north" else "north"
        if re.search(r"\b" + other + r"\s+(?:elevator|lift)", label_text):
            issues.append("wrong_car")
    if observation["asset"] != "elevator_both" and re.search(
        r"\bboth\s+(?:elevators?|lifts?)", label_text
    ):
        issues.append("invented_both_cars")
    positive = bool(
        re.search(
            r"\b(?:elevator|lift|elevators|lifts)\s+(?:(?:is|are|was|were|reported|now|currently)\s+)*(?:working|operational|restored|repaired)\b",
            label_text,
        )
        or re.search(r":\s*restoration\s+reported", label_text)
    )
    if positive and observation["state"] not in {
        "working_observation",
        "restoration",
        "condition_restored",
    }:
        issues.append("false_working_or_restoration")
    if observation["state"] == "partial_service" and re.search(
        r"\bboth\b.{0,30}\b(?:working|restored|operational)\b", label_text
    ):
        issues.append("partial_as_full_service")
    if observation["time_scope"] == "past" and not re.search(
        r"\b(?:earlier|previous|yesterday|ago|last\s+(?:night|week)|past)\b", label_text
    ):
        issues.append("lost_past_timing")
    if observation["time_scope"] == "future" and not re.search(
        r"\b(?:planned|expected|scheduled|tomorrow|future|will|would)\b", label_text
    ):
        issues.append("future_as_present")
    if observation["certainty"] == "uncertain" and not re.search(
        r"\b(?:unconfirmed|uncertain|tentative|possibly|appeared|appears|might|may|rumou?r)\b",
        label_text,
    ):
        issues.append("lost_uncertainty")
    return issues


def audit_source_contract(
    sources: list[tuple],
    public_rows: list[list],
    *,
    overrides=None,
    summary_transform=None,
    context_sources=None,
) -> dict:
    """Compare every source, including excluded/misclassified messages, to live rows."""
    overrides = overrides or {}
    summary_transform = summary_transform or (lambda text: text)
    indexed = []
    for index, row in enumerate(public_rows):
        padded = list(row) + [""] * 7
        try:
            epoch = int(
                datetime.strptime(str(padded[0]), "%Y-%m-%d %I:%M %p")
                .replace(tzinfo=ZoneInfo("America/New_York"))
                .timestamp()
            )
        except ValueError:
            continue
        indexed.append((index, epoch, padded))
    missing = []
    unsupported = []
    unreviewed = []
    excluded = 0
    included = 0
    media = 0
    private_reports = 0
    ambiguous = 0
    media_pending = 0
    media_unavailable = 0
    covered = set()
    seen = set()
    source_ids = {raw.message_id for raw, _ in sources}
    context_sources = {
        **{raw.message_id: raw for raw, _ in sources},
        **(context_sources or {}),
    }
    for raw, decision in sources:
        try:
            capture = (
                json.loads(decision.final_json or "{}").get("capture_review", {})
                if decision
                else {}
            )
        except (ValueError, TypeError):
            capture = {}
        if capture.get("state") in {"incomplete", "suspected_incomplete"}:
            unreviewed.append(
                {"message_id": raw.message_id, "reason": "incomplete_message_capture"}
            )
            continue
        if capture.get("state") == "reviewed_non_issue":
            if _reviewed_non_issue_capture(raw, capture):
                excluded += 1
            else:
                unreviewed.append({"message_id": raw.message_id,
                                   "reason": "invalid_full_text_review"})
            continue
        if capture.get("state") == "superseded":
            full = context_sources.get(capture.get("full_message_id"))
            if (
                full is not None
                and full.sender_hash == raw.sender_hash
                and full.chat_name in observations.chat_aliases(raw.chat_name)
                and full.ts_epoch is not None
                and raw.ts_epoch is not None
                and abs(full.ts_epoch - raw.ts_epoch) <= 120
                and len(full.text) > len(raw.text)
                and full.text.startswith(raw.text)
                and observations.source_hash(full.text)
                == capture.get("full_source_sha256")
            ):
                continue
            unreviewed.append(
                {
                    "message_id": raw.message_id,
                    "reason": "invalid_full_message_replacement",
                }
            )
            continue
        try:
            has_media = bool(json.loads(raw.attachments or "{}").get("items"))
        except (ValueError, TypeError):
            has_media = bool(raw.attachments)
        if has_media or is_media_placeholder_text(raw.text):
            try:
                review = (
                    json.loads(decision.final_json or "{}").get("media_review", {})
                    if decision
                    else {}
                )
            except (ValueError, TypeError):
                review = {}
            valid = review.get("source_sha256") == observations.source_hash(
                raw.text or ""
            ) and review.get("attachments_sha256") == observations.source_hash(
                raw.attachments or ""
            )
            if valid and review.get("state") == "completed":
                pass
            elif not has_media or (valid and review.get("state") == "unavailable"):
                media_unavailable += 1
            else:
                media_pending += 1
        try:
            alias_of = (
                json.loads(decision.final_json or "{}").get("alias_of")
                if decision
                else None
            )
        except (ValueError, TypeError):
            alias_of = None
        if alias_of and alias_of in source_ids:
            if observations.alias_matches(raw, context_sources[alias_of]):
                continue
            unreviewed.append(
                {"message_id": raw.message_id, "reason": "invalid_source_alias"}
            )
            continue
        observation = observations.from_decision(
            decision, raw, context_sources=context_sources
        )
        override = overrides.get(raw.message_id)
        if override and override.raw_text_sha256 != observations.source_hash(
            raw.text or ""
        ):
            unreviewed.append(
                {"message_id": raw.message_id, "reason": "override_source_changed"}
            )
            continue
        if override and override.include and raw.attachments:
            try:
                review = (
                    json.loads(decision.final_json or "{}").get("media_review", {})
                    if decision
                    else {}
                )
            except (ValueError, TypeError):
                review = {}
            if review.get("state") != "completed" or review.get(
                "attachments_sha256"
            ) != observations.source_hash(raw.attachments):
                unreviewed.append(
                    {
                        "message_id": raw.message_id,
                        "reason": "override_attachment_changed_or_unreviewed",
                    }
                )
                continue
        if observation is None:
            if is_media_placeholder_text(raw.text):
                media += 1
                continue
            if override is None:
                unreviewed.append(
                    {
                        "message_id": raw.message_id,
                        "reason": "missing_or_invalid_source_observation",
                    }
                )
                continue
        identity = (observation or {}).get("source_identity") or raw.message_id
        if identity in seen:
            continue
        seen.add(identity)
        if observation and observation.get("needs_review"):
            ambiguous += 1
        if (
            not override
            and observations.publication_policy(observation, raw.text or "")
            == "private_review"
        ):
            private_reports += 1
            continue
        include = override.include if override else observation["include"]
        if not include:
            excluded += 1
            continue
        included += 1
        summary = (
            override.summary
            if override
            else summary_transform(observation["public_summary"])
        )
        matches = [
            (index, row)
            for index, epoch, row in indexed
            if raw.ts_epoch is not None
            and 0 <= int(raw.ts_epoch) - epoch < 60
            and _normal(row[6]) == _normal(summary)
        ]
        if not matches:
            missing.append(
                {
                    "message_id": raw.message_id,
                    "reason": "expected_source_report_not_represented",
                }
            )
            continue
        checked = [
            (
                index,
                (claim_errors(observation, row[1], row[6])
                 if observation and not override else [])
                + _media_permission_errors(raw, decision, row),
            )
            for index, row in matches
        ]
        valid = [index for index, errors in checked if not errors]
        if valid:
            covered.update(valid)
        else:
            unsupported.extend(
                {"message_id": raw.message_id, "row_index": index, "errors": errors}
                for index, errors in checked
            )
    unmapped = [
        {
            "row_index": index,
            "reason": "public_claim_has_no_matching_source_observation",
        }
        for index, _, row in indexed
        if index not in covered and row[1] != "Report awaiting review"
    ]
    return {
        "ok": not missing and not unsupported and not unreviewed and not unmapped,
        "sources_checked": len(sources),
        "included_sources": included,
        "excluded_sources": excluded,
        "media_unverifiable": media,
        "private_reports_for_review": private_reports,
        "public_rows_checked": len(indexed),
        "media_pending_review": media_pending,
        "media_unavailable": media_unavailable,
        "ambiguous_reports": ambiguous,
        "missing_reports": missing,
        "unsupported_claims": unsupported,
        "unreviewed_sources": unreviewed,
        "unmapped_public_rows": unmapped,
    }


def save_publication_lineage(rows: list[dict]) -> None:
    """Generation receipt only; the source audit performs separate live readback."""
    path = Path(
        os.environ.get(
            "PUBLICATION_LINEAGE_PATH",
            str(Path.home() / ".local/state/tenant-issue-os/publication-lineage.json"),
        )
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "state": "written_not_independently_verified",
        "rows": rows,
    }
    temp = path.with_suffix(".tmp")
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as handle:
        json.dump(payload, handle)
    os.replace(temp, path)
