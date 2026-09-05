#!/usr/bin/env python3
"""Restore the exact 20 physical archive occurrences lost on 2026-09-05.

The default mode is read-only.  Apply mode inserts only absent, hash-locked
RawMessage/MessageDecision pairs in one transaction.  It never invokes the
classifier, incident materializer, filing queue, or Google Sheets sync.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import hmac
import json
import sys
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path

from sqlalchemy import func, select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.local_env import load_local_env_file

load_local_env_file(ROOT / ".env")

from packages.audit import append_audit_event, daily_hash_chain, sender_hash  # noqa: E402
from packages.db import (  # noqa: E402
    FilingJob,
    Incident,
    MessageDecision,
    RawMessage,
    ServiceRequestCase,
    WatchdogAction,
    get_session,
)
from packages.whatsapp.attachments import attachment_items  # noqa: E402
from packages.whatsapp.parser import is_media_placeholder_text  # noqa: E402
from packages.timeutil import parse_ts_to_epoch  # noqa: E402
from scripts.audit_whatsapp_export_decisions import ExportMessage, iter_export_messages  # noqa: E402


RESTORE_ID = "2026-09-05-full-archive-occurrence-restore-v1"
REVIEWED_BY = "codex:2026-09-05-full-archive-occurrence-audit"
DEFAULT_ARCHIVE = Path(
    "/Users/max/.local/share/tenant-issue-os/runtime/incoming/cloud_chat_exports/"
    "cloud-2122d0a7948a97c3-WhatsApp Chat - 455 Tenants.zip"
)
EXPECTED_ARCHIVE_SHA256 = "7baad13600e3226105ca79773b0961d00cb3bc87c17eff2b7bd5a0e6644c3471"
EXPECTED_CHAT_ENTRY = "_chat.txt"
EXPECTED_CHAT_TEXT_SHA256 = "761e219b66245afdd8c0d9a38da97efe5a94d4883e6c35c519f8b7f0d0696b4e"
EXPECTED_CHAT_NAME = "455 Tenants"
EXPECTED_PHYSICAL_MESSAGES = 2741
EXPECTED_RESTORE_OCCURRENCES = 20
IMAGE_OMITTED_SHA256 = "692995e611ee58b172f91f6b1dade7d420801c85fa7af03064285a4c3947292e"


@dataclass(frozen=True)
class RestoreSpec:
    export_ordinal: int
    message_id: str
    physical_occurrence: int
    text: str
    text_sha256: str
    decision_kind: str
    rationale: str
    allow_media_enrichment: bool = True


def _media_spec(export_ordinal: int, message_id: str, occurrence: int) -> RestoreSpec:
    return RestoreSpec(
        export_ordinal=export_ordinal,
        message_id=message_id,
        physical_occurrence=occurrence,
        text="image omitted",
        text_sha256=IMAGE_OMITTED_SHA256,
        decision_kind="media_placeholder",
        rationale="Text-only WhatsApp media occurrence; deterministic nonissue placeholder.",
    )


EXPECTED_OCCURRENCES: tuple[RestoreSpec, ...] = (
    _media_spec(217, "5d2a0e2eb83394b7651acb06c61af86349aa86f0a74c52bd442e5e0d522dfd~2", 2),
    RestoreSpec(
        250,
        "c9e35ede6782f24eade787ca2079b0ef9c3198ab58109990ebc15b74633e0a~2",
        2,
        "This message was deleted.",
        "27f65f114e9349e6a433e5c77adc3145b232335fac1b2cb8398b4f10c23a79a9",
        "manual_nonissue",
        "A WhatsApp deletion placeholder contains no recoverable building-condition evidence.",
        False,
    ),
    _media_spec(854, "2f61746a4f5bd14a7e14fd5648c1c67be2216c11a58d68ff1af38ec48c21c2~2", 2),
    _media_spec(1481, "4031cdb381e1781c2846c2ac35e670aef4f658f0b66425c32ae1cfcca34626~2", 2),
    _media_spec(1482, "4031cdb381e1781c2846c2ac35e670aef4f658f0b66425c32ae1cfcca34626~3", 3),
    _media_spec(1614, "fd69d5931e4fcda3babc4807790fccb82e2be2cd798452d3b7c31537c65dfe~2", 2),
    _media_spec(1615, "fd69d5931e4fcda3babc4807790fccb82e2be2cd798452d3b7c31537c65dfe~3", 3),
    RestoreSpec(
        1713,
        "15255e82d89c470ee924422272002fe7d6beb03e0f7f620bc197d6d171d633~2",
        2,
        "And I took a pic 🤭 image omitted",
        "6275d60a66ddf8b9ff73577a039d71c379f628c84609da5e148bf08dd1773575",
        "manual_nonissue",
        "Neighboring organizing/photo chatter was manually reviewed and is not a building issue.",
    ),
    _media_spec(1751, "ee3832877e29fb7226c2e2b578ab2b9a46ec16c7457d4ccf3fe031d5c694d3~2", 2),
    _media_spec(1752, "ee3832877e29fb7226c2e2b578ab2b9a46ec16c7457d4ccf3fe031d5c694d3~3", 3),
    _media_spec(1754, "d74dd5aa60f2332d0d8f01edcddee5f6591c22eb669a3d0ab0c567f0eb2cf2~2", 2),
    _media_spec(1755, "d74dd5aa60f2332d0d8f01edcddee5f6591c22eb669a3d0ab0c567f0eb2cf2~3", 3),
    _media_spec(1756, "d74dd5aa60f2332d0d8f01edcddee5f6591c22eb669a3d0ab0c567f0eb2cf2~4", 4),
    _media_spec(1757, "d74dd5aa60f2332d0d8f01edcddee5f6591c22eb669a3d0ab0c567f0eb2cf2~5", 5),
    _media_spec(1759, "1107505e477f93d851919f9d53f78968997d3ec9fc589d3844b03a6ea0d9ca~2", 2),
    _media_spec(1760, "1107505e477f93d851919f9d53f78968997d3ec9fc589d3844b03a6ea0d9ca~3", 3),
    _media_spec(2352, "4c21394400912b6bdd881b373e5f06508ddfebd8b77bb76fb8ba96097ccedd~2", 2),
    _media_spec(2353, "4c21394400912b6bdd881b373e5f06508ddfebd8b77bb76fb8ba96097ccedd~3", 3),
    _media_spec(2442, "fe8e4f16d63835aa3476eb33edde91913f348c7e31badf5d5758212ac0029d~2", 2),
    _media_spec(2714, "9193aee423404a58b4150fc32b57b67d57403291ccf3ae8751be00a1f74caa~2", 2),
)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_manifest() -> list[str]:
    errors: list[str] = []
    ids = [row.message_id for row in EXPECTED_OCCURRENCES]
    ordinals = [row.export_ordinal for row in EXPECTED_OCCURRENCES]
    if len(EXPECTED_OCCURRENCES) != EXPECTED_RESTORE_OCCURRENCES:
        errors.append(
            f"restore manifest must contain {EXPECTED_RESTORE_OCCURRENCES} rows, "
            f"found {len(EXPECTED_OCCURRENCES)}"
        )
    if len(ids) != len(set(ids)):
        errors.append("restore manifest contains duplicate message IDs")
    if len(ordinals) != len(set(ordinals)):
        errors.append("restore manifest contains duplicate export ordinals")
    for row in EXPECTED_OCCURRENCES:
        if len(row.message_id) != 64 or not row.message_id.endswith(f"~{row.physical_occurrence}"):
            errors.append(f"ordinal {row.export_ordinal}: malformed collision occurrence ID")
        if _sha256_bytes(row.text.encode("utf-8")) != row.text_sha256:
            errors.append(f"ordinal {row.export_ordinal}: manifest text digest mismatch")
        if row.decision_kind not in {"media_placeholder", "manual_nonissue"}:
            errors.append(f"ordinal {row.export_ordinal}: unsupported decision kind {row.decision_kind}")
    return errors


def _read_locked_archive(archive_path: Path) -> tuple[list[str], dict[str, object], dict[int, ExportMessage]]:
    errors = _validate_manifest()
    evidence: dict[str, object] = {
        "path": str(archive_path),
        "expected_sha256": EXPECTED_ARCHIVE_SHA256,
        "expected_chat_text_sha256": EXPECTED_CHAT_TEXT_SHA256,
    }
    if errors:
        return errors, evidence, {}
    try:
        observed_sha256 = _sha256_file(archive_path)
    except OSError as exc:
        return [f"archive read failed: {exc}"], evidence, {}
    evidence["observed_sha256"] = observed_sha256
    if not hmac.compare_digest(observed_sha256, EXPECTED_ARCHIVE_SHA256):
        return ["archive SHA-256 mismatch"], evidence, {}

    try:
        with zipfile.ZipFile(archive_path) as archive:
            chat_bytes = archive.read(EXPECTED_CHAT_ENTRY)
    except (OSError, KeyError, zipfile.BadZipFile) as exc:
        return [f"locked chat entry read failed: {exc}"], evidence, {}
    observed_chat_sha256 = _sha256_bytes(chat_bytes)
    evidence["observed_chat_text_sha256"] = observed_chat_sha256
    if not hmac.compare_digest(observed_chat_sha256, EXPECTED_CHAT_TEXT_SHA256):
        return ["inner chat text SHA-256 mismatch"], evidence, {}

    try:
        messages = iter_export_messages(archive_path, default_chat_name=EXPECTED_CHAT_NAME)
    except Exception as exc:
        return [f"archive parse failed: {exc}"], evidence, {}
    evidence["physical_messages"] = len(messages)
    if len(messages) != EXPECTED_PHYSICAL_MESSAGES:
        errors.append(
            f"physical message count mismatch: expected {EXPECTED_PHYSICAL_MESSAGES}, found {len(messages)}"
        )
    if len({row.message_id for row in messages}) != len(messages):
        errors.append("parsed physical message IDs are not unique")

    by_ordinal = {row.export_ordinal: row for row in messages}
    expected_ids = {row.message_id for row in EXPECTED_OCCURRENCES}
    collision_ids = {row.message_id for row in messages if row.physical_occurrence > 1}
    if collision_ids != expected_ids:
        errors.append(
            "collision follow-up set differs from the 20-row restore manifest: "
            f"missing={sorted(expected_ids - collision_ids)}, unexpected={sorted(collision_ids - expected_ids)}"
        )

    for spec in EXPECTED_OCCURRENCES:
        message = by_ordinal.get(spec.export_ordinal)
        if message is None:
            errors.append(f"ordinal {spec.export_ordinal}: absent from archive")
            continue
        actual = (
            message.message_id,
            message.physical_occurrence,
            message.text,
            _sha256_bytes(message.text.encode("utf-8")),
        )
        expected = (spec.message_id, spec.physical_occurrence, spec.text, spec.text_sha256)
        if actual != expected:
            errors.append(f"ordinal {spec.export_ordinal}: ID/occurrence/content mismatch")
        if spec.decision_kind == "media_placeholder" and not is_media_placeholder_text(message.text):
            errors.append(f"ordinal {spec.export_ordinal}: expected a pure media placeholder")
        if spec.decision_kind == "manual_nonissue" and is_media_placeholder_text(message.text):
            errors.append(f"ordinal {spec.export_ordinal}: manual review cannot be replaced by placeholder logic")
    return errors, evidence, by_ordinal


def _json_object(value: str | None) -> dict[str, object]:
    try:
        parsed = json.loads(value or "{}")
    except (json.JSONDecodeError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _attachments_are_compatible(raw: RawMessage, message: ExportMessage, spec: RestoreSpec) -> bool:
    if raw.attachments == message.attachments:
        return True
    if not spec.allow_media_enrichment:
        return False
    return bool(attachment_items(raw.attachments))


def _existing_pair_errors(
    raw: RawMessage,
    decision: MessageDecision,
    message: ExportMessage,
    spec: RestoreSpec,
) -> list[str]:
    errors: list[str] = []
    expected_raw = (
        message.chat_name,
        message.sender,
        sender_hash(message.sender),
        message.ts_epoch,
        message.text,
    )
    actual_raw = (raw.chat_name, raw.sender, raw.sender_hash, raw.ts_epoch, raw.text)
    if actual_raw != expected_raw:
        errors.append("stored raw identity/content differs from the locked archive")
    if parse_ts_to_epoch(raw.ts_iso) != message.ts_epoch:
        errors.append("stored display timestamp does not resolve to the locked archive epoch")
    if (raw.source or "").casefold() not in {"export", "export_media", "zip_import"}:
        errors.append(f"unexpected stored source {raw.source!r}")
    if not _attachments_are_compatible(raw, message, spec):
        errors.append("stored attachments differ from the archive without allowed media enrichment")
    decision_state = (
        bool(decision.is_issue),
        decision.incident_id,
        decision.category,
        decision.event_type,
        bool(decision.needs_review),
        bool(decision.auto_file_candidate),
    )
    if decision_state != (False, None, None, "non_issue", False, False):
        errors.append("decision is not the required unlinked, non-actionable nonissue")

    final = _json_object(decision.final_json)
    if final.get("is_issue") is not False or final.get("event_type") != "non_issue":
        errors.append("decision final_json is not the required nonissue state")
    if spec.decision_kind == "media_placeholder":
        rules = _json_object(decision.rules_json)
        if decision.chosen_source not in {"media_attachment", "media_placeholder_archive_restore"}:
            errors.append(f"unexpected media decision provenance {decision.chosen_source!r}")
        if rules.get("kind") not in {"media_attachment", "media_placeholder"}:
            errors.append("media decision lacks deterministic placeholder provenance")
    else:
        if decision.chosen_source != "review_codex_archive_occurrence_restore":
            errors.append(f"unexpected manual-review provenance {decision.chosen_source!r}")
        if (
            final.get("review_status") != "completed"
            or final.get("reviewed_by") != REVIEWED_BY
            or final.get("restore_id") != RESTORE_ID
        ):
            errors.append("manual nonissue lacks the exact completed-review provenance")
    return errors


def _decision_for(spec: RestoreSpec, *, created_at: str) -> MessageDecision:
    is_manual = spec.decision_kind == "manual_nonissue"
    chosen_source = (
        "review_codex_archive_occurrence_restore"
        if is_manual
        else "media_placeholder_archive_restore"
    )
    rules = {
        "kind": "manual_archive_review" if is_manual else "media_placeholder",
        "is_issue": False,
        "event_type": "non_issue",
    }
    final: dict[str, object] = {
        "is_issue": False,
        "category": None,
        "asset": None,
        "event_type": "non_issue",
        "needs_review": False,
        "auto_file_candidate": False,
        "restore_id": RESTORE_ID,
    }
    llm: dict[str, object] = {}
    if is_manual:
        final.update(
            {
                "review_status": "completed",
                "review_kind": "codex_archive_occurrence_restore",
                "reviewed_by": REVIEWED_BY,
                "reviewed_at": created_at,
                "review_rationale": spec.rationale,
            }
        )
        llm = {
            "review_status": "completed",
            "reviewed_by": REVIEWED_BY,
            "confidence": 100,
        }
    return MessageDecision(
        message_id=spec.message_id,
        created_at=created_at,
        chosen_source=chosen_source,
        is_issue=False,
        incident_id=None,
        category=None,
        event_type="non_issue",
        confidence=100 if is_manual else 95,
        needs_review=False,
        auto_file_candidate=False,
        rules_json=json.dumps(rules, sort_keys=True),
        llm_json=json.dumps(llm, sort_keys=True),
        final_json=json.dumps(final, sort_keys=True),
    )


def _protected_counts(session) -> dict[str, int]:
    return {
        "incidents": int(session.scalar(select(func.count()).select_from(Incident)) or 0),
        "filing_jobs": int(session.scalar(select(func.count()).select_from(FilingJob)) or 0),
        "service_request_cases": int(
            session.scalar(select(func.count()).select_from(ServiceRequestCase)) or 0
        ),
        "watchdog_actions": int(session.scalar(select(func.count()).select_from(WatchdogAction)) or 0),
    }


def _database_counts(session) -> dict[str, int]:
    return {
        "raw_messages": int(session.scalar(select(func.count()).select_from(RawMessage)) or 0),
        "message_decisions": int(
            session.scalar(select(func.count()).select_from(MessageDecision)) or 0
        ),
        **_protected_counts(session),
    }


def restore(*, archive_path: Path = DEFAULT_ARCHIVE, apply: bool = False) -> dict[str, object]:
    archive_errors, archive_evidence, messages_by_ordinal = _read_locked_archive(archive_path)
    result: dict[str, object] = {
        "ok": not archive_errors,
        "apply": apply,
        "applied": False,
        "changed": False,
        "restore_id": RESTORE_ID,
        "archive": archive_evidence,
        "expected_occurrences": len(EXPECTED_OCCURRENCES),
        "expected_media_placeholders": sum(
            row.decision_kind == "media_placeholder" for row in EXPECTED_OCCURRENCES
        ),
        "expected_manually_reviewed_nonissues": sum(
            row.decision_kind == "manual_nonissue" for row in EXPECTED_OCCURRENCES
        ),
        "to_restore": [],
        "already_restored": [],
        "restored_message_ids": [],
        "sheet_sync_invoked": False,
        "classifier_invoked": False,
        "errors": list(archive_errors),
    }
    if archive_errors:
        return result

    errors: list[str] = []
    to_restore: list[RestoreSpec] = []
    already_restored: list[str] = []
    with get_session() as session:
        counts_before = _database_counts(session)
        for spec in EXPECTED_OCCURRENCES:
            message = messages_by_ordinal[spec.export_ordinal]
            raw = session.get(RawMessage, spec.message_id)
            decision = session.get(MessageDecision, spec.message_id)
            if raw is None:
                if decision is not None:
                    errors.append(f"{spec.message_id}: orphan decision exists without raw message")
                else:
                    to_restore.append(spec)
                continue
            if decision is None:
                errors.append(f"{spec.message_id}: raw message exists without a decision")
                continue
            pair_errors = _existing_pair_errors(raw, decision, message, spec)
            errors.extend(f"{spec.message_id}: {error}" for error in pair_errors)
            if not pair_errors:
                already_restored.append(spec.message_id)

        result["to_restore"] = [row.message_id for row in to_restore]
        result["already_restored"] = already_restored
        if errors or not apply:
            session.rollback()
            result["errors"] = errors
            result["ok"] = not errors
            result["database_counts_before"] = counts_before
            result["database_counts_after"] = counts_before
            return result

        created_at = dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")
        for spec in to_restore:
            message = messages_by_ordinal[spec.export_ordinal]
            session.add(
                RawMessage(
                    message_id=spec.message_id,
                    chat_name=message.chat_name,
                    sender=message.sender,
                    sender_hash=sender_hash(message.sender),
                    ts_iso=message.ts_iso,
                    ts_epoch=message.ts_epoch,
                    text=message.text,
                    attachments=message.attachments,
                    source="zip_import",
                )
            )
        session.flush()
        for spec in to_restore:
            session.add(_decision_for(spec, created_at=created_at))
        session.flush()

        counts_after = _database_counts(session)
        expected_counts_after = {
            **counts_before,
            "raw_messages": counts_before["raw_messages"] + len(to_restore),
            "message_decisions": counts_before["message_decisions"] + len(to_restore),
        }
        if counts_after != expected_counts_after:
            errors.append(
                "database row-count delta differs from the exact restore plan: "
                f"expected={expected_counts_after}, observed={counts_after}"
            )
        for spec in to_restore:
            raw = session.get(RawMessage, spec.message_id)
            decision = session.get(MessageDecision, spec.message_id)
            if raw is None or decision is None:
                errors.append(f"{spec.message_id}: inserted pair missing before commit")
                continue
            errors.extend(
                f"{spec.message_id}: {error}"
                for error in _existing_pair_errors(
                    raw,
                    decision,
                    messages_by_ordinal[spec.export_ordinal],
                    spec,
                )
            )
        if errors:
            session.rollback()
            result["errors"] = errors
            result["ok"] = False
            result["database_counts_before"] = counts_before
            result["database_counts_after"] = counts_before
            return result
        session.commit()

    restored_ids = [row.message_id for row in to_restore]
    result.update(
        {
            "ok": True,
            "applied": True,
            "changed": bool(restored_ids),
            "restored_message_ids": restored_ids,
            "database_counts_before": counts_before,
            "database_counts_after": counts_after,
        }
    )
    if restored_ids:
        append_audit_event(
            "FULL_ARCHIVE_OCCURRENCE_RESTORE",
            None,
            {
                "restore_id": RESTORE_ID,
                "archive_sha256": EXPECTED_ARCHIVE_SHA256,
                "chat_text_sha256": EXPECTED_CHAT_TEXT_SHA256,
                "message_ids": restored_ids,
                "specs": [asdict(row) for row in to_restore],
                "classifier_invoked": False,
                "sheet_sync_invoked": False,
            },
        )
        daily_hash_chain()
    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Restore the hash-locked 2026-09-05 archive occurrence rows; default is read-only."
    )
    parser.add_argument("--archive", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Insert absent exact rows and deterministic nonissue decisions in one transaction.",
    )
    args = parser.parse_args()
    result = restore(archive_path=args.archive, apply=args.apply)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 1 if result["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
