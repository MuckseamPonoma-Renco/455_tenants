from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.local_env import load_local_env_file

load_local_env_file(ROOT / ".env")

from packages.audit import compute_message_id  # noqa: E402
from packages.db import MessageDecision, RawMessage, get_session  # noqa: E402
from packages.incident.rules import classify_rules  # noqa: E402
from packages.tasker_capture import LIVE_CAPTURE_SOURCES, find_recent_cross_source_duplicate, find_recent_duplicate  # noqa: E402
from packages.timeutil import parse_ts_to_epoch  # noqa: E402
from packages.whatsapp.export import parse_export_path  # noqa: E402

DEFAULT_SINCE = "2026-06-05"
EXPORT_EXTENSIONS = {".zip", ".txt"}

CSV_FIELDS = [
    "export_ordinal",
    "export_file",
    "export_chat_name",
    "ts_iso",
    "ts_epoch",
    "sender",
    "text",
    "matched_message_id",
    "match_method",
    "stored_source",
    "cross_source_message_id",
    "cross_source_source",
    "incident_id",
    "chosen_source",
    "is_issue",
    "category",
    "asset",
    "event_type",
    "confidence",
    "needs_review",
    "rule_kind",
    "rule_category",
    "rule_asset",
    "rule_event_type",
    "suspect_reasons",
    "expected_is_issue",
    "expected_category",
    "expected_asset",
    "expected_event_type",
    "correction_notes",
]


@dataclass(frozen=True)
class ExportMessage:
    export_ordinal: int
    export_file: str
    chat_name: str
    sender: str
    ts_iso: str | None
    ts_epoch: int | None
    text: str
    attachments: str | None


def iter_export_messages(export_path: Path, *, default_chat_name: str = "Tenants WhatsApp") -> list[ExportMessage]:
    messages: list[ExportMessage] = []
    export = parse_export_path(export_path, default_chat_name=default_chat_name)
    fallback_file = Path(export_path).name
    chat_files = export.chat_files or [fallback_file]
    for ordinal, item in enumerate(export.messages, start=1):
        messages.append(
            ExportMessage(
                export_ordinal=ordinal,
                export_file=chat_files[0] if len(chat_files) == 1 else item.chat_name,
                chat_name=item.chat_name,
                sender=item.sender,
                ts_iso=item.ts_iso,
                ts_epoch=parse_ts_to_epoch(item.ts_iso),
                text=item.text,
                attachments=item.attachments,
            )
        )
    return messages


def _safe_json(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _decision_row(decision: MessageDecision | None) -> dict[str, Any]:
    if decision is None:
        return {
            "incident_id": "",
            "chosen_source": "",
            "is_issue": "",
            "category": "",
            "asset": "",
            "event_type": "",
            "confidence": "",
            "needs_review": "",
        }
    final = _safe_json(decision.final_json)
    return {
        "incident_id": decision.incident_id or "",
        "chosen_source": decision.chosen_source or "",
        "is_issue": bool(decision.is_issue),
        "category": decision.category or "",
        "asset": final.get("asset") or "",
        "event_type": decision.event_type or final.get("event_type") or "",
        "confidence": int(decision.confidence or 0),
        "needs_review": bool(decision.needs_review),
    }


def _suspect_reasons(
    *,
    raw: RawMessage | None,
    decision: MessageDecision | None,
    rules: dict[str, Any],
    decision_fields: dict[str, Any],
) -> list[str]:
    reasons: list[str] = []
    if raw is None:
        return ["no_db_match"]
    if decision is None:
        return ["no_decision"]
    chosen_source = str(decision_fields.get("chosen_source") or "")
    if decision.needs_review:
        reasons.append("needs_review")
    if chosen_source.startswith("guardrail"):
        reasons.append(chosen_source)
    if chosen_source in {"hybrid_disagreement", "rules_with_llm_disagreement", "review"}:
        reasons.append(chosen_source)
    if rules.get("is_issue") and not decision.is_issue:
        reasons.append("rules_issue_but_final_nonissue")
    if (
        chosen_source != "rules_context"
        and not rules.get("is_issue")
        and decision.is_issue
        and int(decision.confidence or 0) < 80
    ):
        reasons.append("llm_only_low_confidence_issue")
    if decision.is_issue and (decision.category or "") == "other":
        reasons.append("other_issue_category")
    return sorted(set(reasons))


def _decision_signature(decision: MessageDecision | None) -> tuple[object, ...]:
    if decision is None:
        return (None, "", "", False)
    return (
        bool(decision.is_issue),
        decision.category or "",
        decision.event_type or "",
        bool(decision.needs_review),
    )


def _match_export_message(session, message: ExportMessage) -> tuple[RawMessage | None, str, RawMessage | None]:
    message_id = compute_message_id(message.chat_name, message.sender, message.ts_iso or "", message.text)
    exact = session.get(RawMessage, message_id)
    cross_source = find_recent_cross_source_duplicate(
        session,
        text=message.text,
        ts_epoch=message.ts_epoch,
        sources=LIVE_CAPTURE_SOURCES,
    )
    if cross_source is not None and cross_source.message_id != message_id:
        if exact is None or exact.source in {"zip_import", "export"}:
            return cross_source, "cross_source", exact
    if exact is not None:
        return exact, "exact", None

    duplicate = find_recent_duplicate(
        session,
        chat_name=message.chat_name,
        sender=message.sender,
        text=message.text,
        ts_epoch=message.ts_epoch,
        require_chat_match=False,
    )
    if duplicate is not None:
        return duplicate, "near_duplicate", None

    return None, "", None


def _row_from_message(session, message: ExportMessage) -> tuple[dict[str, Any], list[str]]:
    raw, match_method, alternate = _match_export_message(session, message)
    decision = session.get(MessageDecision, raw.message_id) if raw else None
    rules = classify_rules(message.text)
    decision_fields = _decision_row(decision)
    reasons = _suspect_reasons(raw=raw, decision=decision, rules=rules, decision_fields=decision_fields)
    alternate_decision = session.get(MessageDecision, alternate.message_id) if alternate else None
    if alternate is not None and _decision_signature(decision) != _decision_signature(alternate_decision):
        reasons.append("cross_source_decision_conflict")
    reasons = sorted(set(reasons))
    row = {
        "export_ordinal": message.export_ordinal,
        "export_file": message.export_file,
        "export_chat_name": message.chat_name,
        "ts_iso": message.ts_iso or "",
        "ts_epoch": message.ts_epoch or "",
        "sender": message.sender,
        "text": message.text,
        "matched_message_id": raw.message_id if raw else "",
        "match_method": match_method,
        "stored_source": raw.source if raw else "",
        "cross_source_message_id": alternate.message_id if alternate else "",
        "cross_source_source": alternate.source if alternate else "",
        "rule_kind": rules.get("kind") or "",
        "rule_category": rules.get("category") or "",
        "rule_asset": rules.get("asset") or "",
        "rule_event_type": rules.get("event_type") or "",
        "suspect_reasons": ";".join(reasons),
        "expected_is_issue": "",
        "expected_category": "",
        "expected_asset": "",
        "expected_event_type": "",
        "correction_notes": "",
        **decision_fields,
    }
    return row, reasons


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _write_summary(path: Path, *, summary: dict[str, Any], outputs: dict[str, Path]) -> None:
    lines = [
        "# WhatsApp Export Decision Audit",
        "",
        f"- Export: `{summary['export_path']}`",
        f"- Since: `{summary['since']}`",
        f"- Parsed messages: {summary['parsed_messages']}",
        f"- Audited messages: {summary['audited_messages']}",
        f"- Matched messages: {summary['matched_messages']}",
        f"- Missing from DB: {summary['missing_db_messages']}",
        f"- Missing decisions: {summary['missing_decisions']}",
        f"- Review roster rows: {summary['review_roster_rows']}",
        "",
        "## Outputs",
        "",
    ]
    for label, output_path in outputs.items():
        lines.append(f"- {label}: `{output_path}`")
    lines.extend(
        [
            "",
            "## Next",
            "",
            "Fill the `expected_*` and `correction_notes` columns only for rows that are wrong, then ask Codex to turn that roster into rules/tests and reprocess the affected messages.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_audit(
    export_path: Path,
    *,
    since: str = DEFAULT_SINCE,
    out_dir: Path | None = None,
    default_chat_name: str = "Tenants WhatsApp",
) -> dict[str, Any]:
    export_path = Path(export_path).expanduser().resolve()
    since_epoch = parse_ts_to_epoch(since)
    if since_epoch is None:
        raise SystemExit(f"Unable to parse --since value: {since}")

    messages = iter_export_messages(export_path, default_chat_name=default_chat_name)
    audited = [message for message in messages if message.ts_epoch is None or int(message.ts_epoch) >= int(since_epoch)]

    if out_dir is None:
        stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
        out_dir = ROOT / "exports" / "message_decision_audits" / stamp
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    all_rows: list[dict[str, Any]] = []
    review_rows: list[dict[str, Any]] = []
    counters: Counter[str] = Counter()

    with get_session() as session:
        for message in audited:
            row, reasons = _row_from_message(session, message)
            all_rows.append(row)
            counters["matched_messages"] += bool(row["matched_message_id"])
            counters["missing_db_messages"] += "no_db_match" in reasons
            counters["missing_decisions"] += "no_decision" in reasons
            if reasons:
                review_rows.append(row)
                for reason in reasons:
                    counters[f"reason:{reason}"] += 1

    all_messages_csv = out_dir / "all_messages.csv"
    review_roster_csv = out_dir / "review_roster.csv"
    summary_json = out_dir / "summary.json"
    summary_md = out_dir / "summary.md"

    _write_csv(all_messages_csv, all_rows)
    _write_csv(review_roster_csv, review_rows)

    summary: dict[str, Any] = {
        "ok": True,
        "export_path": str(export_path),
        "since": since,
        "parsed_messages": len(messages),
        "audited_messages": len(audited),
        "matched_messages": counters["matched_messages"],
        "missing_db_messages": counters["missing_db_messages"],
        "missing_decisions": counters["missing_decisions"],
        "review_roster_rows": len(review_rows),
        "reason_counts": {key.removeprefix("reason:"): value for key, value in counters.items() if key.startswith("reason:")},
        "out_dir": str(out_dir),
        "all_messages_csv": str(all_messages_csv),
        "review_roster_csv": str(review_roster_csv),
        "summary_md": str(summary_md),
    }
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_summary(
        summary_md,
        summary=summary,
        outputs={
            "all messages": all_messages_csv,
            "review roster": review_roster_csv,
            "summary JSON": summary_json,
        },
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit stored message decisions against a WhatsApp export.")
    parser.add_argument("--export", required=True, help="WhatsApp .zip or .txt export to audit")
    parser.add_argument("--since", default=DEFAULT_SINCE, help=f"Only audit messages at/after this timestamp. Default: {DEFAULT_SINCE}")
    parser.add_argument("--out-dir", help="Output directory for CSV/JSON/Markdown audit artifacts")
    parser.add_argument("--default-chat-name", default="Tenants WhatsApp", help="Chat name to use for _chat.txt exports")
    args = parser.parse_args()

    summary = run_audit(
        Path(args.export),
        since=args.since,
        out_dir=Path(args.out_dir) if args.out_dir else None,
        default_chat_name=args.default_chat_name,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
