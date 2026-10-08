"""Archive obsolete public tabs privately, then retire only verified copies.

Default: read-only plan. Run --archive before deploying the new projections.
Run --retire after normal sync and the public readback audit pass. Neither mode
changes sharing permissions or removes database records. Unknown tabs are never
deleted. Archive names include a content digest, so reruns are idempotent.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.local_env import load_local_env_file
from packages.sheets.schema import OPERATOR_HEADERS, PUBLIC_HEADERS, RETIRED_PUBLIC_DETAIL_TABS, WATCHDOG_TAB_ENV


def _title(name):
    env = "SHEETS_PUBLIC_UPDATES_TAB" if name == "Tenant Log" else WATCHDOG_TAB_ENV[name]
    return os.environ.get(env, name).strip() or name


def _metadata(service, spreadsheet_id):
    result = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields="sheets(properties(sheetId,title))",
    ).execute()
    return {sheet["properties"]["title"]: sheet["properties"]["sheetId"] for sheet in result["sheets"]}


def _values(service, spreadsheet_id, title):
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range="'" + title.replace("'", "''") + "'",
        valueRenderOption="FORMULA",
    ).execute()
    rows = [list(row) for row in result.get("values", [])]
    for row in rows:
        while row and row[-1] == "":
            row.pop()
    while rows and not rows[-1]:
        rows.pop()
    return rows


def archive_title(title, values):
    digest = hashlib.sha256(json.dumps(values, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()[:12]
    return f"Archive {title[:70]} {digest}"


def plan(service, operator_id, public_id):
    if not operator_id or not public_id or operator_id == public_id:
        raise ValueError("Migration requires distinct operator and public workbooks")
    public = _metadata(service, public_id)
    private = _metadata(service, operator_id)
    obsolete = {_title(name) for name in RETIRED_PUBLIC_DETAIL_TABS}
    protected = {_title(name) for name in PUBLIC_HEADERS}
    if obsolete & protected:
        raise ValueError("Public view names overlap obsolete detail tabs")
    result = []
    for title, sheet_id in public.items():
        if title not in protected and (title in obsolete or title.startswith("QA Draft ")):
            values = _values(service, public_id, title)
            archive = archive_title(title, values)
            verified = archive in private and _values(service, operator_id, archive) == values
            result.append({"title": title, "sheet_id": sheet_id, "archive": archive, "verified": verified})
    return result


def archive(service, operator_id, public_id):
    items = plan(service, operator_id, public_id)
    for item in items:
        if item["verified"]:
            continue
        existing = _metadata(service, operator_id)
        if item["archive"] in existing:
            raise RuntimeError("An archive with this name differs; refusing to overwrite it")
        copied = service.spreadsheets().sheets().copyTo(
            spreadsheetId=public_id, sheetId=item["sheet_id"],
            body={"destinationSpreadsheetId": operator_id},
        ).execute()
        service.spreadsheets().batchUpdate(spreadsheetId=operator_id, body={"requests": [
            {"updateSheetProperties": {"properties": {"sheetId": copied["sheetId"], "title": item["archive"]}, "fields": "title"}}
        ]}).execute()
    result = plan(service, operator_id, public_id)
    if not all(item["verified"] for item in result):
        raise RuntimeError("Archive readback failed or source changed; all public tabs retained")
    return result


def retire(service, operator_id, public_id, *, audit):
    items = plan(service, operator_id, public_id)
    if not all(item["verified"] for item in items):
        raise RuntimeError("Archive every obsolete tab and verify readback before retirement")
    # Full raw tables remain accessible to the operator, even after the public
    # workbook has reader projections with fewer columns.
    for name in WATCHDOG_TAB_ENV:
        values = _values(service, operator_id, _title(name))
        if not values or values[0] != OPERATOR_HEADERS[name]:
            raise RuntimeError(f"Missing complete operator table: {name}")
    if not audit().get("ok"):
        raise RuntimeError("Public readback audit did not pass; all public tabs retained")
    # Re-read after the audit so a source update cannot be deleted under an
    # archive of an earlier version.
    current = plan(service, operator_id, public_id)
    if current != items or not all(item["verified"] for item in current):
        raise RuntimeError("Public tabs changed during verification; retry after a fresh archive")
    if items:
        service.spreadsheets().batchUpdate(spreadsheetId=public_id, body={"requests": [
            {"deleteSheet": {"sheetId": item["sheet_id"]}} for item in items
        ]}).execute()
    remaining = _metadata(service, public_id)
    if any(item["title"] in remaining for item in items):
        raise RuntimeError("Public tab retirement was not confirmed")
    return items


def audit_all_views():
    """Require value-level readback of private detail and both public outputs."""
    from scripts.audit_public_watchdog_tabs import run_audit as audit_watchdog
    from scripts.audit_public_tenant_log import run_audit as audit_tenant_log
    checks = {
        "operator": audit_watchdog(audience="operator", retries=1, retry_sleep=0),
        "public_watchdog": audit_watchdog(audience="public", retries=1, retry_sleep=0),
        "tenant_log": audit_tenant_log(days=7, resync=False, retries=1, retry_sleep=0, limit=20),
    }
    return {"ok": all(result.get("ok") for result in checks.values()), "checks": checks}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--archive", action="store_true")
    mode.add_argument("--retire", action="store_true")
    args = parser.parse_args()
    load_local_env_file(ROOT / ".env")
    from packages.sheets import sync
    operator_id = os.environ.get("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()
    public_id = os.environ.get("GOOGLE_PUBLIC_SHEETS_SPREADSHEET_ID", "").strip()
    service = sync._service()
    if args.archive:
        result = archive(service, operator_id, public_id)
    elif args.retire:
        result = retire(service, operator_id, public_id, audit=audit_all_views)
    else:
        result = plan(service, operator_id, public_id)
    print(json.dumps({"mode": "archive" if args.archive else "retire" if args.retire else "read_only_plan", "tabs": result}, indent=2))


if __name__ == "__main__":
    main()
