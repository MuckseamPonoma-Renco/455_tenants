"""Initialize a Google Sheet with required tabs and headers."""
import argparse
import os
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
TABS = {
    "Incidents": [
        "incident_id", "category", "asset", "severity", "status", "start_ts", "end_ts", "duration_min",
        "title", "summary", "proof_refs",
        "evidence_preview", "evidence_1", "evidence_2", "evidence_3", "reply_context", "link_1", "link_2",
        "report_count", "witness_count", "confidence", "needs_review", "updated_at",
    ],
    "Dashboard": ["metric", "value"],
    "Coverage": ["day", "messages", "first_ts_epoch", "last_ts_epoch"],
    "Cases311": ["service_request_number", "incident_id", "source", "complaint_type", "status", "agency", "submitted_at", "last_checked_at", "closed_at", "resolution_description"],
    "Queue311": ["job_id", "incident_id", "state", "priority", "complaint_type", "form_target", "attempts", "created_at", "claimed_at", "completed_at", "notes"],
    "DecisionLog": [
        "message_ts", "decision_updated_at", "message_id", "source", "text", "chosen_source", "is_issue", "category", "event_type",
        "confidence", "needs_review", "incident_id", "auto_file_candidate",
        "media_preview", "media_1", "media_2", "media_3", "reply_context", "link_1", "link_2",
        "rules_json", "llm_json", "final_json",
    ],
    "Tenant Log": [
        "455 Tenants Log", "", "", "", "", "",
    ],
    "ElevatorWatch": [
        "What people need to know", "Current clear answer", "Why it matters",
        "Checked by", "Last checked", "Human needed", "Source",
    ],
    "ProjectStatus": [
        "section", "item", "status", "detail", "source", "updated_at",
    ],
    "PublicRecords": [
        "source_system", "record_type", "record_key", "verification_status", "machine_confidence",
        "verification_summary", "status", "status_detail", "filed_at", "approved_at", "permit_issued_at",
        "inspection_date", "expires_at", "needs_human_verification", "machine_verified_at",
        "human_verified_at", "human_verified_by", "source_url", "bbl", "bin", "job_number",
        "permit_number", "device_number",
    ],
    "WatchdogChecks": [
        "check_type", "status", "checked_at", "checked_by", "photo_url", "source_url", "notes",
    ],
    "ActionQueue": [
        "severity", "action_type", "title", "detail", "due_at", "owner_role", "status",
        "source_record_id", "related_incident_id", "draft_message", "created_at", "completed_at",
    ],
    "WeeklyDigest": [
        "period_start", "period_end", "tenant_update", "watchdog_status",
        "tenant_action_needed", "generated_at", "used_llm",
    ],
}

PRIVATE_ACCESS_NEEDS_TAB = {
    "AccessNeeds_Private": [
        "apartment_or_contact_hash", "need_type", "request_text", "management_response",
        "status", "due_at", "notes", "created_at", "updated_at",
    ]
}


PUBLIC_WATCHDOG_TABS = {
    key: TABS[key]
    for key in ("ElevatorWatch", "ProjectStatus", "PublicRecords", "WatchdogChecks", "ActionQueue", "WeeklyDigest")
}


def tabs_to_initialize():
    tabs = dict(TABS)
    if os.environ.get("ENABLE_PRIVATE_ACCESS_NEEDS_SHEET", "0").strip().lower() in {"1", "true", "yes", "on"}:
        tabs.update(PRIVATE_ACCESS_NEEDS_TAB)
    return tabs


def public_watchdog_tabs_to_initialize():
    return dict(PUBLIC_WATCHDOG_TABS)


def service():
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path or not os.path.exists(creds_path):
        raise SystemExit("GOOGLE_APPLICATION_CREDENTIALS not set or missing")
    creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def _service_account_email_from_creds_file():
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path or not os.path.exists(creds_path):
        return None
    try:
        import json
        with open(creds_path, "r", encoding="utf-8") as f:
            return json.load(f).get("client_email")
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser()
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--title")
    group.add_argument("--spreadsheet-id")
    ap.add_argument("--public-watchdog-tabs", action="store_true", help="Only add/update public replacement-watchdog tabs; preserve existing tabs.")
    args = ap.parse_args()

    svc = service()
    if args.title:
        try:
            spreadsheet = svc.spreadsheets().create(body={"properties": {"title": args.title}}).execute()
        except HttpError as exc:
            if getattr(exc, "resp", None) is not None and getattr(exc.resp, "status", None) == 403:
                email = _service_account_email_from_creds_file()
                print("ERROR: Sheets API create failed (403 PERMISSION_DENIED).")
                print("Create a Google Sheet manually and share it with the service account as Editor.")
                if email:
                    print("Service account:", email)
                raise SystemExit(1)
            raise
        sid = spreadsheet["spreadsheetId"]
        print("Created spreadsheetId:", sid)
    else:
        sid = args.spreadsheet_id
        spreadsheet = svc.spreadsheets().get(spreadsheetId=sid).execute()
        print("Using spreadsheetId:", sid)

    sheets = spreadsheet.get("sheets", [])
    titles = {sh["properties"]["title"]: sh["properties"]["sheetId"] for sh in sheets}
    requests = []
    if not args.public_watchdog_tabs and sheets and "Incidents" not in titles:
        requests.append({
            "updateSheetProperties": {
                "properties": {"sheetId": sheets[0]["properties"]["sheetId"], "title": "Incidents"},
                "fields": "title",
            }
        })
    tabs = public_watchdog_tabs_to_initialize() if args.public_watchdog_tabs else tabs_to_initialize()
    for tab in tabs:
        if tab != "Incidents" and tab not in titles:
            requests.append({"addSheet": {"properties": {"title": tab}}})
    if requests:
        svc.spreadsheets().batchUpdate(spreadsheetId=sid, body={"requests": requests}).execute()

    for tab, headers in tabs.items():
        svc.spreadsheets().values().update(
            spreadsheetId=sid,
            range=f"{tab}!A1",
            valueInputOption="RAW",
            body={"values": [headers]},
        ).execute()

    print("Share this sheet with your service-account email as Editor and set GOOGLE_SHEETS_SPREADSHEET_ID=", sid)


if __name__ == "__main__":
    main()
