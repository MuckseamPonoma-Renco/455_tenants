"""Initialize a Google Sheet with required tabs and headers."""
import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from packages.sheets.schema import OPERATOR_HEADERS, PUBLIC_HEADERS, PUBLIC_WATCHDOG_HEADERS
from packages.local_env import load_local_env_file

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
TABS = OPERATOR_HEADERS

PRIVATE_ACCESS_NEEDS_TAB = {
    "AccessNeeds_Private": [
        "apartment_or_contact_hash", "need_type", "request_text", "management_response",
        "status", "due_at", "notes", "created_at", "updated_at",
    ]
}


PUBLIC_WATCHDOG_TABS = PUBLIC_WATCHDOG_HEADERS


def tabs_to_initialize():
    tabs = dict(TABS)
    if os.environ.get("ENABLE_PRIVATE_ACCESS_NEEDS_SHEET", "0").strip().lower() in {"1", "true", "yes", "on"}:
        tabs.update(PRIVATE_ACCESS_NEEDS_TAB)
    return tabs


def public_watchdog_tabs_to_initialize():
    return dict(PUBLIC_WATCHDOG_TABS)


def public_tabs_to_initialize():
    return dict(PUBLIC_HEADERS)


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
    ap.add_argument("--public-tabs", action="store_true", help="Initialize only the four resident-facing tabs in a dedicated public workbook.")
    ap.add_argument("--public-watchdog-tabs", action="store_true", help="Only add/update public replacement-watchdog tabs; preserve existing tabs.")
    args = ap.parse_args()
    load_local_env_file(ROOT / ".env")

    if args.public_tabs and args.public_watchdog_tabs:
        ap.error("Choose --public-tabs or --public-watchdog-tabs")
    public_mode = args.public_tabs or args.public_watchdog_tabs
    internal_id = os.environ.get("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()
    public_id = os.environ.get("GOOGLE_PUBLIC_SHEETS_SPREADSHEET_ID", "").strip()
    if public_mode and not internal_id:
        ap.error("Configure the private GOOGLE_SHEETS_SPREADSHEET_ID before initializing a public workbook")
    if args.spreadsheet_id and ((public_mode and args.spreadsheet_id == internal_id) or (not public_mode and args.spreadsheet_id == public_id)):
        ap.error("Public and operator workbook destinations must be distinct")

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
    if not public_mode and sheets and "Incidents" not in titles:
        requests.append({
            "updateSheetProperties": {
                "properties": {"sheetId": sheets[0]["properties"]["sheetId"], "title": "Incidents"},
                "fields": "title",
            }
        })
    tabs = public_tabs_to_initialize() if args.public_tabs else (public_watchdog_tabs_to_initialize() if args.public_watchdog_tabs else tabs_to_initialize())
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

    setting = "GOOGLE_PUBLIC_SHEETS_SPREADSHEET_ID" if public_mode else "GOOGLE_SHEETS_SPREADSHEET_ID"
    print(f"Share this sheet with your service-account email as Editor and set {setting}=", sid)


if __name__ == "__main__":
    main()
