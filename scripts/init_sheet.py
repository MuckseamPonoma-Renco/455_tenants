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
        "title", "summary", "proof_refs", "report_count", "witness_count", "confidence", "needs_review", "updated_at",
    ],
    "Dashboard": ["metric", "value"],
    "Coverage": ["day", "messages", "first_ts_epoch", "last_ts_epoch"],
    "Cases311": ["service_request_number", "incident_id", "source", "complaint_type", "status", "agency", "submitted_at", "last_checked_at", "closed_at", "resolution_description"],
    "Queue311": ["job_id", "incident_id", "state", "priority", "complaint_type", "form_target", "attempts", "created_at", "claimed_at", "completed_at", "notes"],
}


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
    if sheets and "Incidents" not in titles:
        requests.append({
            "updateSheetProperties": {
                "properties": {"sheetId": sheets[0]["properties"]["sheetId"], "title": "Incidents"},
                "fields": "title",
            }
        })
    for tab in TABS:
        if tab != "Incidents" and tab not in titles:
            requests.append({"addSheet": {"properties": {"title": tab}}})
    if requests:
        svc.spreadsheets().batchUpdate(spreadsheetId=sid, body={"requests": requests}).execute()

    for tab, headers in TABS.items():
        svc.spreadsheets().values().update(
            spreadsheetId=sid,
            range=f"{tab}!A1",
            valueInputOption="RAW",
            body={"values": [headers]},
        ).execute()

    print("Share this sheet with your service-account email as Editor and set GOOGLE_SHEETS_SPREADSHEET_ID=", sid)


if __name__ == "__main__":
    main()
