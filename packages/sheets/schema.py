"""Workbook presentation contracts shared by initialization, sync and audits.

The operator workbook retains full diagnostics. Public tables are projections;
no underlying records or operator columns are removed when presentation changes.
"""

PUBLIC_WORKBOOK_TITLE = "455 Tenants Log"
PUBLIC_LOG_SECTION = "Public update log"
PUBLIC_LOG_HEADERS = ["Updated", "Issue", "Category", "311 follow-up", "Preview", "Open evidence", "Summary"]
PUBLIC_LOG_COLUMNS = len(PUBLIC_LOG_HEADERS)
WATCHDOG_TAB_ENV = {
    "ElevatorWatch": "SHEETS_ELEVATOR_WATCH_TAB",
    "ProjectStatus": "SHEETS_PROJECT_STATUS_TAB",
    "PublicRecords": "SHEETS_PUBLIC_RECORDS_TAB",
    "WatchdogChecks": "SHEETS_WATCHDOG_CHECKS_TAB",
    "ActionQueue": "SHEETS_WATCHDOG_ACTIONS_TAB",
    "WeeklyDigest": "SHEETS_WEEKLY_DIGEST_TAB",
}

OPERATOR_HEADERS = {'Incidents': ['incident_id',
               'category',
               'asset',
               'severity',
               'status',
               'start_ts',
               'end_ts',
               'duration_min',
               'title',
               'summary',
               'proof_refs',
               'evidence_preview',
               'evidence_1',
               'evidence_2',
               'evidence_3',
               'reply_context',
               'link_1',
               'link_2',
               'report_count',
               'witness_count',
               'confidence',
               'needs_review',
               'updated_at'],
 'Dashboard': ['metric', 'value'],
 'Coverage': ['day', 'messages', 'first_ts_epoch', 'last_ts_epoch'],
 'Cases311': ['service_request_number',
              'incident_id',
              'source',
              'complaint_type',
              'status',
              'agency',
              'submitted_at',
              'last_checked_at',
              'closed_at',
              'resolution_description'],
 'Queue311': ['job_id',
              'incident_id',
              'state',
              'priority',
              'complaint_type',
              'form_target',
              'attempts',
              'created_at',
              'claimed_at',
              'completed_at',
              'notes'],
 'DecisionLog': ['message_ts',
                 'decision_updated_at',
                 'message_id',
                 'source',
                 'text',
                 'chosen_source',
                 'is_issue',
                 'category',
                 'event_type',
                 'confidence',
                 'needs_review',
                 'incident_id',
                 'auto_file_candidate',
                 'media_preview',
                 'media_1',
                 'media_2',
                 'media_3',
                 'reply_context',
                 'link_1',
                 'link_2',
                 'rules_json',
                 'llm_json',
                 'final_json'],
 'ElevatorWatch': ['What people need to know',
                   'Current clear answer',
                   'Why it matters',
                   'Checked by',
                   'Last checked',
                   'Human needed',
                   'Source'],
 'ProjectStatus': ['section', 'item', 'status', 'detail', 'source', 'updated_at'],
 'PublicRecords': ['source_system',
                   'record_type',
                   'record_key',
                   'verification_status',
                   'machine_confidence',
                   'verification_summary',
                   'status',
                   'status_detail',
                   'filed_at',
                   'approved_at',
                   'permit_issued_at',
                   'inspection_date',
                   'expires_at',
                   'needs_human_verification',
                   'machine_verified_at',
                   'human_verified_at',
                   'human_verified_by',
                   'source_url',
                   'bbl',
                   'bin',
                   'job_number',
                   'permit_number',
                   'device_number'],
 'WatchdogChecks': ['check_type', 'status', 'checked_at', 'checked_by', 'photo_url', 'source_url', 'notes'],
 'ActionQueue': ['severity',
                 'action_type',
                 'title',
                 'detail',
                 'due_at',
                 'owner_role',
                 'status',
                 'source_record_id',
                 'related_incident_id',
                 'draft_message',
                 'created_at',
                 'completed_at'],
 'WeeklyDigest': ['period_start',
                  'period_end',
                  'tenant_update',
                  'watchdog_status',
                  'tenant_action_needed',
                  'generated_at',
                  'used_llm']}

OPERATOR_WATCHDOG_HEADERS = {name: OPERATOR_HEADERS[name] for name in WATCHDOG_TAB_ENV}
PUBLIC_WATCHDOG_HEADERS = {
    "ElevatorWatch": ["Topic", "Current answer", "Details", "Checked by", "Last checked", "Action needed", "Source"],
    "PublicRecords": ["Record", "Status", "Details", "Dates", "Verification", "Identifiers", "Source"],
    "WeeklyDigest": ["Period start", "Period end", "Tenant update", "Actions at that time", "Generated"],
}
PUBLIC_HEADERS = {
    "Tenant Log": [PUBLIC_WORKBOOK_TITLE] + [""] * (PUBLIC_LOG_COLUMNS - 1),
    **PUBLIC_WATCHDOG_HEADERS,
}
RETIRED_PUBLIC_DETAIL_TABS = ("ProjectStatus", "WatchdogChecks", "ActionQueue")
PUBLIC_COLUMN_WIDTHS = {
    "ElevatorWatch": (210, 235, 360, 155, 155, 240, 200),
    "PublicRecords": (165, 145, 360, 205, 250, 190, 200),
    "WeeklyDigest": (125, 125, 500, 350, 165),
}
RECORD_LABELS = {
    "elevator_permit_application": "Elevator permit application",
    "electrical_permit_application": "Electrical permit application",
    "elevator_device_detail": "Elevator registration",
    "elevator_safety_compliance": "Elevator safety inspection",
    "dob_complaint": "Building complaint",
    "dob_violation": "Building violation",
    "dob_ecb_violation": "Building violation and summons",
    "oath_hearing_case": "Administrative hearing",
    "nyc_311_service_request": "311 service request",
    "hpd_building": "Housing registration",
    "hpd_registration_contact": "Registered building contact",
    "hpd_violation": "Housing violation",
}
SOURCE_LABELS = {
    "dob_now_elevator_applications": "DOB NOW",
    "dob_now_electrical_applications": "DOB NOW",
    "dob_now_elevator_device_details": "DOB NOW",
    "dob_now_elevator_safety_compliance": "DOB NOW",
    "dob_complaints": "NYC Department of Buildings",
    "dob_violations": "NYC Department of Buildings",
    "dob_ecb_violations": "NYC Department of Buildings",
    "oath_hearings": "NYC Office of Administrative Trials and Hearings",
    "nyc_311": "NYC 311",
    "hpd_building": "NYC Housing Preservation and Development",
    "hpd_registration_contacts": "NYC Housing Preservation and Development",
    "hpd_violations": "NYC Housing Preservation and Development",
}
VERIFICATION_LABELS = {
    "official_corroborated": "Matched across official records",
    "official_elevator_match": "Matched to building elevators",
    "official_building_match": "Matched to building",
    "accepted": "Matched to official records",
    "needs_review": "Needs review",
    "official_conflict": "Building match conflicts; needs review",
}


def _reader_label(value: object) -> str:
    text = str(value or "").strip().replace("_", " ")
    if text.startswith("LAST OBSERVED:"):
        return "Last observed: " + _reader_label(text.partition(":")[2])
    return text.capitalize() if text.islower() or text.isupper() else text


def public_log_values(values: list[list[object]]) -> list[list[object]]:
    """Remove obsolete blank layout columns, refusing to discard any content."""
    for row in values:
        if any(cell not in ("", None) for cell in row[PUBLIC_LOG_COLUMNS:]):
            raise ValueError("Tenant Log contains content beyond its declared public columns")
    return [(list(row) + [""] * PUBLIC_LOG_COLUMNS)[:PUBLIC_LOG_COLUMNS] for row in values]


def public_record_values(raw: list[object]) -> list[object]:
    """Project a complete trusted record without dropping status, dates or IDs."""
    headers = OPERATOR_HEADERS["PublicRecords"]
    record = dict(zip(headers, list(raw) + [""] * len(headers)))
    dates = {"filed_at": "Filed", "approved_at": "Approved", "permit_issued_at": "Permit issued",
             "inspection_date": "Inspected", "expires_at": "Expires"}
    identifiers = {"record_key": "Record", "bbl": "Tax lot (BBL)", "bin": "Building (BIN)",
                   "job_number": "Job", "permit_number": "Permit", "device_number": "Elevator"}
    verification = VERIFICATION_LABELS.get(str(record["verification_status"]), "Needs review")
    if record["needs_human_verification"] and "needs review" not in verification.lower():
        verification += "\nReview requested"
    return [
        RECORD_LABELS.get(str(record["record_type"]), _reader_label(record["record_type"])) + "\n" + SOURCE_LABELS.get(str(record["source_system"]), _reader_label(record["source_system"])),
        _reader_label(record["status"]),
        record["status_detail"],
        "\n".join(f"{label}: {record[key]}" for key, label in dates.items() if record[key]),
        "\n".join(str(value) for value in (
            verification,
            f"Checked: {record['machine_verified_at']}" if record["machine_verified_at"] else "",
            f"Human confirmation: {record['human_verified_at']}" if record["human_verified_at"] else "",
        ) if value),
        "\n".join(f"{label}: {record[key]}" for key, label in identifiers.items() if record[key]),
        record["source_url"],
    ]


def public_digest_values(raw: list[object]) -> list[object]:
    return [raw[0], raw[1], raw[2], str(raw[4]).replace("see current ActionQueue", "see current ElevatorWatch"), raw[5]]


def public_watchdog_values(
    elevator: list[list[object]], project: list[list[object]],
    checks: list[list[object]], actions: list[list[object]],
) -> list[list[object]]:
    """Consolidate the existing views without losing timelines, checks or tasks.

Only automatic-check rows use Last checked. Historical dates stay with their
source details so a past management claim cannot resemble a fresh verification.
    """
    values = [list(PUBLIC_WATCHDOG_HEADERS["ElevatorWatch"])] + [list(row) for row in elevator[1:]]
    values.append(["Project and timeline", "", "", "", "", "", ""])
    for raw in project[1:]:
        section, item, status, detail, source, updated = raw
        if section == "summary":
            continue  # The existing answer rows above already cover these counters.
        topic = f"{str(section).replace('_', ' ').capitalize()}: {str(item).replace('_', ' ')}"
        context = str(detail or "")
        if updated:
            context += f"\nRecorded: {updated}"
        values.append([topic, status, context, "", "", "", source])
    values.append(["Resident checks", "", "", "", "", "", ""])
    for raw in checks[1:]:
        check_type, status, checked_at, checked_by, photo, source, notes = raw
        # Free-text notes and the observer's identity remain in the operator
        # history. The public record retains the condition, date and evidence.
        detail = f"Checked: {checked_at}" if checked_at else ""
        values.append([str(check_type).replace("_", " "), status, detail, "Resident report", "", "", "\n".join(str(part) for part in (photo, source) if part)])
    if len(checks) == 1:
        values.append(["No resident checks recorded", "", "", "", "", "", ""])
    values.append(["Tenant actions", "", "", "", "", "", ""])
    for raw in actions[1:]:
        severity, action_type, title, detail, due, owner, status, record_id, incident_id, draft, created, completed = raw
        context = "\n".join(str(part) for part in (detail, f"Suggested message: {draft}" if draft else "",
            f"Created: {created}" if created else "", f"Completed: {completed}" if completed else "") if part)
        owner_label = {"resident": "Resident", "tenant_association": "Tenant association"}.get(str(owner), "")
        action = "\n".join(str(part) for part in (owner_label, f"Due: {due}" if due else "") if part)
        values.append([title, " / ".join(str(part) for part in (status, severity) if part), context, "", "", action, ""])
    return values
