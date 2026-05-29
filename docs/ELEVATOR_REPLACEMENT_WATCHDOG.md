# Elevator Replacement Watchdog

This module adds a sheet-first tracking layer for the 455 Ocean Parkway elevator replacement. It does not replace the WhatsApp, 311, outage, or public tenant-log flow.

## Streams

- Management claims: the project schedule and contacts stated by management.
- Official records: NYC Open Data rows imported from DOB/NYC sources. Rows are labeled as `official_*` machine matches when the building identifiers and record keys are strong enough; weaker or conflicting rows stay `needs_human_verification`.
- Tenant reality: existing incident, 311, and report-form records from residents.

## Sheet Tabs

- `ElevatorWatch`: first public-facing view. Plain-language answers only: whether a current replacement permit exists, whether an active official elevator violation exists, what tenant reports say, and exactly when a resident photo/check is needed.
- `ProjectStatus`: management timeline, current bottleneck, next expected official record.
- `PublicRecords`: imported NYC/DOB/HPD/311 rows with `verification_status`, `machine_confidence`, `verification_summary`, and any remaining `needs_human_verification` warning.
- `WatchdogChecks`: volunteer checks for posted permits, TPP, SCBR, emergency contacts, work hours, interruption notices, barricades, and elevator notices.
- `ActionQueue`: deterministic next actions from public-record changes and tenant-observed outages.
- `WeeklyDigest`: weekly tenant and management drafts.
- `AccessNeeds_Private`: optional private tab only when `ENABLE_PRIVATE_ACCESS_NEEDS_SHEET=1`. Do not publish it.

## Volunteer Workflow

1. Run `POST /admin/sync_public_records`.
2. Start with `ElevatorWatch`; this is the public view residents should read.
3. Treat the first imported rows for each source as baseline history unless `ElevatorWatch` or `ActionQueue` flags a current risk.
4. Do not ask residents to check DOB/ECB/BIS when the system already imported the official record.
5. Review only rows whose `verification_status` is `needs_review` or `official_conflict`, plus any high-severity `ActionQueue` item.
6. Machine-accepted rows are not called human-verified. They are marked with `machine_verified_at`, `machine_confidence`, and an official-source reason.
7. If a volunteer is available, open the record source link only for weak/conflicting records, then mark human-verified with `POST /admin/verify_public_record/{id}`.
8. Add physical field checks with `POST /admin/add_watchdog_check`.

## No-Volunteer Automation Policy

When nobody can manually verify records, the watchdog uses only official NYC/DOB/Open Data identifiers:

- Exact `BBL`, `BIN`, block/lot, or address match establishes the record belongs to 455 Ocean Parkway.
- Elevator-specific datasets, `unit=ELEVR`, DOB elevator violation codes, elevator complaint types, or device numbers raise confidence.
- Cross-source joins by DOB NOW job filing number, elevator device number, permit number, ECB ticket, or OATH ticket raise confidence further.
- Records at or above `PUBLIC_RECORD_AUTO_VERIFY_MIN_CONFIDENCE` default `80` are machine-accepted and no longer create a volunteer verification task.
- Conflicts or weak matches remain visible but flagged as `needs_review`; the system does not silently turn them into verified facts.

## Portal Cadence

- Open Data sync: every `AUTOMATION_PUBLIC_RECORD_SYNC_SECONDS` seconds when `scripts/run_automation_daemon.py` is running. Default: 21600 seconds, or every 6 hours.
- DOB/ECB/BIS manual check: only for weak/conflicting machine matches or when the public source cannot be machine-read.
- Lobby posting check: only when `ElevatorWatch` says a current permit-issued signal exists and a resident photo/check is needed.

## Escalation Rules

- No public filing after 30 days: ask management for filing date/status.
- New public record: auto-verification first; only weak/conflicting records require volunteer review.
- First-source import: baseline only; do not create one action per historical row.
- Objection or hold: ask management for correction/resubmission date.
- Approved but no permit: mark not construction-ready.
- Permit issued: ask for one resident photo/check of lobby postings and start-date notice because the system cannot see the hallway.
- One elevator down during replacement watch: yellow or critical based on duration.
- Both elevators down: file 311, notify management, prepare escalation packet.
- No public movement for 14 days: ask management for update.
- Permit expiring within 30 days: ask for renewal/extension plan.

## Privacy Rules

- Access and mobility needs never go to public endpoints or public sheet sync.
- Automatically imported public records are either machine-accepted official-source matches or review-needed rows. Human verification remains separate and is only shown when a person confirms the record.
- Public updates should separate management claims, official records, and tenant-observed conditions.

## Management Email Template

Subject: 455 Ocean Parkway elevator replacement status request

Please share the current DOB filing number, permit status, expected start date, and required posting plan for the full elevator replacement at 455 Ocean Parkway. Tenants are tracking management claims, official public records, and observed elevator service separately so updates can stay accurate.

## Tenant WhatsApp Update Template

Elevator watch update: the system checks DOB/NYC records automatically. Residents do not need to search DOB or ECB manually. Please report real elevator outages when they happen, and send a clear lobby/hallway notice photo only when the public `ElevatorWatch` view asks for it.
