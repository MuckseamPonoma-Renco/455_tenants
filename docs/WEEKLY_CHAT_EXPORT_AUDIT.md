# Weekly Chat Export Audit

This is the batch path for refreshing the app's durable chat context and checking whether WhatsApp messages since the last audit were identified correctly.

## Best Upload Path

Use an iCloud Drive folder or a cloud-drive folder that syncs to this Mac, then point Codex at the local path.

Recommended local inbox:

```bash
mkdir -p incoming/chat_exports
```

Recommended iCloud inbox:

```bash
mkdir -p "$HOME/Library/Mobile Documents/com~apple~CloudDocs/455 Tenant Chat Exports"
```

From iPhone, save the WhatsApp export ZIP into `iCloud Drive/455 Tenant Chat Exports`. The Mac puller copies the newest file from there into `incoming/chat_exports/`. It also recognizes a root-level iCloud Drive file named like `WhatsApp Chat - ...zip`, so a normal Share Sheet save cannot silently miss the inbox folder.

For a one-off Codex run, it is also fine to give Codex a private Dropbox/Google Drive/iCloud download link, but a local synced file is more reliable than browser upload for a 196 MB ZIP.

## Run The Weekly Audit

Default cutoff is `2026-06-05`, the day of the latest public Tenant Log/watchdog audit in this repo history.

```bash
.venv/bin/python scripts/run_weekly_chat_export_audit.py
```

To use a specific file:

```bash
.venv/bin/python scripts/run_weekly_chat_export_audit.py \
  --export "incoming/chat_exports/WhatsApp Chat - 455 Tenants.zip" \
  --since 2026-06-05
```

To audit without importing first:

```bash
.venv/bin/python scripts/run_weekly_chat_export_audit.py --skip-import
```

## Automatic Mac Pull

The Mac-side puller watches the dedicated iCloud inbox and scans both that folder and the iCloud Drive root on its regular run. Manual repo runs stage into `incoming/chat_exports/`; the installed LaunchAgent stages into `$HOME/.local/share/tenant-issue-os/runtime/incoming/chat_exports/`. It skips an export if that exact file was already processed, imports new messages, audits decisions since the cutoff, and resyncs Sheets through the normal app path. It only records an export as processed after the staged archive is valid and its audit parses at least one chat message.

Install the LaunchAgent:

```bash
./scripts/install_chat_export_sync_launch_agent.sh
```

Default schedule is every 15 minutes, once when the agent loads, and immediately when the dedicated iCloud inbox changes. Root-level Share Sheet saves are picked up on the next scheduled scan, avoiding a high-churn watch over all of iCloud Drive. That gives a short catch-up path after the Mac wakes, while the unchanged-file state prevents repeated imports.

Run the same pull manually:

```bash
.venv/bin/python scripts/sync_chat_export_inbox.py
```

Check logs:

```bash
tail -n 80 "$HOME/.local/var/log/tenant-issue-os/chat-export-sync.out.log"
tail -n 80 "$HOME/.local/var/log/tenant-issue-os/chat-export-sync.err.log"
```

The script first imports the export into the app database, dedupes already-seen messages, processes new messages, and resyncs Sheets through the normal queue path. It reads every WhatsApp chat `.txt` file inside a ZIP, so a full export can refresh more than one chat.

The script then writes:

- `exports/message_decision_audits/<timestamp>/all_messages.csv`
- `exports/message_decision_audits/<timestamp>/review_roster.csv`
- `exports/message_decision_audits/<timestamp>/summary.md`
- `exports/message_decision_audits/<timestamp>/summary.json`

For messages with the same normalized text within two minutes, the importer also dedupes a weekly archive against live WhatsApp capture even when WhatsApp changes the visible sender or timestamp. If the archive arrives first, the later live capture promotes that stored row to the live metadata and reprocesses it. The audit prefers the live capture and records any historical disagreement as `cross_source_decision_conflict` in the private review roster. Short messages are not merged this way by default.

Security-access incidents that require review, including their follow-up messages, are held out of the public Tenant Log until they are resolved through the private review process.

## Correction Loop

The roster is an exception queue, not the main decision path.

1. Review `review_roster.csv` only for rows the app marked as suspicious, missing, or needing review.
2. Fill `expected_is_issue`, `expected_category`, `expected_asset`, `expected_event_type`, and `correction_notes` only for rows that are wrong.
3. Ask Codex to turn that filled roster into rules/tests.
4. Reprocess the affected messages and resync Sheets.

Codex and ChatGPT do not reliably train themselves from uploaded chats. The durable learning path here is explicit: full chat exports refresh the app database, corrected roster rows become repo rules, prompts, and tests, then stored decisions are regenerated.

## Autonomous Context Model

The app cannot put a 196 MB ZIP into every model call. The reliable design is retrieval from durable state:

- full WhatsApp export -> `RawMessage`
- message decision -> `MessageDecision`
- active and historical issue timeline -> `Incident`
- filing queue and 311 state -> `FilingJob` and `ServiceRequestCase`
- public elevator/project watchdog state -> public-record and watchdog tables
- spreadsheet -> generated control surface from the database

When the classifier runs, it uses a retrieved database context around the message: open and recently closed incidents, same-chat messages from the last few days, related issue messages from recent history, rules, model review, and final guardrails. The default context window is 3 days / 80 same-chat messages, plus 14 days / 40 related issue messages. That is the practical version of "know the whole chat": the full export stays in the database, and each decision gets the relevant recent slice instead of a single message in isolation.

The spreadsheet is already represented by the same database state when the sheet is generated.

If you manually edit spreadsheet cells and expect those edits to steer future decisions, that needs a separate readback step. Without readback, the sheet is output, not the source of truth.

## iPhone Shortcut

iOS Shortcuts cannot reliably tap through WhatsApp's private in-app export screens by itself. The stable setup is a share-sheet shortcut that saves the exported ZIP to the iCloud inbox.

Create a shortcut named `Save 455 Chat Export`:

1. Enable `Use as Share Sheet`.
2. Accept `Files`.
3. Add `Save File`.
4. Destination: `iCloud Drive/455 Tenant Chat Exports`.
5. Turn off `Ask Where to Save`.
6. Keep `Overwrite If File Exists` off.
7. Add `Show Notification`: `Saved chat export for Tenant Issue OS`.

Weekly phone flow:

1. Open WhatsApp.
2. Open the tenant chat.
3. Tap the chat name.
4. Tap `Export Chat`.
5. Choose `Without Media` unless you specifically need attachment files.
6. In the share sheet, choose `Save 455 Chat Export`.

Optional reminder automation:

1. Shortcuts -> Automation -> New Personal Automation.
2. Choose `Time of Day`, weekly.
3. Add `Open App` -> WhatsApp.
4. Add `Show Alert` with the export steps above.

Avoid coordinate-tap Voice Control or Switch Control recipes for this. They can be made to click buttons, but WhatsApp layout changes and iOS permission prompts make them too brittle for a legal/tenant evidence pipeline.

## Reliability Notes

This does not require leaving a fragile long-running Codex task alive. It is a batch import and audit. The staging step retries temporary iCloud file locks, including macOS's `Resource deadlock avoided` error, before treating an export as failed. If the Mac is asleep or off, it cannot process new data while it has no power, but the LaunchAgent runs a catch-up check after it is awake again; the latest full export is idempotently deduped against messages already stored. A truly real-time path while the Mac is off needs a separate cloud upload receiver, not an iCloud-to-Mac workflow.

This does not replace live capture. If the Mac-side WhatsApp watcher is stalled, the weekly full export is the backstop that finds missed messages and decision errors.
