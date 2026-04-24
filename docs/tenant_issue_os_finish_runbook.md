# Tenant Issue OS runbook

## Core product

1. Chrome/Playwright captures WhatsApp Web messages on the Mac mini.
2. Backend stores and dedupes the raw message.
3. Rules plus optional LLM review classify whether it is a real building issue.
4. Backend clusters related reports into one incident.
5. Backend updates the operator workbook and the clean public `Tenant Log`.
6. Eligible incidents are queued for NYC311.
7. The Playwright portal worker files the complaint through the NYC311 web portal.
8. The SR number and NYC status sync back into the case row and public tenant log.

## Current stance

- Google Sheets is the main control surface.
- The public `Tenant Log` should stay clean, tenant-readable, and evidence-rich.
- LLM support is only for classification confidence and review, not uncontrolled filing.
- Android/Tasker capture and Android app filing are retired. Their endpoints remain only as compatibility shims for old data and tests.
- The browser-based NYC311 worker is the current filing path.

## Operator checklist

1. Start or repair services with `./scripts/check_mac_services.sh --repair`.
2. Confirm `./scripts/check_mac_services.sh --json` shows API, automation, WhatsApp capture, and tunnel as healthy.
3. Confirm `/whatsapp/status` is ready, `login_required` is false, and the chat name is `455 Tenants`.
4. Import any history through `/ingest/export`.
5. Let live WhatsApp Web capture run for new messages.
6. Resync Sheets with `/admin/resync_sheets`.
7. Review `Dashboard`, `Incidents`, `Queue311`, `Cases311`, and `DecisionLog` in the private workbook.
8. Share only the separate public `Tenant Log` workbook with tenants or court-facing viewers.
9. Run the Playwright 311 portal worker for filing jobs.
10. Run `/admin/sync_311_statuses` so case rows use the newest NYC lookup result.

## Definition of done

- One fresh WhatsApp Web message is captured.
- One incident is created with the correct category.
- One public `Tenant Log` row appears with clean wording and evidence links.
- Tenant-useful screenshots preview in the sheet; tiny message-strip screenshots remain as links.
- One filing job reaches the Playwright portal worker.
- One SR is stored in `Cases311`.
- The public sheet shows the NYC status from the case row.

## Human actions still required

- Keep `.env` credentials current.
- Keep the Google service account shared into the private/source workbook.
- Approve Drive permission changes only if we intentionally upload mirrored evidence files to Drive.
- Re-link WhatsApp Web if Chrome shows `login_required`.
