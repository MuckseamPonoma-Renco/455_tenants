# Final workflow

1. Bulk import the WhatsApp export through `/ingest/export`.
2. Turn on WhatsApp Web capture through Chrome/Playwright.
3. Review `/api/incidents` and `/api/queue`.
4. Let the Playwright filing worker claim and submit each current eligible elevator job automatically.
5. The worker revalidates the incident and exact payload at the portal review screen before submission.
6. After submission, store SR numbers in `/api/cases`.
7. Run status sync daily.
8. Export the legal chronology bundle whenever you need a pressure / attorney / tenant-association packet.

## What is automatic now
- message ingest
- incident clustering
- witness counting
- automatic elevator filing with bounded portal retries
- automatic SR number extraction from chat and portal confirmations
- case storage
- case-status sync
- legal bundle export

## What still requires your phone
- WhatsApp may occasionally ask you to re-link the Mac Chrome session.
- The retired Android/Tasker path should stay off unless you are replaying old migration data.
- A portal job that reaches its retry limit requires inspection before it is reset.


## Fast operator view
- `GET /api/summary` for structured state / next step
- `GET /api/briefing` for a tenant-ready update plus a management follow-up draft
