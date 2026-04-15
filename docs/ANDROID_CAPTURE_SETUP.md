# Android capture setup (WhatsApp → API)

## Goal

Send every tenant-group WhatsApp message from the Android companion phone to `/ingest/tasker`.

## Phone apps

- WhatsApp linked as companion device
- Tasker
- AutoNotification

## Tasker profile

### Profile
- Event → Plugin → AutoNotification → Intercept
- App: WhatsApp
- Filter by conversation title if possible: `455 Ocean Parkway` / your exact group name

### Task
HTTP Request → POST

URL:
`https://YOUR_DOMAIN/ingest/tasker`

Headers:
- `Authorization: Bearer YOUR_INGEST_TOKEN`
- `Content-Type: application/json`

Body:
```json
{
  "chat_name": "%WA_CHAT",
  "text": "%WA_TEXT",
  "sender": "%WA_SENDER",
  "ts_epoch": "%WA_TS"
}
```

## Reliability settings

- disable battery optimization for Tasker, AutoNotification, WhatsApp
- keep Android on charger + Wi‑Fi
- keep notification access enabled after reboots
- add a daily Tasker heartbeat POST to `/health`
- add a local backlog queue on the phone and replay it to `POST /ingest/tasker_batch` after downtime

## First live test

1. Send a message in the tenant group: `north elevator dead again`
2. Confirm the API returns `ok: true`
3. Open `/api/incidents` and verify an elevator incident was created
4. Open `/api/queue` and verify a 311 filing job exists

## Downtime fallback

If the Mac or tunnel is down:

1. The phone should keep appending intercepted messages to a local queue file.
2. A retry task should later post that queue to `/ingest/tasker_batch`.
3. If the phone did not retain the notifications, export the WhatsApp chat and import it on the Mac with:

```bash
./.venv/bin/python scripts/import_whatsapp_export.py "/path/to/WhatsApp Chat - 455 Tenants.zip"
```
