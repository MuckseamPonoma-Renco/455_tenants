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
  "chat_name": "%antitle",
  "text": "%antext",
  "sender": "%ansubtext",
  "ts_epoch": "%TIMES"
}
```

## Reliability settings

- disable battery optimization for Tasker, AutoNotification, WhatsApp
- keep Android on charger + Wi‑Fi
- keep notification access enabled after reboots
- add a daily Tasker heartbeat POST to `/health`

## First live test

1. Send a message in the tenant group: `north elevator dead again`
2. Confirm the API returns `ok: true`
3. Open `/api/incidents` and verify an elevator incident was created
4. Open `/api/queue` and verify a 311 filing job exists
