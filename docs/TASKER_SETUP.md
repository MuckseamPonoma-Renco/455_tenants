# Tasker setup (current, minimal)

Use this only for live WhatsApp capture.

## Trigger
- AutoNotification Intercept
- App: WhatsApp
- Filter the tenant group title if possible

## Action
Tasker HTTP Request:
- Method: `POST`
- URL: `https://YOUR_DOMAIN/ingest/tasker`
- Headers:
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

## Why `ts_epoch`
The backend now accepts Tasker epoch timestamps directly, which avoids the parsing failures that happen when Android locale/date formatting changes.
