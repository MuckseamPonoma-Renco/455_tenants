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
  "chat_name": "%WA_CHAT",
  "text": "%WA_TEXT",
  "sender": "%WA_SENDER",
  "ts_epoch": "%WA_TS"
}
```

## Why `ts_epoch`
The backend now accepts Tasker epoch timestamps directly, which avoids the parsing failures that happen when Android locale/date formatting changes.

## Downtime recovery

If the laptop or API is offline, Tasker cannot pull missed WhatsApp history from WhatsApp later by itself. The simplest reliable fix is:

1. Keep the normal live `POST /ingest/tasker`.
2. Also save each intercepted message into a local queue file on the phone.
3. Run a retry task on boot and every 15 minutes that posts the queued items to `POST /ingest/tasker_batch`.

Suggested queue format:

```json
{"items":[
  {"chat_name":"455 Tenants","text":"Both elevators are out again","sender":"Karen","ts_epoch":1770000000}
]}
```

Simple Tasker pattern:

1. On every intercepted WhatsApp notification, append the same JSON body to a local file first.
2. Immediately try `POST /ingest/tasker` as usual.
3. On a schedule, read the queued rows and send them in one request to `/ingest/tasker_batch`.
4. If the batch call returns `200`, clear the local queue file.

Why this works:

- if live capture succeeds, replay is still safe because the backend dedupes repeated messages
- if the API was down, the phone can replay the backlog later after the Mac comes back up
- if the phone itself missed notifications, use a WhatsApp export as the fallback

## Exact Tasker recipe

Use these AutoNotification variables in the task:

- `%anapp` = app name
- `%antitle` = notification title
- `%antext` = notification text
- `%ansubtext` = notification subtext

Suggested Tasker globals:

- `%API_BASE` = `https://api.455tenants.com`
- `%INGEST_TOKEN` = your ingest token
- `%QUEUE_FILE` = `/sdcard/Tasker/tenant_issue_os_queue.jsonl`

Create this profile:

1. `Profile` -> `Event` -> `Plugin` -> `AutoNotification` -> `Intercept`
2. App filter: `WhatsApp`
3. If possible, limit to the tenant group title so unrelated chats do not enter the queue

Attach this task: `Capture Tenant Notification`

1. `If`
Condition:
`%anapp ~ WhatsApp`

2. `Variable Set`
Name: `%WA_CHAT`
To: `%antitle`

3. `Variable Set`
Name: `%WA_SENDER`
To: `%ansubtext`

4. `Variable Set`
Name: `%WA_TEXT`
To: `%antext`

5. `If`
Condition:
`%WA_TEXT !Set`

6. `Variable Set`
Name: `%WA_TEXT`
To: `%ansubtext`

7. `End If`

8. `Variable Set`
Name: `%WA_TS`
To: `%TIMES`

9. `Variable Set`
Name: `%QUEUE_LINE`
To:

```json
{"chat_name":"%WA_CHAT","text":"%WA_TEXT","sender":"%WA_SENDER","ts_epoch":"%WA_TS"}
```

Important:
turn on variable replacement for this action

10. `Write File`
File: `%QUEUE_FILE`
Text: `%QUEUE_LINE`
Append: `On`
Add Newline: `On`

11. `HTTP Request`
Method: `POST`
URL: `%API_BASE/ingest/tasker`
Headers:

```text
Authorization: Bearer %INGEST_TOKEN
Content-Type: application/json
```

Body:

```json
{
  "chat_name": "%WA_CHAT",
  "text": "%WA_TEXT",
  "sender": "%WA_SENDER",
  "ts_epoch": "%WA_TS"
}
```

12. `If`
Condition:
`%http_response_code ~ 200`

13. `Perform Task`
Name: `Replay Tenant Queue`
Priority: `%priority`

14. `End If`

15. `End If`

Create a second task: `Replay Tenant Queue`

1. `Test File`
Type: `Exists`
Data: `%QUEUE_FILE`
Store Result In: `%QUEUE_EXISTS`

2. `If`
Condition:
`%QUEUE_EXISTS !~ true`

3. `Stop`

4. `End If`

5. `Read File`
File: `%QUEUE_FILE`
To Var: `%QUEUE_RAW`

6. `If`
Condition:
`%QUEUE_RAW !Set`

7. `Stop`

8. `End If`

9. `Variable Search Replace`
Variable: `%QUEUE_RAW`
Search: `\n`
Replace Matches: `,`
Store Matches In: leave empty
Regex: `On`

10. `Variable Set`
Name: `%QUEUE_BODY`
To:

```json
{"items":[%QUEUE_RAW]}
```

11. `HTTP Request`
Method: `POST`
URL: `%API_BASE/ingest/tasker_batch`
Headers:

```text
Authorization: Bearer %INGEST_TOKEN
Content-Type: application/json
```

Body: `%QUEUE_BODY`

12. `If`
Condition:
`%http_response_code ~ 200`

13. `Write File`
File: `%QUEUE_FILE`
Text:
leave empty
Append: `Off`

14. `End If`

Create a third profile:

1. `Profile` -> `Event` -> `Device Boot`
2. Task: `Replay Tenant Queue`

Create a fourth profile:

1. `Profile` -> `Time`
2. Every: `15 Minutes`
3. Task: `Replay Tenant Queue`

## What this gives you

- normal live capture while everything is up
- phone-side local buffering when the Mac or tunnel is down
- automatic replay after reboot or after the Mac comes back online
- safe duplicate handling if a message was already ingested live
