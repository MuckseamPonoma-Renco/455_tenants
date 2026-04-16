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
- `%TENANT_GROUP_NAME` = your exact WhatsApp group title

Before you build the tasks:

1. Open Tasker once and grant storage/file access if Android asks.
2. Make sure AutoNotification still has notification access after reboot.
3. Disable battery optimization for Tasker, AutoNotification, and WhatsApp.
4. Create the folder `/sdcard/Tasker/` if it does not already exist.
5. In Tasker, create the three globals above exactly in uppercase so Tasker keeps them globally.

Create this profile:

1. `Profile` -> `Event` -> `Plugin` -> `AutoNotification` -> `Intercept`
2. App filter: `WhatsApp`
3. If AutoNotification shows a title or text filter field, set it to `%TENANT_GROUP_NAME`
4. If you cannot filter inside the plugin, keep the plugin broad and add a Tasker `If %WA_CHAT ~ %TENANT_GROUP_NAME` check in the task

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

9. `If`
Condition:
`%WA_CHAT !~ %TENANT_GROUP_NAME`

10. `Stop`

11. `End If`

12. `If`
Condition:
`%WA_TEXT !Set`

13. `Stop`

14. `End If`

15. `Variable Set`
Name: `%QUEUE_LINE`
To:

```json
{"chat_name":"%WA_CHAT","text":"%WA_TEXT","sender":"%WA_SENDER","ts_epoch":"%WA_TS"}
```

Important:
turn on variable replacement for this action

16. `Write File`
File: `%QUEUE_FILE`
Text: `%QUEUE_LINE`
Append: `On`
Add Newline: `On`

17. `HTTP Request`
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

Recommended Tasker options for this action:

- Body Content Type: `application/json`
- Timeout: leave default or set `30`
- Continue Task After Error: `On`

18. `If`
Condition:
`%http_response_code ~ 200`

19. `Perform Task`
Name: `Replay Tenant Queue`
Priority: `%priority`
Stop: `Off`
Local Variable Passthrough: `Off`

20. `End If`

21. `End If`

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
Search: `\r`
Replace Matches:
leave empty
Store Matches In: leave empty
Regex: `On`

10. `Variable Set`
Name: `%QUEUE_RAW`
To: `%QUEUE_RAW`

Important:
turn on `Do Maths/Variable Replacement`

11. `Variable Search Replace`
Variable: `%QUEUE_RAW`
Search: `[\n]+`
Replace Matches: `,`
Store Matches In: leave empty
Regex: `On`

12. `Variable Search Replace`
Variable: `%QUEUE_RAW`
Search: `,+$`
Replace Matches:
leave empty
Store Matches In: leave empty
Regex: `On`

13. `If`
Condition:
`%QUEUE_RAW !Set`

14. `Stop`

15. `End If`

16. `Variable Set`
Name: `%QUEUE_BODY`
To:

```json
{"items":[%QUEUE_RAW]}
```

Important:
turn on variable replacement for this action

17. `HTTP Request`
Method: `POST`
URL: `%API_BASE/ingest/tasker_batch`
Headers:

```text
Authorization: Bearer %INGEST_TOKEN
Content-Type: application/json
```

Body: `%QUEUE_BODY`

Recommended Tasker options for this action:

- Body Content Type: `application/json`
- Timeout: leave default or set `60`
- Continue Task After Error: `On`

18. `If`
Condition:
`%http_response_code ~ 200`

19. `Write File`
File: `%QUEUE_FILE`
Text:
leave empty
Append: `Off`

20. `End If`

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

## Fast test

After you finish the setup:

1. Start the Mac side:
```bash
cd /Users/max/Desktop/scripts/455-tenants-finalized-v6/455-tenants-finalized
./scripts/start_mac_services.sh
./scripts/check_mac_services.sh
```
2. Send one fresh WhatsApp message in the tenant group:
`both elevators are out right now`
3. Confirm the Mac shows the local API as healthy.
4. Confirm:
```bash
curl -H "Authorization: Bearer YOUR_INGEST_TOKEN" http://127.0.0.1:8000/api/summary
curl -H "Authorization: Bearer YOUR_INGEST_TOKEN" http://127.0.0.1:8000/api/queue
```
5. Turn Wi‑Fi off on the Mac or stop the Mac services.
6. Send another tenant-group message from WhatsApp.
7. Turn the Mac back on or restart the services.
8. Manually run `Replay Tenant Queue` once in Tasker.
9. Confirm the queued message appears after replay.
