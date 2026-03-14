# Android capture setup (WhatsApp companion phone -> `/ingest/tasker`)

## Goal

Use one Android phone as the live WhatsApp capture device so every tenant-group message is sent to this API and processed into incidents/queue jobs.

This document is for the capture side only. It does not cover the NYC311 filing flow. For that, use [ANDROID_FILER_SETUP.md](/media/max/T7/scripts/455-tenants-finalized/docs/ANDROID_FILER_SETUP.md).

## What Codex can and cannot do

Codex can complete the server-side setup in this repo, verify the API contract, and give you the exact values and steps to enter.

Codex cannot tap through Android settings, grant notification access, pair WhatsApp as a companion device, or create the Tasker profile on your phone. You must do those steps on-device.

## Before you touch the phone

Complete these backend prerequisites first.

### 1. Configure the repo

If you have not already done it:

```bash
cp .env.example .env
```

Set at least these values in `.env`:

- `INGEST_TOKEN`
- `DATABASE_URL`
- `PROCESS_INLINE=1` for simple local testing, or leave worker/Redis enabled if you already run the full stack
- `DISABLE_SHEETS_SYNC=1` for local testing if Sheets is not configured yet

If you are testing locally without Docker, the simplest path is:

```bash
export PROCESS_INLINE=1
export DISABLE_SHEETS_SYNC=1
uvicorn apps.api.main:app --reload
```

If you are using Docker:

```bash
docker compose up --build
```

### 2. Verify the API is reachable

Before you touch Tasker, verify all of these:

1. the API process is up
2. the authenticated ingest endpoint accepts requests
3. the Android phone can reach the exact same base URL you plan to paste into Tasker

#### Option A: you are testing on the same machine that runs the API

Start with a simple health check from that machine:

```bash
curl http://127.0.0.1:8000/health
```

Expected response:

```json
{"ok":true}
```

Then verify the authenticated ingest path, not just `/health`:

```bash
export TOKEN='YOUR_INGEST_TOKEN'
curl -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"chat_name":"455 Tenants","text":"connectivity test","sender":"Setup Check","ts_epoch":1770000000}' \
  http://127.0.0.1:8000/ingest/tasker
```

Expected response on the first run:

```json
{"ok":true,"deduped":false,"message_id":"...","job_id":"..."}
```

If you repeat the same request, `deduped` may become `true`. That is normal.

If this step fails, do not continue to the phone yet. Fix the backend first.

#### Option B: the Android phone will talk to your local dev machine over Wi-Fi

Important:

- `http://127.0.0.1:8000` only works from the same machine running the API.
- Your Android phone cannot send to `127.0.0.1` on your laptop or server.

If you are running the API directly with `uvicorn`, make it listen on the network:

```bash
uvicorn apps.api.main:app --host 0.0.0.0 --port 8000 --reload
```

If you are running Docker with:

```bash
docker compose up --build
```

the port is already published on `8000`, so you usually only need the machine's LAN IP.

Find that LAN IP on the machine running the API:

```bash
hostname -I
```

Use the address on the same Wi-Fi network as the phone, then test that URL from the phone browser:

```text
http://YOUR_LAN_IP:8000/health
```

Expected response in the phone browser:

```json
{"ok":true}
```

Only use that LAN URL in Tasker if the phone can open it successfully.

If the phone cannot open that URL:

- confirm both devices are on the same Wi-Fi
- confirm the API is still running
- confirm your OS firewall allows inbound connections on port `8000`
- confirm you started `uvicorn` with `--host 0.0.0.0` if you are not using Docker

#### Option C: the Android phone will talk to a deployed server

Make sure you know the final public base URL and test it from the phone browser too, for example:

```text
https://your-domain.example
```

Then confirm:

```text
https://your-domain.example/health
```

opens from the phone without certificate or network errors.

If the phone will not stay on the same Wi-Fi as your dev machine, prefer a deployed HTTPS URL instead of a LAN URL.

#### Final result

By the end of this step, you should know the exact base URL to use in Tasker:

- local same-machine check only: `http://127.0.0.1:8000`
- local Wi-Fi phone testing: `http://YOUR_LAN_IP:8000`
- deployed server: `https://your-domain.example`

The Tasker endpoint will be:

```text
YOUR_BASE_URL/ingest/tasker
```

### 3. Copy the two values you will need on the phone

You will enter these into Tasker:

- API URL: `https://YOUR_DOMAIN/ingest/tasker`
- Bearer token: the value of `INGEST_TOKEN` from `.env`

## What the backend expects

The live capture endpoint is:

```text
POST /ingest/tasker
```

It requires this header:

```text
Authorization: Bearer YOUR_INGEST_TOKEN
```

And it accepts JSON in this shape:

```json
{
  "chat_name": "455 Tenants",
  "text": "Both elevators are out again",
  "sender": "Tibor Simon",
  "ts_epoch": 1770000000
}
```

Notes:

- `ts_epoch` is preferred over date strings.
- Duplicate notifications are safe; the backend dedupes repeated messages.
- `/api/incidents` and `/api/queue` also require the same bearer token.

## On the Android phone

### 1. Install the apps

Install:

- WhatsApp
- Tasker
- AutoNotification

Recommended: update all three before you begin so the first setup matches the current UI.

### 2. Link WhatsApp as a companion device

On your primary WhatsApp device:

1. Open WhatsApp.
2. Open linked devices.
3. Add a linked device.
4. Scan the QR code shown on the Android companion phone.

On the Android phone:

1. Sign in to WhatsApp as a linked device.
2. Open the tenant group.
3. Confirm new messages appear normally on this phone.

Do not continue until the companion phone is receiving the same group messages in real time.

### 3. Make notifications visible enough for capture

Tasker/AutoNotification can only forward what Android exposes in the notification.

On the Android phone:

1. Enable WhatsApp notifications.
2. Allow notification content to appear on the lock screen if your device hides message text by default.
3. Make sure the tenant group is not muted in a way that suppresses notifications.
4. Send one test message into the tenant group and confirm the companion phone shows a WhatsApp notification with sender and message text.

If the notification only shows "1 new message" without the message body, Tasker will not have enough data.

### 4. Grant the required Android permissions

Grant these before building the Tasker profile:

1. Tasker:
   - allow background activity if Android asks
   - disable battery optimization
2. AutoNotification:
   - enable Notification Access
   - disable battery optimization
3. WhatsApp:
   - disable battery optimization

Also recommended:

- keep the phone on Wi-Fi
- keep it on a charger
- exclude Tasker and AutoNotification from vendor-specific "sleep" or "deep clean" features

## Create the Tasker automation

The cleanest setup is one Tasker task plus one Tasker profile.

### 1. Create the task

In Tasker:

1. Go to the `Tasks` tab.
2. Tap `+`.
3. Name it `WhatsApp Capture -> API`.
4. Add one action: `Net` -> `HTTP Request`.

Fill the HTTP Request action with:

- `Method`: `POST`
- `URL`: `https://YOUR_DOMAIN/ingest/tasker`
- `Headers`:

```text
Authorization: Bearer YOUR_INGEST_TOKEN
Content-Type: application/json
```

- `Body`:

```json
{
  "chat_name": "%antitle",
  "text": "%antext",
  "sender": "%ansubtext",
  "ts_epoch": "%TIMES"
}
```

Important details:

- Keep the body as raw JSON.
- `%TIMES` is the Tasker epoch timestamp and is what this backend is designed to accept.
- `%antitle` should be the conversation title or notification title.
- `%antext` should be the notification text.
- `%ansubtext` is commonly the sender name in group-message notifications.

If your Tasker version exposes response fields, store them if convenient:

- response code -> `%http_response_code`
- response body -> `%http_data`

That makes troubleshooting easier, but it is optional.

### 2. Create the profile

In Tasker:

1. Go to the `Profiles` tab.
2. Tap `+`.
3. Choose `Event`.
4. Choose `Plugin`.
5. Choose `AutoNotification`.
6. Choose `Intercept`.
7. Tap the pencil/config icon.

Inside AutoNotification Intercept:

1. Set `App` to `WhatsApp`.
2. If your version allows title filtering, add the tenant group title exactly as it appears in WhatsApp.
3. If group-title filtering is unreliable, leave filtering broad for the first test and tighten it later.
4. Save the intercept configuration.

When Tasker asks for the linked task, choose:

```text
WhatsApp Capture -> API
```

### 3. Reduce false captures

After the first successful end-to-end test, narrow the profile so it only forwards the tenant group.

Use whichever of these works best on your phone:

1. exact notification title match for the group name
2. notification text/title contains the building or tenant-group name
3. additional Tasker `If` checks before the HTTP action

If you use an `If` check, the safest gate is the chat title, not the sender.

## First live test

### 1. Send a trigger message

Post a message in the tenant group from a different phone, for example:

```text
north elevator dead again
```

That message should:

1. appear as a WhatsApp notification on the Android companion phone
2. trigger the Tasker profile
3. send a `POST` to `/ingest/tasker`

### 2. Confirm the ingest request worked

Check the API response if Tasker shows it. A successful first-time insert looks like:

```json
{
  "ok": true,
  "deduped": false,
  "message_id": "...",
  "job_id": "..."
}
```

If the same notification fires again, this is also fine:

```json
{
  "ok": true,
  "deduped": true,
  "message_id": "..."
}
```

### 3. Verify from the API

Use your ingest token in these checks:

```bash
export TOKEN='YOUR_INGEST_TOKEN'
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8000/api/incidents
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8000/api/queue
```

Expected result after an elevator-outage message:

- `/api/incidents` contains an elevator incident
- `/api/queue` contains a 311 filing job

If you are testing against a deployed server, replace `http://127.0.0.1:8000` with your real base URL.

## Strong reliability settings

Apply these after the first successful test.

### Android system settings

1. Disable battery optimization for `Tasker`, `AutoNotification`, and `WhatsApp`.
2. Allow background activity for those apps.
3. Keep the device on charger power.
4. Keep the device on stable Wi-Fi.
5. Re-check Notification Access after every reboot or Android update.

### WhatsApp/device habits

1. Do not log the companion phone out of WhatsApp.
2. Do not mute the tenant group in a way that suppresses notifications entirely.
3. Avoid aggressive vendor cleanup apps or "RAM booster" features.

## Recommended maintenance automations

These are optional, but worth adding once the base capture works.

### 1. Tasker heartbeat

Create a separate Tasker task that runs every 10 to 15 minutes and calls:

```text
GET https://YOUR_DOMAIN/health
```

Expected response:

```json
{"ok":true}
```

This only confirms the server is alive. It does not confirm notification capture is still working.

### 2. Daily manual sanity check

Once per day:

1. send one harmless message into the tenant group
2. confirm it appears on the capture phone
3. confirm it reaches `/api/incidents` or at least the raw ingest path without auth errors

## Troubleshooting

### `401 Missing Authorization: Bearer <token>`

The Tasker request does not include the `Authorization` header in the expected format.

Fix:

- confirm the header is exactly `Authorization: Bearer YOUR_INGEST_TOKEN`
- confirm there is a space after `Bearer`

### `403 Invalid token`

The token in Tasker does not match `.env`.

Fix:

- copy `INGEST_TOKEN` again from `.env`
- restart the API if you recently changed `.env`

### `500 Auth token not configured`

The server is running without `INGEST_TOKEN`.

Fix:

- set `INGEST_TOKEN` in `.env`
- restart the API/container

### Tasker fires, but nothing reaches the API

Likely causes:

- wrong URL
- phone cannot reach the server
- HTTPS/TLS issue
- Android blocked background network activity

Fix:

1. test the same URL manually from the phone browser if possible
2. confirm your server is publicly reachable from the phone
3. confirm the Tasker action is `POST`
4. confirm the request body is valid JSON

### The API receives requests, but incidents are not created

Likely causes:

- the notification text is missing or empty
- the message does not describe a detectable elevator incident
- you are testing with a non-tenant-group chat

Fix:

1. inspect the captured `text` value in Tasker
2. test with a simple elevator outage phrase such as `both elevators are out again`
3. verify the correct group title is being sent as `chat_name`

### The phone stopped capturing after a reboot

Android often disables background behavior or notification access after updates/restarts.

Check:

1. Tasker is enabled
2. AutoNotification still has Notification Access
3. battery optimization is still disabled
4. WhatsApp notifications still show full content

## Minimum final checklist

You are done when all of this is true:

1. the Android phone is a working WhatsApp companion device
2. WhatsApp notifications on that phone include sender + message text
3. Tasker sends `POST https://YOUR_DOMAIN/ingest/tasker`
4. the request includes `Authorization: Bearer YOUR_INGEST_TOKEN`
5. the JSON body uses `%antitle`, `%antext`, `%ansubtext`, and `%TIMES`
6. a live tenant-group elevator message creates an incident
7. that incident creates a queue job

After this doc is complete, continue with [ANDROID_FILER_SETUP.md](/media/max/T7/scripts/455-tenants-finalized/docs/ANDROID_FILER_SETUP.md).
