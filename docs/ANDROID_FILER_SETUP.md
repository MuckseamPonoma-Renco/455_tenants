# Android filing worker setup (superseded)

Use [docs/NYC311_PORTAL_AUTOMATION.md](/media/max/T7/scripts/455-tenants-finalized-v6/455-tenants-finalized/docs/NYC311_PORTAL_AUTOMATION.md) for the current filing path. This Android/AutoInput flow is kept only as historical reference.

## Goal

Turn the spare Android phone into a local 311 filing robot without CAPTCHA-solving services.

## Apps

- Tasker
- AutoInput
- NYC311 app

## Current repo values

Use the live Cloudflare hostname for this repo:

- `https://api.455tenants.com`

Use this token on the filer phone:

- `MOBILE_FILER_TOKEN`
- if that is not set on the server, use `INGEST_TOKEN` instead

## Backend flow

1. Tasker polls `POST /mobile/filings/claim_next`
2. If a job exists, Tasker stores the JSON response in `%job_json`
3. Tasker reads `%job_json.job.job_id`
4. Tasker reads `%job_json.job.payload.description`
5. Tasker opens NYC311 app
6. AutoInput navigates to the elevator complaint flow
7. Tasker pastes the generated description
8. Tasker extracts the Service Request Number from the success screen
9. Tasker calls `POST /mobile/filings/{job_id}/submitted`
10. On any hard failure, Tasker calls `POST /mobile/filings/{job_id}/failed`

Tasker can read JSON natively, so once `%job_json` contains the full response body you can use variable paths such as:

- `%job_json.job.job_id`
- `%job_json.job.incident_id`
- `%job_json.job.complaint_type`
- `%job_json.job.form_target`
- `%job_json.job.payload.title`
- `%job_json.job.payload.problem`
- `%job_json.job.payload.description`

## Tasker globals

Create a one-time setup task such as `Filer Init` with:

1. `Variable Set` → `%API_BASE` = `https://api.455tenants.com`
2. `Variable Set` → `%FILER_TOKEN` = your live mobile token

Because these are uppercase variables, Tasker will keep them as globals.

## Task: Claim Next Filing

Create a Tasker task named `Claim Next Filing`:

1. `HTTP Request`
2. Method: `POST`
3. URL: `%API_BASE/mobile/filings/claim_next`
4. Headers:
   - `Authorization:Bearer %FILER_TOKEN`
5. Leave body empty
6. Timeout: `30`
7. `If` `%http_response_code !~ 200`
8. `Flash`:
   - `claim failed: %http_response_code`
   - `%http_data`
9. `Stop`
10. `End If`
11. `If` `%http_data ~ *\"job\":null*`
12. `Flash`:
   - `No filing job right now`
13. `Stop`
14. `End If`
15. `Variable Set` → `%job_json` = `%http_data`

At that point these values should work directly inside the same task or any child task:

- `%job_json.job.job_id`
- `%job_json.job.payload.description`

## Task: Run Filing Loop

Create a parent task named `Run Filing Loop`:

1. `Perform Task` → `Claim Next Filing`
2. `Set Clipboard` → `%job_json.job.payload.description`
3. `Variable Set` → `%job_id` = `%job_json.job.job_id`
4. `Variable Set` → `%job_description` = `%job_json.job.payload.description`
5. `Launch App` → `NYC311`
6. `Wait` → `2` seconds
7. Run the AutoInput steps that reach the elevator complaint text field
8. Focus the description field
9. Paste the clipboard contents
10. Continue through the complaint flow until the final confirmation screen is visible

Keep `%job_id` available until the success or failure callback finishes.

## AutoInput selector strategy

Because the NYC311 app UI can change, use this order of preference for AutoInput selectors:

1. visible exact text
2. content description
3. nearby label + click coordinates as fallback

Recommended checkpoints to save screenshots for:

1. app home
2. complaint category selection
3. elevator complaint path
4. description field before paste
5. final review screen before submit
6. success screen showing SR number
7. every failure branch

## Suggested NYC311 path

The exact labels can change, but the stable operator pattern is:

1. open NYC311
2. start a new complaint
3. choose the elevator / escalator complaint path
4. choose the non-working / defective problem
5. focus the description box
6. paste `%job_description`
7. review and submit

When building selectors, capture a screenshot after every step that successfully advances the form.

## Extract the SR number

After the app shows the final success screen:

1. Use `Get Screen Info (Assistant)` or your preferred AutoInput screen-text action
2. Create `%screen_text` from the visible screen text
3. Run `Simple Match/Regex` against `%screen_text`
4. Use this regex:

```regex
(?<sr>(?:311-)?\d{8})
```

If `%sr` is empty, treat that as a hard failure and call the failure callback.

Normalize the result:

1. `If` `%sr !~ 311-*`
2. `Variable Set` → `%sr_number` = `311-%sr`
3. `Else`
4. `Variable Set` → `%sr_number` = `%sr`
5. `End If`

This accepts either:

- `311-25815998`
- bare `25815998`

and always normalizes to `311-########`.

## Submit callback

On successful SR extraction, call:

`POST %API_BASE/mobile/filings/%job_id/submitted`

Headers:

- `Authorization:Bearer %FILER_TOKEN`
- `Content-Type:application/json`

Body:

```json
{
  "service_request_number": "%sr_number",
  "app_status": "submitted",
  "notes": "submitted from Android NYC311 app"
}
```

Expected result:

- HTTP `200`
- response body includes `"ok": true`

## Failure callback

On any hard failure, call:

`POST %API_BASE/mobile/filings/%job_id/failed`

Headers:

- `Authorization:Bearer %FILER_TOKEN`
- `Content-Type:application/json`

Body:

```json
{
  "error": "%failure_code",
  "notes": "%failure_notes"
}
```

Useful `%failure_code` values:

- `claim_http_error`
- `claim_empty_job`
- `selector_home_changed`
- `selector_category_changed`
- `selector_description_changed`
- `selector_submit_changed`
- `sr_not_found`
- `submitted_callback_http_error`

Useful `%failure_notes` values:

- selector name or screen label that failed
- screenshot path if you saved one
- the relevant HTTP status code

## Retry and safety rules

Add these two safeguards before calling the setup complete:

1. retry claim and callback requests once before giving up
2. save a screenshot before final submit and on every failure branch

If the app stops in an unknown state, do not keep tapping blindly. Save the screenshot, post `/failed`, and reset to the home screen for the next run.

## Polling profile

Once the task works manually, create a Tasker profile that runs it on a schedule:

1. `Time` profile
2. every `5` or `10` minutes
3. optional extra constraints:
   - on charger
   - Wi-Fi connected
   - screen unlocked only if AutoInput behaves better that way
4. task: `Run Filing Loop`

## Verification checklist

Run one real end-to-end test and confirm all of these:

1. `Claim Next Filing` returns a non-null job
2. `%job_json.job.job_id` resolves correctly
3. `%job_json.job.payload.description` pastes into NYC311
4. the success screen shows an SR number
5. `POST /mobile/filings/{job_id}/submitted` returns `200`
6. `/api/cases` contains the new `311-########`
7. `/api/summary` moves toward `tracking_live`

## Strong recommendation

Build the filing flow against the NYC311 Android app first. Only use the mobile web path if the app blocks a specific complaint type.
