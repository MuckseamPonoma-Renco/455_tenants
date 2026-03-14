# Android filing worker setup (queue → NYC311 app → API callback)

## Goal

Turn the spare Android phone into a local 311 filing robot without CAPTCHA-solving services.

## Apps

- Tasker
- AutoInput
- NYC311 app

## Flow

1. Tasker polls `POST /mobile/filings/claim_next`
2. If a job exists, Tasker opens NYC311 app
3. AutoInput navigates to the elevator complaint flow
4. Tasker pastes the generated description from the job payload
5. When the app shows the Service Request Number, Tasker extracts it
6. Tasker calls `POST /mobile/filings/{job_id}/submitted`

## Poll request

`POST https://YOUR_DOMAIN/mobile/filings/claim_next`

Headers:
- `Authorization: Bearer YOUR_MOBILE_FILER_TOKEN`

## Submit callback

`POST https://YOUR_DOMAIN/mobile/filings/{job_id}/submitted`

Body:
```json
{
  "service_request_number": "311-25815998",
  "app_status": "submitted",
  "notes": "submitted from Android NYC311 app"
}
```

## Failure callback

`POST https://YOUR_DOMAIN/mobile/filings/{job_id}/failed`

Body:
```json
{
  "error": "selector changed on final submit button",
  "notes": "stopped before submission"
}
```

## Practical selector strategy

Because the NYC311 app UI can change, use this order of preference for AutoInput selectors:

1. visible exact text
2. content description
3. nearby label + click coordinates as fallback

Save screenshots for each step while building the flow in Tasker.

## Strong recommendation

Build the filing flow against the NYC311 Android app first. Only use the mobile web path if the app blocks a specific complaint type.
