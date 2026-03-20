# Tenant Issue OS — finish runbook

## Core product, stripped to essentials

1. Android notification capture from WhatsApp.
2. Backend stores raw message.
3. Backend classifies whether it is a real building issue.
4. Backend clusters related reports into one incident.
5. Backend updates Google Sheets immediately.
6. Backend decides whether to queue a 311 filing.
7. Spare Android phone claims next filing job and submits in NYC311 app.
8. Backend stores SR number and tracks case status.

## Correct product stance

- Google Sheets is first-class, not optional in the product.
- LLM should not be removed.
- LLM should not directly tap the NYC311 app.
- LLM should sit above classification, clustering, confidence, escalation, and suggested actions.
- Final submit authority remains under user control / configured policy.

## What to keep deterministic

- auth
- message storage
- dedupe by message id
- job queue storage
- SR storage
- Google Sheets writes
- Android filing handshake

## What to make hybrid or LLM-first

- issue classification
- outage vs restore
- same-incident vs new-incident
- whether a filing should be queued
- whether incident is strong enough to escalate
- drafting incident summaries for spreadsheet / dashboard

## Best immediate additions

1. LLM triage mode = default for uncertain and medium-confidence messages.
2. Spreadsheet-first dashboard as the main face of the system.
3. Tenant report link / QR code that opens a tiny web form:
   - elevator north out
   - elevator south out
   - both out
   - restored
   - other issue
4. Form submission posts directly to backend and also optionally creates a WhatsApp-ready share text.
5. LLM daily digest for operator only, not weekly spam for tenants.
6. Repeated-outage streak detection and management-escalation trigger.

## What NOT to build now

- heavy agent swarm
- full tenant-facing polished frontend
- recurring tenant brief spam
- court packet automation beyond export
- anything that replaces the spreadsheet as the main surface

## Definition of done for this phase

- one live notification captured from Android
- one incident created correctly
- one row appears in Sheets correctly
- one filing job created correctly
- one job claimed by spare Android phone
- one real SR submitted
- one SR stored and synced back
- one elevator QR/form report creates same pipeline outcome

## Codex order

### Prompt 1 — architecture hardening
You are working inside Tenant Issue OS. Do not remove any existing creds or working flows. Keep Android capture, filing queue, and SR tracking intact. Make Google Sheets a first-class required product surface, not an optional extra except for local testing toggles. Refactor the pipeline so that deterministic rules remain for obvious messages, but the default decision layer for classification, outage/restore, incident clustering, and filing eligibility becomes hybrid: rules for obvious cases, LLM for uncertain or medium-confidence cases. Add tests. Return a short changelog and exact files changed.

### Prompt 2 — spreadsheet-first face
You are working inside Tenant Issue OS. Do not remove any working behavior. Make the spreadsheet the primary operator face of the app. Ensure incidents, queue, cases, and a top dashboard are always synced after ingest, incident updates, job submission, and case status sync. Add a sheet tab for operator decisions with columns like incident_id, recommended_action, confidence, escalate_now, llm_reason_short, human_override. Wire backend reads/writes so this tab becomes part of the control loop. Add tests and docs.

### Prompt 3 — LLM decision layer
You are working inside Tenant Issue OS. Implement an LLM decision layer that receives: raw message text, recent open incidents, last 20 related raw messages, recent SR cases, and sheet override context. The model must return strict JSON with fields: is_issue, category, asset, kind, confidence, same_incident_as, should_queue_311, recommended_action, short_reason. Keep deterministic fallback if model is unavailable. Add caching and low-cost safeguards. Add tests for both fallback and LLM paths. Do not break current endpoints.

### Prompt 4 — simple tenant QR / report form
You are working inside Tenant Issue OS. Add the smallest possible tenant report surface. Create a minimal web form page with large mobile-friendly buttons for: north elevator out, south elevator out, both elevators out, restored, other issue. It should post to the backend, create a raw message-like event, flow through the same incident pipeline, and update Sheets. Keep it very simple and intuitive. Generate one static QR target URL for the building. Add docs for printing the QR code and placing it in the elevator.

### Prompt 5 — escalation and anti-spam logic
You are working inside Tenant Issue OS. Add smart escalation logic. The system should not spam tenants. It should detect repeated outages, long unresolved incidents, and repeated failures after reported restoration. Add operator-facing escalation suggestions only. Suggested outputs: file 311 now, wait for second witness, send management follow-up, mark restored, monitor only. Update spreadsheet tabs and add tests.

### Prompt 6 — second-phone filing stabilization
You are working inside Tenant Issue OS. Do not change the backend contract. Improve the Android filing worker docs and payload shape so selector mapping is easier. Add a verification checklist, expected payload examples, and a failure-code taxonomy for common Android issues. Add any missing tests on claim_next / submitted / failed endpoints.

### Prompt 7 — operator runbook cleanup
You are working inside Tenant Issue OS. Write a concise runbook for the human operator. Cover: first startup, import history, enable live capture, validate Sheets sync, run one simulated trigger, run one real filing, recover from failed filing, and daily usage. Keep it short and action-oriented.

## What the user should do now without second phone number

1. Run backend locally.
2. Import the WhatsApp export.
3. Confirm Sheets sync works.
4. Simulate one trigger with curl.
5. Confirm incident + queue + Sheets rows update.
6. Add OPENAI_API_KEY.
7. Run the LLM-path tests.
8. Use Codex prompts 1 and 2 first.
9. Use prompt 3 after prompt 2 passes tests.
10. Use prompt 4 only after the core pipeline is stable.

## Human clicks still required

- populate .env
- confirm Google service account access to the spreadsheet
- share spreadsheet with service account email
- run backend
- inspect sheet tabs
- later configure Tasker / AutoInput on Android
- later place QR code if the form is added

