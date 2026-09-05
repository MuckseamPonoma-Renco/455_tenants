import assert from "node:assert/strict";
import test from "node:test";

import {
  acknowledgementReceipt,
  compactAudit,
  compactPipelineReceipt,
  effectiveExportTimestamp,
  normalizeFilename,
  parseUploadSize,
  receiptNeedsRetry,
  shouldReplaceLatestReceipt,
} from "./worker.js";

test("normalizes the extensionless filename emitted by Shortcuts", () => {
  assert.equal(
    normalizeFilename("WhatsApp Chat - 455 Tenants 2"),
    "WhatsApp Chat - 455 Tenants 2.zip",
  );
});

test("preserves explicit supported extensions", () => {
  assert.equal(normalizeFilename("WhatsApp Chat - 455 Tenants.zip"), "WhatsApp Chat - 455 Tenants.zip");
  assert.equal(normalizeFilename("WhatsApp Chat - 455 Tenants.txt"), "WhatsApp Chat - 455 Tenants.txt");
});

test("accepts Shortcuts display sizes", () => {
  assert.equal(parseUploadSize("48 KB"), 48 * 1024);
  assert.equal(parseUploadSize("205.1 MB"), Math.round(205.1 * 1024 ** 2));
});

test("still rejects unsafe filenames", () => {
  assert.throws(() => normalizeFilename("../WhatsApp Chat - 455 Tenants"));
  assert.throws(() => normalizeFilename("other export"));
});

test("preserves strict model-review counters in acknowledgment receipts", () => {
  assert.deepEqual(
    compactAudit({
      parsed_messages: 2509,
      llm_review_required: 197,
      llm_review_completed: 55,
      llm_review_missing: 104,
      llm_review_failed: 38,
      unique_physical_message_ids: 2507,
      colliding_base_message_ids: 2,
      collision_followup_occurrences: 2,
      unsafe_detail: "must not be retained",
    }),
    {
      parsed_messages: 2509,
      llm_review_required: 197,
      llm_review_completed: 55,
      llm_review_missing: 104,
      llm_review_failed: 38,
      unique_physical_message_ids: 2507,
      colliding_base_message_ids: 2,
      collision_followup_occurrences: 2,
    },
  );
});

test("persists a sanitized versioned pipeline receipt with server-confirmed acknowledgement", () => {
  const receipt = compactPipelineReceipt({
    uploaded_at: "2026-09-05T11:56:26.873Z",
    discovered_at: "2026-09-05T12:01:00Z",
    staged_export: "/private/chat.zip",
    stages: {
      upload: { state: "complete", at: "2026-09-05T11:56:26.873Z" },
      discovery: { state: "complete", at: "2026-09-05T12:01:00Z" },
      download: { state: "complete" },
      processing: { state: "complete" },
      audit: { state: "complete" },
      sheet_sync: { state: "complete" },
      sheet_readback: { state: "complete" },
      acknowledgement: { state: "pending" },
    },
  }, {
    key: "pending/export-key",
    acknowledgedAt: "2026-09-05T12:05:00.000Z",
    sha256: "a".repeat(64),
    audit: { parsed_messages: 2741 },
  });

  assert.equal(receipt.schema_version, 1);
  assert.equal(receipt.receipt_version, 1);
  assert.equal(receipt.status, "ready");
  assert.deepEqual(receipt.stages.acknowledgement, {
    state: "complete",
    at: "2026-09-05T12:05:00.000Z",
  });
  assert.equal("staged_export" in receipt, false);
});

test("requeues incomplete and legacy unresolved audit receipts", () => {
  assert.equal(receiptNeedsRetry({ audit: {
    llm_review_required: 197,
    llm_review_completed: 55,
    llm_review_missing: 104,
    llm_review_failed: 38,
  } }), true);
  assert.equal(receiptNeedsRetry({ audit: {
    llm_review_required: 197,
    llm_review_completed: 197,
    llm_review_missing: 0,
    llm_review_failed: 0,
    review_roster_rows: 5,
  } }), false);
  assert.equal(receiptNeedsRetry({ audit: {
    llm_review_required: 197,
    llm_review_completed: 197,
    review_roster_rows: 5,
  } }), true);
  assert.equal(receiptNeedsRetry({ audit: { review_roster_rows: 142 } }), true);
  assert.equal(receiptNeedsRetry({ audit: { review_roster_rows: 0 } }), false);
});

test("an older export acknowledged later cannot replace the latest export pointer", () => {
  const newer = {
    key: "pending/20260905T140000000Z-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-WhatsApp Chat.zip",
    uploaded_at: "2026-09-05T14:00:30.000Z",
    discovered_at: "2026-09-05T14:01:00.000Z",
    acknowledged_at: "2026-09-05T14:05:00.000Z",
  };
  const olderRetry = {
    key: "pending/20260905T120000000Z-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-WhatsApp Chat.zip",
    uploaded_at: "2026-09-05T12:00:30.000Z",
    discovered_at: "2026-09-05T12:01:00.000Z",
    acknowledged_at: "2026-09-05T15:00:00.000Z",
  };

  assert.equal(effectiveExportTimestamp(newer), "2026-09-05T14:00:30.000Z");
  assert.equal(shouldReplaceLatestReceipt(olderRetry, newer), false);
  assert.equal(shouldReplaceLatestReceipt(newer, olderRetry), true);
});

test("latest pointer falls back to the server-generated key time before discovery", () => {
  const receipt = {
    key: "pending/20260905T140000123Z-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-WhatsApp Chat.zip",
    uploaded_at: "not-a-timestamp",
    discovered_at: "2026-09-05T14:02:00.000Z",
    acknowledged_at: "2026-09-05T14:05:00.000Z",
  };
  assert.equal(effectiveExportTimestamp(receipt), "2026-09-05T14:00:00.123Z");
});

test("idempotent acknowledgement preserves a complete ready receipt", () => {
  const existing = { status: "ready", audit: { parsed_messages: 2741 } };
  const retry = { status: "sheet_readback_unverified", audit: {} };
  assert.equal(acknowledgementReceipt(existing, retry), existing);
  assert.equal(acknowledgementReceipt({ status: "audit_error" }, retry), retry);
});
