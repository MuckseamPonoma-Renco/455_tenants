import assert from "node:assert/strict";
import test from "node:test";

import {
  compactAudit,
  normalizeFilename,
  parseUploadSize,
  receiptNeedsRetry,
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
      unsafe_detail: "must not be retained",
    }),
    {
      parsed_messages: 2509,
      llm_review_required: 197,
      llm_review_completed: 55,
      llm_review_missing: 104,
      llm_review_failed: 38,
    },
  );
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
