import assert from "node:assert/strict";
import test from "node:test";

import { normalizeFilename, parseUploadSize } from "./worker.js";

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
