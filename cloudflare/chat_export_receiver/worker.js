const DEFAULT_MAX_UPLOAD_BYTES = 512 * 1024 * 1024;
const DEFAULT_PRESIGN_TTL_SECONDS = 15 * 60;
const PENDING_PREFIX = "pending/";
const RECEIPT_PREFIX = "receipts/";
const encoder = new TextEncoder();

class HttpError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

function json(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "Cache-Control": "no-store",
      "Content-Type": "application/json; charset=utf-8",
    },
  });
}

function asPositiveInteger(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function maxUploadBytes(env) {
  return asPositiveInteger(env.MAX_UPLOAD_BYTES, DEFAULT_MAX_UPLOAD_BYTES);
}

function presignTtlSeconds(env) {
  return Math.min(asPositiveInteger(env.PRESIGN_TTL_SECONDS, DEFAULT_PRESIGN_TTL_SECONDS), 7 * 24 * 60 * 60);
}

function requireBucket(env) {
  if (!env.EXPORTS || !env.R2_BUCKET_NAME || !env.R2_ACCOUNT_ID || !env.R2_ACCESS_KEY_ID || !env.R2_SECRET_ACCESS_KEY) {
    throw new HttpError(503, "receiver is not configured");
  }
}

function constantTimeEquals(left, right) {
  if (typeof left !== "string" || typeof right !== "string") {
    return false;
  }
  const leftBytes = encoder.encode(left);
  const rightBytes = encoder.encode(right);
  let difference = leftBytes.length ^ rightBytes.length;
  const length = Math.max(leftBytes.length, rightBytes.length);
  for (let index = 0; index < length; index += 1) {
    difference |= (leftBytes[index] || 0) ^ (rightBytes[index] || 0);
  }
  return difference === 0;
}

function requireBearer(request, expectedToken) {
  const authorization = request.headers.get("Authorization") || "";
  const prefix = "Bearer ";
  const supplied = authorization.startsWith(prefix) ? authorization.slice(prefix.length) : "";
  if (!expectedToken || !constantTimeEquals(supplied, expectedToken)) {
    throw new HttpError(401, "unauthorized");
  }
}

async function readJson(request) {
  const text = await request.text();
  if (encoder.encode(text).byteLength > 8 * 1024) {
    throw new HttpError(413, "request body is too large");
  }
  try {
    return JSON.parse(text);
  } catch {
    throw new HttpError(400, "request body must be valid JSON");
  }
}

function normalizeFilename(value) {
  if (typeof value !== "string") {
    throw new HttpError(400, "filename is required");
  }
  const suppliedFilename = value.trim();
  // Shortcuts' Get Name action strips the extension from shared ZIP files.
  const filename = /\.(zip|txt)$/i.test(suppliedFilename) ? suppliedFilename : `${suppliedFilename}.zip`;
  if (
    filename.length < 12 ||
    filename.length > 180 ||
    /[\\/\u0000]/.test(filename) ||
    !/^whatsapp chat(?: - [a-z0-9][a-z0-9 _.()-]*)?\.(zip|txt)$/i.test(filename)
  ) {
    throw new HttpError(400, "filename must be a WhatsApp Chat .zip or .txt export");
  }
  return filename;
}

function parseUploadSize(value) {
  if (typeof value === "number") {
    return Number.isSafeInteger(value) ? value : NaN;
  }
  if (typeof value !== "string") {
    return NaN;
  }

  // Shortcuts may serialize its File Size value as display text (for example,
  // "196 MB") when the JSON field is configured as Text.
  const match = /^([0-9][0-9,]*(?:\.[0-9]+)?)\s*(bytes?|kb|mb|gb)?$/i.exec(value.trim());
  if (!match) {
    return NaN;
  }
  const amount = Number(match[1].replaceAll(",", ""));
  const unit = (match[2] || "bytes").toLowerCase();
  const multiplier = unit.startsWith("g") ? 1024 ** 3 : unit.startsWith("m") ? 1024 ** 2 : unit.startsWith("k") ? 1024 : 1;
  const sizeBytes = Math.round(amount * multiplier);
  return Number.isSafeInteger(sizeBytes) ? sizeBytes : NaN;
}

function requireUploadRequest(payload, env) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    throw new HttpError(400, "request body must be an object");
  }
  const filename = normalizeFilename(payload.filename);
  const sizeBytes = parseUploadSize(payload.size_bytes);
  if (!Number.isSafeInteger(sizeBytes) || sizeBytes <= 0 || sizeBytes > maxUploadBytes(env)) {
    throw new HttpError(400, `size_bytes must be between 1 and ${maxUploadBytes(env)}`);
  }
  return { filename, sizeBytes };
}

function encodeRfc3986(value) {
  return encodeURIComponent(value).replace(/[!'()*]/g, (character) => `%${character.charCodeAt(0).toString(16).toUpperCase()}`);
}

function canonicalObjectPath(key) {
  return `/${key.split("/").map(encodeRfc3986).join("/")}`;
}

function toHex(bytes) {
  return Array.from(bytes, (value) => value.toString(16).padStart(2, "0")).join("");
}

async function sha256(value) {
  const bytes = typeof value === "string" ? encoder.encode(value) : value;
  return new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
}

async function hmac(key, value) {
  const rawKey = typeof key === "string" ? encoder.encode(key) : key;
  const cryptoKey = await crypto.subtle.importKey("raw", rawKey, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  return new Uint8Array(await crypto.subtle.sign("HMAC", cryptoKey, encoder.encode(value)));
}

function amzTimestamp(now) {
  const year = now.getUTCFullYear().toString().padStart(4, "0");
  const month = (now.getUTCMonth() + 1).toString().padStart(2, "0");
  const day = now.getUTCDate().toString().padStart(2, "0");
  const hour = now.getUTCHours().toString().padStart(2, "0");
  const minute = now.getUTCMinutes().toString().padStart(2, "0");
  const second = now.getUTCSeconds().toString().padStart(2, "0");
  return `${year}${month}${day}T${hour}${minute}${second}Z`;
}

function canonicalQuery(parameters) {
  return Object.entries(parameters)
    .map(([key, value]) => [encodeRfc3986(key), encodeRfc3986(String(value))])
    .sort(([leftKey, leftValue], [rightKey, rightValue]) => {
      if (leftKey !== rightKey) {
        return leftKey < rightKey ? -1 : 1;
      }
      if (leftValue !== rightValue) {
        return leftValue < rightValue ? -1 : 1;
      }
      return 0;
    })
    .map(([key, value]) => `${key}=${value}`)
    .join("&");
}

async function presignObjectRequest(env, method, key, { contentType } = {}) {
  requireBucket(env);
  const now = new Date();
  const timestamp = amzTimestamp(now);
  const date = timestamp.slice(0, 8);
  const region = "auto";
  const service = "s3";
  const scope = `${date}/${region}/${service}/aws4_request`;
  const host = `${env.R2_BUCKET_NAME}.${env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`;
  const headers = contentType ? { "content-type": contentType, host } : { host };
  const signedHeaders = Object.keys(headers).sort().join(";");
  const canonicalHeaders = Object.keys(headers)
    .sort()
    .map((name) => `${name}:${headers[name]}\n`)
    .join("");
  const parameters = {
    "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
    "X-Amz-Content-Sha256": "UNSIGNED-PAYLOAD",
    "X-Amz-Credential": `${env.R2_ACCESS_KEY_ID}/${scope}`,
    "X-Amz-Date": timestamp,
    "X-Amz-Expires": presignTtlSeconds(env),
    "X-Amz-SignedHeaders": signedHeaders,
  };
  const path = canonicalObjectPath(key);
  const canonicalRequest = [
    method,
    path,
    canonicalQuery(parameters),
    canonicalHeaders,
    signedHeaders,
    "UNSIGNED-PAYLOAD",
  ].join("\n");
  const stringToSign = [
    "AWS4-HMAC-SHA256",
    timestamp,
    scope,
    toHex(await sha256(canonicalRequest)),
  ].join("\n");
  const dateKey = await hmac(`AWS4${env.R2_SECRET_ACCESS_KEY}`, date);
  const regionKey = await hmac(dateKey, region);
  const serviceKey = await hmac(regionKey, service);
  const signingKey = await hmac(serviceKey, "aws4_request");
  const signature = toHex(await hmac(signingKey, stringToSign));
  return `https://${host}${path}?${canonicalQuery({ ...parameters, "X-Amz-Signature": signature })}`;
}

function randomSegment() {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  return toHex(bytes);
}

function buildObjectKey(filename) {
  const timestamp = new Date().toISOString().replace(/[-:.]/g, "").replace("Z", "Z");
  return `${PENDING_PREFIX}${timestamp}-${randomSegment()}-${filename}`;
}

async function receiptKeyFor(objectKey) {
  return `${RECEIPT_PREFIX}${toHex(await sha256(objectKey))}.json`;
}

function filenameFromKey(objectKey) {
  const match = /^pending\/[^-]+-[a-f0-9]{32}-(.+)$/i.exec(objectKey);
  if (!match) {
    throw new HttpError(500, "stored export key has an unexpected format");
  }
  return match[1];
}

function requirePendingKey(value) {
  if (typeof value !== "string" || !value.startsWith(PENDING_PREFIX) || value.includes("..") || value.length > 512) {
    throw new HttpError(400, "invalid export key");
  }
  return value;
}

function compactAudit(payload) {
  const audit = payload && typeof payload === "object" && !Array.isArray(payload) ? payload : {};
  const allowed = [
    "parsed_messages",
    "audited_messages",
    "matched_messages",
    "missing_db_messages",
    "missing_decisions",
    "llm_review_required",
    "llm_review_completed",
    "llm_review_missing",
    "llm_review_failed",
    "review_roster_rows",
  ];
  return Object.fromEntries(
    allowed
      .map((key) => [key, Number(audit[key])])
      .filter(([, value]) => Number.isSafeInteger(value) && value >= 0),
  );
}

function receiptNeedsRetry(receipt) {
  if (!receipt || typeof receipt !== "object" || Array.isArray(receipt)) {
    return true;
  }
  const audit = receipt.audit;
  if (!audit || typeof audit !== "object" || Array.isArray(audit)) {
    return true;
  }
  const hasCompleteModelCounts = [
    "llm_review_required",
    "llm_review_completed",
    "llm_review_missing",
    "llm_review_failed",
  ].every((key) => Number.isSafeInteger(Number(audit[key])));
  if (hasCompleteModelCounts) {
    const required = Number(audit.llm_review_required || 0);
    const completed = Number(audit.llm_review_completed || 0);
    const missing = Number(audit.llm_review_missing || 0);
    const failed = Number(audit.llm_review_failed || 0);
    return missing > 0 || failed > 0 || required > completed;
  }
  // Legacy receipts did not preserve model-review counters. Requeue only when
  // that receipt explicitly recorded unresolved roster rows.
  return Number(audit.review_roster_rows || 0) > 0;
}

async function uploadIntent(request, env) {
  requireBucket(env);
  requireBearer(request, env.UPLOAD_AUTH_TOKEN);
  const { filename, sizeBytes } = requireUploadRequest(await readJson(request), env);
  const key = buildObjectKey(filename);
  const contentType = "application/octet-stream";
  const expiresIn = presignTtlSeconds(env);
  return json({
    key,
    filename,
    size_bytes: sizeBytes,
    upload_url: await presignObjectRequest(env, "PUT", key, { contentType }),
    required_headers: { "Content-Type": contentType },
    expires_at: new Date(Date.now() + expiresIn * 1000).toISOString(),
  }, 201);
}

async function pendingExports(request, env) {
  requireBucket(env);
  requireBearer(request, env.PULL_AUTH_TOKEN);
  const url = new URL(request.url);
  const cursor = url.searchParams.get("cursor");
  const listOptions = { prefix: PENDING_PREFIX, limit: 1000 };
  if (cursor) {
    listOptions.cursor = cursor;
  }
  const listing = await env.EXPORTS.list(listOptions);
  const exports = [];
  for (const object of listing.objects) {
    const receipt = await env.EXPORTS.get(await receiptKeyFor(object.key));
    if (receipt) {
      let receiptPayload = null;
      try {
        receiptPayload = await receipt.json();
      } catch {
        receiptPayload = null;
      }
      if (!receiptNeedsRetry(receiptPayload)) {
        continue;
      }
    }
    exports.push({
      key: object.key,
      filename: filenameFromKey(object.key),
      size_bytes: Number(object.size),
      uploaded_at: object.uploaded.toISOString(),
      download_url: await presignObjectRequest(env, "GET", object.key),
    });
  }
  return json({
    exports,
    truncated: Boolean(listing.truncated),
    cursor: listing.truncated && typeof listing.cursor === "string" ? listing.cursor : null,
  });
}

async function acknowledgeExport(request, env) {
  requireBucket(env);
  requireBearer(request, env.PULL_AUTH_TOKEN);
  const payload = await readJson(request);
  const key = requirePendingKey(payload.key);
  const receiptKey = await receiptKeyFor(key);
  const existingReceipt = await env.EXPORTS.head(receiptKey);
  if (!(await env.EXPORTS.head(key))) {
    throw new HttpError(404, "export was not found");
  }
  const sha256 = typeof payload.sha256 === "string" && /^[a-f0-9]{64}$/i.test(payload.sha256) ? payload.sha256.toLowerCase() : null;
  await env.EXPORTS.put(
    receiptKey,
    JSON.stringify({
      key,
      acknowledged_at: new Date().toISOString(),
      sha256,
      audit: compactAudit(payload.audit),
    }),
    { httpMetadata: { contentType: "application/json; charset=utf-8" } },
  );
  return json({ acknowledged: true, key, idempotent: Boolean(existingReceipt) });
}

async function health(env) {
  try {
    requireBucket(env);
    await env.EXPORTS.list({ prefix: PENDING_PREFIX, limit: 1 });
    return json({ ok: true, service: "tenant-chat-export-receiver", r2_ready: true });
  } catch {
    return json({ ok: false, service: "tenant-chat-export-receiver", r2_ready: false }, 503);
  }
}

async function route(request, env) {
  const url = new URL(request.url);
  if (request.method === "GET" && url.pathname === "/health") {
    return health(env);
  }
  if (request.method === "POST" && url.pathname === "/v1/uploads") {
    return uploadIntent(request, env);
  }
  if (request.method === "GET" && url.pathname === "/v1/exports") {
    return pendingExports(request, env);
  }
  if (request.method === "POST" && url.pathname === "/v1/exports/ack") {
    return acknowledgeExport(request, env);
  }
  throw new HttpError(404, "not found");
}

export default {
  async fetch(request, env) {
    try {
      return await route(request, env);
    } catch (error) {
      if (error instanceof HttpError) {
        return json({ error: error.message }, error.status);
      }
      return json({ error: "internal server error" }, 500);
    }
  },
};

export { compactAudit, normalizeFilename, parseUploadSize, receiptNeedsRetry };
