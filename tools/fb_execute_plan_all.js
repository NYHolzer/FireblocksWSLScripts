#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const readline = require("readline");

const API_KEY = process.env.FIREBLOCKS_API_KEY;
const PK_PATH = process.env.FIREBLOCKS_PRIVATE_KEY;
if (!API_KEY) throw new Error("Missing FIREBLOCKS_API_KEY");
if (!PK_PATH) throw new Error("Missing FIREBLOCKS_PRIVATE_KEY");

const BASE = process.env.FIREBLOCKS_BASE_URL || "https://api.fireblocks.io";
const EXECUTE = process.env.EXECUTE === "1";
const BATCH = Math.max(1, Math.min(500, Number(process.env.BATCH || "20") || 20));

const ROOT = process.cwd();
const PLAN_JSONL = path.join(ROOT, "plan", "plan.jsonl");
if (!fs.existsSync(PLAN_JSONL)) throw new Error(`Missing ${PLAN_JSONL}. Re-run plan.`);

const EXEC_DIR = path.join(ROOT, "execute");
fs.mkdirSync(EXEC_DIR, { recursive: true });

const RUN_ID = process.env.RUN_ID || `exec_all_${Date.now()}_${crypto.randomUUID()}`;
const JOURNAL = path.join(EXEC_DIR, `journal_${RUN_ID}.jsonl`);
const COMPLETED_PATH = path.join(EXEC_DIR, "completed_all.txt");
const FAILED_1402_PATH = path.join(EXEC_DIR,"failed_1402.txt");

// Optional: skip specific source vault(s) via env var: SKIP_SOURCE_VAULTS="94828,12345"
const SKIP_SOURCE_VAULTS = new Set((process.env.SKIP_SOURCE_VAULTS || "")
  .split(",").map(s => s.trim()).filter(Boolean));

const privateKeyPem = fs.readFileSync(PK_PATH, "utf8");

function base64url(x) {
  return Buffer.from(x).toString("base64")
    .replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
}
function sha256Hex(s) {
  return crypto.createHash("sha256").update(s).digest("hex");
}
function signJwt(uri, bodyStr) {
  const now = Math.floor(Date.now() / 1000);
  const payload = {
    uri,
    nonce: crypto.randomUUID(),
    iat: now,
    exp: now + 25,
    sub: API_KEY,
    bodyHash: sha256Hex(bodyStr || "")
  };
  const header = { alg: "RS256", typ: "JWT" };
  const data = `${base64url(JSON.stringify(header))}.${base64url(JSON.stringify(payload))}`;
  const sig = crypto.createSign("RSA-SHA256").update(data).end().sign(privateKeyPem);
  return `${data}.${base64url(sig)}`;
}
async function fbPost(uri, bodyObj, idempotencyKey) {
  const bodyStr = JSON.stringify(bodyObj);
  const jwt = signJwt(uri, bodyStr);
  const res = await fetch(`${BASE}${uri}`, {
    method: "POST",
    headers: {
      "X-API-Key": API_KEY,
      "Authorization": `Bearer ${jwt}`,
      "Content-Type": "application/json",
      "Idempotency-Key": idempotencyKey
    },
    body: bodyStr
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${text.slice(0, 2000)}`);
  return JSON.parse(text);
}

function normalizeAmountString(s) {
  const str = String(s ?? "").trim();
  if (!str) return "0";
  if (!/[eE]/.test(str)) return str;
  const m = str.match(/^([+-]?)(\d+)(?:\.(\d+))?[eE]([+-]?\d+)$/);
  if (!m) return str;
  const sign = m[1] === "-" ? "-" : "";
  const intPart = m[2], fracPart = m[3] || "", exp = parseInt(m[4], 10);
  const digits = (intPart + fracPart).replace(/^0+/, "") || "0";
  const decimalPos = intPart.length + exp;
  if (decimalPos <= 0) return sign + "0." + ("0".repeat(-decimalPos)) + digits;
  if (decimalPos >= digits.length) return sign + digits + ("0".repeat(decimalPos - digits.length));
  return sign + digits.slice(0, decimalPos) + "." + digits.slice(decimalPos);
}

function rowId(item) {
  return `${item.sourceVaultId}|${item.assetId}|${item.destinationVaultId}`;
}

function loadLedgers() {
  const completed = new Set();
  const failed1402 = new Set();

  if (fs.existsSync(COMPLETED_PATH)) {
    for (const l of fs.readFileSync(COMPLETED_PATH, "utf8")
      .split(/\r?\n/).map(s => s.trim()).filter(Boolean)) {
      completed.add(l);
    }
  }

  // Also read any historical journals for SUBMIT_OK
  const files = fs.readdirSync(EXEC_DIR).filter(f => /^journal_.*\.jsonl$/.test(f));
  for (const f of files) {
    const txt = fs.readFileSync(path.join(EXEC_DIR, f), "utf8");
    for (const line of txt.split(/\r?\n/)) {
      if (!line.trim()) continue;
      try {
        const j = JSON.parse(line);
        if (j.action === "SUBMIT_OK" && j.rowId) completed.add(j.rowId);
      } catch {}
    }
  }

  // Load permanent failures for code 1402
  if (fs.existsSync(FAILED_1402_PATH)) {
    for (const l of fs.readFileSync(FAILED_1402_PATH, "utf8")
      .split(/\r?\n/).map(s => s.trim()).filter(Boolean)) {
      failed1402.add(l);
    }
  }

  return { completed, failed1402 };
}


function isPositiveAmount(amountStr) {
  const n = Number(amountStr);
  return Number.isFinite(n) && n > 0;
}

(async () => {
  const { completed, failed1402 } = loadLedgers();

  console.log(`Mode: ${EXECUTE ? "EXECUTE (live)" : "DRY RUN"}`);
  console.log(`Batch size: ${BATCH}`);
  console.log(`Plan: ${PLAN_JSONL}`);
  console.log(`Journal: ${JOURNAL}`);
  console.log(`Completed ledger: ${COMPLETED_PATH}`);
  console.log(`Skip source vaults: ${SKIP_SOURCE_VAULTS.size ? Array.from(SKIP_SOURCE_VAULTS).join(",") : "(none)"}`);
  console.log(`Already completed loaded: ${completed.size}\n`);

  const journal = fs.createWriteStream(JOURNAL, { flags: "a", encoding: "utf8" });
  const completedAppend = fs.createWriteStream(COMPLETED_PATH, { flags: "a", encoding: "utf8" });
  const failed1402Append = fs.createWriteStream(FAILED_1402_PATH,{flags:"a",encoding:"utf8"});
  const rl = readline.createInterface({
    input: fs.createReadStream(PLAN_JSONL, { encoding: "utf8" }),
    crlfDelay: Infinity
  });

  let attempted = 0, failed = 0, skippedDone = 0, skippedVault = 0, skippedInvalid = 0;

  for await (const line of rl) {
    if (!line.trim()) continue;

    const item = JSON.parse(line);

    if (!item.assetId || item.sourceVaultId == null || item.destinationVaultId == null) {
      skippedInvalid++;
      continue;
    }
    if (String(item.sourceVaultId) === String(item.destinationVaultId)) {
      skippedInvalid++;
      continue;
    }
    if (SKIP_SOURCE_VAULTS.has(String(item.sourceVaultId))) {
      skippedVault++;
      continue;
    }

    const rid = rowId(item);
    if (completed.has(rid)) {
      skippedDone++;
      continue;
    }

    const amountStr = normalizeAmountString(item.amount);
    if (!isPositiveAmount(amountStr)) {
      skippedInvalid++;
      continue;
    }

    const externalTxId = `consolidate_${rid}`.slice(0, 190);
    const idempotencyKey = crypto.randomUUID();

    const body = {
      operation: "TRANSFER",
      assetId: item.assetId,
      source: { type: "VAULT_ACCOUNT", id: String(item.sourceVaultId) },
      destination: { type: "VAULT_ACCOUNT", id: String(item.destinationVaultId) },
      amount: amountStr,
      externalTxId,
      note: `Consolidation ${rid}`
    };

    try {
      if (!EXECUTE) {
        journal.write(JSON.stringify({ ts: new Date().toISOString(), action: "DRYRUN_WOULD_SUBMIT", rowId: rid, body }) + "\n");
      } else {
        const resp = await fbPost("/v1/transactions", body, idempotencyKey);
        journal.write(JSON.stringify({ ts: new Date().toISOString(), action: "SUBMIT_OK", rowId: rid, body, resp }) + "\n");
        completed.add(rid);
        completedAppend.write(rid + "\n");
        console.log(`SUBMIT_OK ${attempted + 1}/${BATCH}: ${rid} txId=${resp?.id || "?"}`);
      }
      attempted++;
      if (attempted >= BATCH) break;
    } catch (e) {
      failed++;
      journal.write(JSON.stringify({
        ts: new Date().toISOString(),
        action: "SUBMIT_FAIL",
        rowId: rid,
        body,
        error: String(e && e.stack ? e.stack : e)
      }) + "\n");
      console.log(`SUBMIT_FAIL: ${rid} :: ${String(e).slice(0, 240)}`);
    }
  }

  journal.end();
  completedAppend.end();
  if (typeof failed1402Append !== "undefined") failed1402Append.end();
  console.log(`\n✅ Batch complete
attempted_this_batch=${attempted}
failed_this_batch=${failed}
skipped_source_vault=${skippedVault}
skipped_invalid=${skippedInvalid}
skipped_already_done=${skippedDone}

Re-run to submit next batch of ${BATCH}.`);
})().catch(e => {
  console.error("ERROR:", e && e.stack ? e.stack : String(e));
  process.exit(1);
});
