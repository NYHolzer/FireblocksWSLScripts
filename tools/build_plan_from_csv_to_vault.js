#!/usr/bin/env node
"use strict";

const fs = require("fs");

const [,, csvPath, destVaultId] = process.argv;
if (!csvPath || !destVaultId) {
  console.error("Usage: node tools/build_plan_from_csv_to_vault.js <CSV_PATH> <DEST_VAULT_ID>");
  process.exit(2);
}

// Leave 1 unit behind for these assets (per source vault)
const LEAVE_ONE = new Set(["DOT", "XRP", "XLM"]);

function splitCsvLine(line) {
  const out = [];
  let cur = "", inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') {
      if (inQ && line[i + 1] === '"') { cur += '"'; i++; }
      else inQ = !inQ;
    } else if (c === ',' && !inQ) {
      out.push(cur); cur = "";
    } else {
      cur += c;
    }
  }
  out.push(cur);
  return out;
}

function toNum(x) {
  const n = Number(String(x ?? "").trim());
  return Number.isFinite(n) ? n : NaN;
}

const txt = fs.readFileSync(csvPath, "utf8");
const lines = txt.split(/\r?\n/).filter(l => l.trim().length);
if (lines.length < 2) throw new Error("CSV appears empty");

const header = splitCsvLine(lines[0]).map(s => s.trim());
const idx = (name) => header.findIndex(h => h === name);

const iAccountId = idx("Account ID");
const iAssetId   = idx("Asset ID");
const iTotal     = idx("Total Balance");

if (iAccountId < 0 || iAssetId < 0 || iTotal < 0) {
  throw new Error("Missing required columns. Need: Account ID, Asset ID, Total Balance");
}

const outDir = "plan";
fs.mkdirSync(outDir, { recursive: true });
const outPath = `${outDir}/plan_from_csv_to_${destVaultId}.jsonl`;

let written = 0;
let skippedZero = 0;
let skippedSelf = 0;
let reservedApplied = 0;

const ws = fs.createWriteStream(outPath, { flags: "w", encoding: "utf8" });

for (let li = 1; li < lines.length; li++) {
  const cols = splitCsvLine(lines[li]);

  const sourceVaultId = (cols[iAccountId] || "").trim();
  const assetId = (cols[iAssetId] || "").trim();
  const amountRaw = (cols[iTotal] || "").trim();

  if (!sourceVaultId || !assetId || !amountRaw) continue;
  if (String(sourceVaultId) === String(destVaultId)) { skippedSelf++; continue; }

  const bal = toNum(amountRaw);
  if (!Number.isFinite(bal) || bal <= 0) { skippedZero++; continue; }

  // Apply reserve rule: keep 1 in each wallet for DOT/XRP/XLM
  let move = bal;
  if (LEAVE_ONE.has(assetId)) {
    move = bal - 1;
    reservedApplied++;
  }

  if (!(Number.isFinite(move) && move > 0)) { skippedZero++; continue; }

  // Keep original numeric string as much as possible; but since we computed, stringify safely
  // Avoid scientific notation by using toFixed when needed:
  let moveStr = String(move);
  if (/[eE]/.test(moveStr)) moveStr = move.toFixed(18).replace(/\.?0+$/,"");

  const obj = {
    assetId,
    sourceVaultId,
    destinationVaultId: String(destVaultId),
    amount: moveStr,
    reserveRule: LEAVE_ONE.has(assetId) ? "leave_1" : "none"
  };

  ws.write(JSON.stringify(obj) + "\n");
  written++;
}

ws.end();

console.log(`Wrote ${written} moves to ${outPath}`);
console.log(`Skipped zero/non-numeric (or <= reserve): ${skippedZero}`);
console.log(`Skipped source==dest: ${skippedSelf}`);
console.log(`Reserve rule applied rows (DOT/XRP/XLM): ${reservedApplied}`);
