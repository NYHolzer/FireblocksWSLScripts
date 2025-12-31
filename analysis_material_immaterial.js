/**
 * Material vs Immaterial Wallets (Remaining work)
 *
 * Reads:
 *  - inventory/inventory.csv
 *  - plan/plan.csv
 *  - execute/completed_*.txt (all ledgers)
 *  - execute/asset_to_coingecko.json (optional)
 *  - execute/asset_price_basis.json  (optional)
 *  - execute/last_prices_usd.json    (optional)
 *
 * Writes:
 *  - analysis/material_immaterial_summary.txt
 *  - analysis/wallets_material.csv
 *  - analysis/wallets_immaterial.csv
 *  - analysis/wallets_unknown_price.csv
 *  - analysis/wallet_asset_details_remaining.csv
 *  - analysis/prices_used.json
 */

const fs = require("fs");
const path = require("path");
const https = require("https");

const ROOT = process.cwd();
const INV = path.join(ROOT, "inventory", "inventory.csv");
const PLAN = path.join(ROOT, "plan", "plan.csv");
const EXEC_DIR = path.join(ROOT, "execute");
const OUT_DIR = path.join(ROOT, "analysis");

if (!fs.existsSync(INV)) throw new Error("Missing inventory/inventory.csv");
if (!fs.existsSync(PLAN)) throw new Error("Missing plan/plan.csv");
fs.mkdirSync(OUT_DIR, { recursive: true });

function readJsonIfExists(p) {
  try {
    if (fs.existsSync(p)) return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {}
  return null;
}

const assetToCg = readJsonIfExists(path.join(ROOT, "execute", "asset_to_coingecko.json")) || {};
const assetToBasis = readJsonIfExists(path.join(ROOT, "execute", "asset_price_basis.json")) || {};
const basisPrices = readJsonIfExists(path.join(ROOT, "execute", "last_prices_usd.json")) || {};

const MIN_USD_PER_WALLET = Number(process.env.MIN_USD_PER_WALLET || "1");     // wallet-level materiality
const MIN_USD_PER_TX = Number(process.env.MIN_USD_PER_TX || "0.01");          // non-stable line-item floor
const STABLECOIN_MIN_USD = Number(process.env.STABLECOIN_MIN_USD || "0.25");  // stable line-item floor
const STRICT_PRICING = process.env.STRICT_PRICING === "1";                    // if 1: no stable fallback, unknown stays unknown
const USE_PLAN_SCOPE = process.env.USE_PLAN_SCOPE !== "0";                    // default true: only look at assets/wallets present in plan

function num(x) {
  if (x === null || x === undefined) return 0;
  const s = String(x).trim();
  if (!s) return 0;
  const n = Number(s);
  return Number.isFinite(n) ? n : 0;
}
function csvEscape(v) {
  if (v === null || v === undefined) return "";
  const s = String(v);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}
function isStableLike(assetId) {
  const a = String(assetId || "").toUpperCase();
  return a.includes("USDC") || a.includes("USDT") || a.includes("TUSD") || a.includes("BUSD");
}
function httpGetJson(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { "User-Agent": "fireblocks-analysis/1.0" } }, (res) => {
      let data = "";
      res.on("data", (ch) => (data += ch));
      res.on("end", () => {
        try {
          const json = JSON.parse(data);
          if (res.statusCode >= 200 && res.statusCode < 300) resolve(json);
          else reject(new Error(`HTTP ${res.statusCode}: ${data.slice(0, 2000)}`));
        } catch (e) {
          reject(new Error(`Bad JSON: ${e.message}; body=${data.slice(0, 400)}`));
        }
      });
    }).on("error", reject);
  });
}

function getIdx(header, candidates) {
  for (const c of candidates) {
    const i = header.indexOf(c);
    if (i >= 0) return i;
  }
  return -1;
}

function listCompletedLedgers() {
  if (!fs.existsSync(EXEC_DIR)) return [];
  return fs.readdirSync(EXEC_DIR)
    .filter(f => f.startsWith("completed_") && f.endsWith(".txt"))
    .map(f => path.join(EXEC_DIR, f));
}

function loadCompletedRowIds() {
  const ledgers = listCompletedLedgers();
  const done = new Set();
  for (const p of ledgers) {
    const lines = fs.readFileSync(p, "utf8").split(/\r?\n/).map(s => s.trim()).filter(Boolean);
    for (const l of lines) done.add(l);
  }
  return { done, ledgers };
}

function parsePlanCsv() {
  const txt = fs.readFileSync(PLAN, "utf8");
  const lines = txt.split(/\r?\n/).filter(Boolean);
  const header = lines[0].split(",").map(s => s.trim());
  const I = {
    sourceVaultId: getIdx(header, ["sourceVaultId"]),
    assetId: getIdx(header, ["assetId"]),
    amount: getIdx(header, ["amount"]),
    destinationVaultId: getIdx(header, ["destinationVaultId"]),
    requiresGas: getIdx(header, ["requiresGas"]),
    gasAssetId: getIdx(header, ["gasAssetId"]),
    gasReady: getIdx(header, ["gasReady"])
  };
  for (const k of Object.keys(I)) if (I[k] < 0) throw new Error(`plan.csv missing column: ${k}`);

  const rows = [];
  const scopeVaults = new Set();
  const scopeAssets = new Set();
  for (let i = 1; i < lines.length; i++) {
    const r = lines[i].split(",");
    const src = r[I.sourceVaultId];
    const asset = r[I.assetId];
    const dest = r[I.destinationVaultId];
    const amount = r[I.amount];
    const requiresGas = String(r[I.requiresGas]).toLowerCase() === "true";
    const gasAsset = r[I.gasAssetId] || "";
    const gasReady = String(r[I.gasReady]).toLowerCase() === "true";
    const rowId = `${src}|${asset}|${dest}`;
    rows.push({ rowId, src, asset, dest, amount, requiresGas, gasAsset, gasReady });
    scopeVaults.add(src);
    scopeAssets.add(asset);
  }
  return { rows, scopeVaults, scopeAssets };
}

function parseInventoryCsv() {
  const txt = fs.readFileSync(INV, "utf8");
  const lines = txt.split(/\r?\n/).filter(Boolean);
  const header = lines[0].split(",").map(s => s.trim());
  const I = {
    vaultId: getIdx(header, ["vaultAccountId","vaultId","id"]),
    vaultName: getIdx(header, ["vaultAccountName","name","vaultName"]),
    assetId: getIdx(header, ["assetId","asset"]),
    available: getIdx(header, ["available"]),
    total: getIdx(header, ["total","balance"])
  };
  if (I.vaultId < 0) throw new Error("inventory.csv missing vault id column (vaultAccountId/vaultId/id)");
  if (I.assetId < 0) throw new Error("inventory.csv missing assetId column");
  if (I.available < 0 && I.total < 0) throw new Error("inventory.csv missing available/total columns");

  const vaultNameById = new Map();
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const r = lines[i].split(",");
    const vaultId = r[I.vaultId];
    const vaultName = (I.vaultName >= 0 ? (r[I.vaultName] || "") : "");
    if (vaultName && !vaultNameById.has(vaultId)) vaultNameById.set(vaultId, vaultName);

    const assetId = r[I.assetId];
    const available = I.available >= 0 ? num(r[I.available]) : 0;
    const total = I.total >= 0 ? num(r[I.total]) : available;
    // keep only meaningful numeric rows (nonzero)
    if (!(total > 0 || available > 0)) continue;
    rows.push({ vaultId, vaultName, assetId, available, total });
  }
  return { rows, vaultNameById };
}

async function fetchCoingeckoPrices(neededCgIds) {
  const ids = [...neededCgIds];
  const out = {};
  const chunkSize = 250;
  for (let i = 0; i < ids.length; i += chunkSize) {
    const chunk = ids.slice(i, i + chunkSize);
    const url = `https://api.coingecko.com/api/v3/simple/price?ids=${encodeURIComponent(chunk.join(","))}&vs_currencies=usd`;
    const json = await httpGetJson(url);
    for (const [id, obj] of Object.entries(json || {})) {
      if (obj && typeof obj.usd === "number") out[id] = obj.usd;
    }
  }
  return out;
}

(async () => {
  const { done, ledgers } = loadCompletedRowIds();
  const { rows: planRows, scopeVaults, scopeAssets } = parsePlanCsv();
  const { rows: invRows, vaultNameById } = parseInventoryCsv();

  // remaining plan rows
  const remainingPlan = planRows.filter(r => !done.has(r.rowId));

  // scope (default: only what plan says we intend to move)
  const scopedVaults = USE_PLAN_SCOPE ? scopeVaults : new Set(invRows.map(r => r.vaultId));
  const scopedAssets = USE_PLAN_SCOPE ? scopeAssets : new Set(invRows.map(r => r.assetId));

  // build a set of (vaultId, assetId) pairs that are still "remaining work"
  const remainingPairs = new Set(remainingPlan.map(r => `${r.src}||${r.asset}`));

  // gather inventory rows relevant to remaining work
  const relevantInv = invRows.filter(r => {
    if (!scopedVaults.has(r.vaultId)) return false;
    if (!scopedAssets.has(r.assetId)) return false;
    // if using plan scope, narrow to only assets in remainingPlan
    if (USE_PLAN_SCOPE) return remainingPairs.has(`${r.vaultId}||${r.assetId}`);
    return true;
  });

  // determine which CoinGecko IDs we need
  const neededCg = new Set();
  for (const r of relevantInv) {
    const cg = assetToCg[r.assetId];
    if (cg) neededCg.add(cg);
  }

  // fetch live prices (best effort)
  let cgPrices = {};
  let cgOk = true;
  try {
    cgPrices = await fetchCoingeckoPrices(neededCg);
  } catch (e) {
    cgOk = false;
    console.error("WARN: CoinGecko fetch failed; continuing with basisPrices / fallback. Error:", String(e.message || e));
  }

  const priceUsed = {}; // assetId -> {method, usd, ref}
  function priceUsdFor(assetId) {
    // 1) direct CoinGecko mapping
    const cg = assetToCg[assetId];
    if (cg && typeof cgPrices[cg] === "number") {
      const p = cgPrices[cg];
      priceUsed[assetId] = { method: "coingecko_id", usd: p, ref: cg };
      return p;
    }

    // 2) basis mapping (assetId -> basis symbol -> USD)
    const basis = assetToBasis[assetId];
    if (basis && typeof basisPrices[basis] === "number") {
      const p = basisPrices[basis];
      priceUsed[assetId] = { method: "basis_symbol", usd: p, ref: basis };
      return p;
    }

    // 3) stable fallback (unless strict)
    if (!STRICT_PRICING && isStableLike(assetId)) {
      priceUsed[assetId] = { method: "stable_fallback", usd: 1.0, ref: "$1.00" };
      return 1.0;
    }

    priceUsed[assetId] = { method: "unknown", usd: null, ref: "" };
    return null;
  }

  // wallet aggregates (remaining work only)
  const walletAgg = new Map(); // vaultId -> {name, usdKnown, usdUnknown, itemCount, assets:{assetId:{total,usd?}}}
  function getWallet(vaultId) {
    if (!walletAgg.has(vaultId)) {
      walletAgg.set(vaultId, { vaultId, name: vaultNameById.get(vaultId) || "", usdKnown: 0, usdUnknown: 0, itemCount: 0, unknownCount: 0, assets: new Map() });
    }
    return walletAgg.get(vaultId);
  }

  // classify line items as "below min" per policy (for reporting)
  function belowMinUsd(assetId, usdValue) {
    if (usdValue === null) return false; // unknown is not "below min"; it's unknown
    if (isStableLike(assetId)) return usdValue < STABLECOIN_MIN_USD;
    return usdValue < MIN_USD_PER_TX;
  }

  let totalRows = 0;
  let belowMinRows = 0;
  let unknownPriceRows = 0;

  for (const r of relevantInv) {
    totalRows++;
    const p = priceUsdFor(r.assetId);
    const usd = (typeof p === "number") ? (r.total * p) : null;

    const w = getWallet(r.vaultId);
    w.itemCount++;

    // store asset detail
    w.assets.set(r.assetId, { assetId: r.assetId, amountTotal: r.total, amountAvailable: r.available, priceUsd: (typeof p === "number" ? p : null), usdValue: usd });

    if (usd === null) {
      w.usdUnknown += 0;
      w.unknownCount++;
      unknownPriceRows++;
    } else {
      if (belowMinUsd(r.assetId, usd)) belowMinRows++;
      w.usdKnown += usd;
    }
  }

  // wallet classification
  // - "material": known USD >= MIN_USD_PER_WALLET
  // - "immaterial": known USD < MIN_USD_PER_WALLET AND no unknown-priced assets
  // - "unknown": any unknown-priced assets (we break out separately, regardless of known USD)
  const walletsMaterial = [];
  const walletsImmaterial = [];
  const walletsUnknown = [];

  for (const w of walletAgg.values()) {
    const hasUnknown = w.unknownCount > 0;
    if (hasUnknown) walletsUnknown.push(w);
    else if (w.usdKnown >= MIN_USD_PER_WALLET) walletsMaterial.push(w);
    else walletsImmaterial.push(w);
  }

  // sort
  walletsMaterial.sort((a,b)=>b.usdKnown-a.usdKnown);
  walletsImmaterial.sort((a,b)=>b.usdKnown-a.usdKnown);
  walletsUnknown.sort((a,b)=> (b.usdKnown-a.usdKnown) || (b.unknownCount-a.unknownCount));

  // write wallet CSVs
  function writeWalletCsv(file, arr) {
    const rows = [
      "vaultId,vaultName,knownUsd,unknownAssetCount,itemCount"
    ];
    for (const w of arr) {
      rows.push([
        w.vaultId,
        csvEscape(w.name),
        w.usdKnown.toFixed(2),
        w.unknownCount,
        w.itemCount
      ].join(","));
    }
    fs.writeFileSync(path.join(OUT_DIR, file), rows.join("\n"));
  }

  writeWalletCsv("wallets_material.csv", walletsMaterial);
  writeWalletCsv("wallets_immaterial.csv", walletsImmaterial);
  writeWalletCsv("wallets_unknown_price.csv", walletsUnknown);

  // write wallet-asset details (remaining)
  const det = ["vaultId,vaultName,assetId,amountTotal,amountAvailable,priceUsd,usdValue,belowMinByPolicy"];
  const allWallets = [...walletAgg.values()];
  // stable ordering: by usd desc then vaultId
  allWallets.sort((a,b)=> (b.usdKnown-a.usdKnown) || (a.vaultId.localeCompare(b.vaultId)));
  for (const w of allWallets) {
    const assets = [...w.assets.values()].sort((a,b)=> ( (b.usdValue||0) - (a.usdValue||0) ));
    for (const a of assets) {
      const below = (a.usdValue===null) ? "" : (belowMinUsd(a.assetId, a.usdValue) ? "true" : "false");
      det.push([
        w.vaultId,
        csvEscape(w.name),
        a.assetId,
        a.amountTotal,
        a.amountAvailable,
        (a.priceUsd===null ? "" : a.priceUsd),
        (a.usdValue===null ? "" : a.usdValue.toFixed(6)),
        below
      ].join(","));
    }
  }
  fs.writeFileSync(path.join(OUT_DIR, "wallet_asset_details_remaining.csv"), det.join("\n"));

  // summary
  function sumKnown(arr){ return arr.reduce((s,w)=>s+w.usdKnown,0); }
  const knownMaterial = sumKnown(walletsMaterial);
  const knownImmaterial = sumKnown(walletsImmaterial);
  const knownUnknown = sumKnown(walletsUnknown);

  const summary = [];
  summary.push("Material vs Immaterial Wallets (Remaining Work)");
  summary.push("=============================================");
  summary.push("");
  summary.push("Inputs:");
  summary.push(`- ${INV}`);
  summary.push(`- ${PLAN}`);
  summary.push(`- Completed ledgers loaded (${ledgers.length}): ${ledgers.map(p=>path.basename(p)).join(", ") || "(none)"}`);
  summary.push("");
  summary.push("Pricing:");
  summary.push(`- CoinGecko live prices: ${cgOk ? "yes" : "no (fallback used)"}`);
  summary.push(`- STRICT_PRICING: ${STRICT_PRICING ? "1 (unknown stays unknown)" : "0 (stable fallback enabled)"}`);
  summary.push(`- MIN_USD_PER_WALLET: $${MIN_USD_PER_WALLET}`);
  summary.push(`- MIN_USD_PER_TX (non-stable): $${MIN_USD_PER_TX}`);
  summary.push(`- STABLECOIN_MIN_USD: $${STABLECOIN_MIN_USD}`);
  summary.push(`- USE_PLAN_SCOPE: ${USE_PLAN_SCOPE ? "1 (only remaining plan rows counted)" : "0 (all inventory counted)"}`);
  summary.push("");
  summary.push("Remaining work scope:");
  summary.push(`- Remaining plan rows (not completed): ${remainingPlan.length}`);
  summary.push(`- Relevant inventory rows (scoped):     ${totalRows}`);
  summary.push(`- Rows below min policy (priced only):  ${belowMinRows}`);
  summary.push(`- Rows with unknown price:              ${unknownPriceRows}`);
  summary.push("");
  summary.push("Wallet classification (remaining work only):");
  summary.push(`- MATERIAL wallets:   ${walletsMaterial.length}   (known USD total: $${knownMaterial.toFixed(2)})`);
  summary.push(`- IMMATERIAL wallets: ${walletsImmaterial.length} (known USD total: $${knownImmaterial.toFixed(2)})`);
  summary.push(`- UNKNOWN wallets:    ${walletsUnknown.length}    (known USD total: $${knownUnknown.toFixed(2)}, plus unknown-priced assets)`);
  summary.push("");
  summary.push("Outputs:");
  summary.push("- analysis/material_immaterial_summary.txt");
  summary.push("- analysis/wallets_material.csv");
  summary.push("- analysis/wallets_immaterial.csv");
  summary.push("- analysis/wallets_unknown_price.csv");
  summary.push("- analysis/wallet_asset_details_remaining.csv");
  summary.push("- analysis/prices_used.json");

  fs.writeFileSync(path.join(OUT_DIR, "material_immaterial_summary.txt"), summary.join("\n"));

  fs.writeFileSync(path.join(OUT_DIR, "prices_used.json"), JSON.stringify({
    asOf: new Date().toISOString(),
    priceUsed,
    basisPricesLoadedKeys: Object.keys(basisPrices),
    coingeckoFetchedCount: Object.keys(cgPrices).length
  }, null, 2));

  console.log("✅ Done. See:");
  console.log("- analysis/material_immaterial_summary.txt");
  console.log("- analysis/wallets_material.csv");
  console.log("- analysis/wallets_immaterial.csv");
  console.log("- analysis/wallets_unknown_price.csv");
  console.log("- analysis/wallet_asset_details_remaining.csv");
  console.log("- analysis/prices_used.json");
})();
