const fs = require("fs");
const path = require("path");
const https = require("https");

const ROOT = process.cwd();
const INV = path.join(ROOT, "inventory", "inventory.csv");
const VAULTS_JSON = path.join(ROOT, "inventory", "vaults.json");
const POLICY = path.join(ROOT, "policy.json");

if (!fs.existsSync(INV)) throw new Error("Missing inventory/inventory.csv");
if (!fs.existsSync(VAULTS_JSON)) throw new Error("Missing inventory/vaults.json");
if (!fs.existsSync(POLICY)) throw new Error("Missing policy.json");

const vaultMap = JSON.parse(fs.readFileSync(VAULTS_JSON, "utf8"));
const policy = JSON.parse(fs.readFileSync(POLICY, "utf8"));

const ANALYSIS_DIR = path.join(ROOT, "analysis");
fs.mkdirSync(ANALYSIS_DIR, { recursive: true });
fs.mkdirSync(path.join(ROOT, "execute"), { recursive: true });

/**
 * Pricing inputs:
 * - execute/asset_to_coingecko.json  (optional mapping for Fireblocks assetId -> coingecko id)
 * - execute/price_overrides_usd.json (optional manual overrides: { "ASSETID": 1.23, ... })
 * - execute/last_prices_usd.json     (cache written by this script)
 *
 * Env:
 *   PRICE_SOURCE=coingecko|cache|offline   (default coingecko)
 *   MATERIAL_WALLET_USD=1                  (default 1.00)
 *   MIN_USD_PER_TX=0.01                    (default 0.01)
 *   STABLECOIN_MIN_USD=0.25                (default 0.25)
 *   STABLECOIN_SYMBOLS="USDC,USDT,TUSD,BUSD" (default shown)
 */
const PRICE_SOURCE = (process.env.PRICE_SOURCE || "coingecko").toLowerCase();
const MATERIAL_WALLET_USD = Number(process.env.MATERIAL_WALLET_USD || "1");
const MIN_USD_PER_TX = Number(process.env.MIN_USD_PER_TX || "0.01");
const STABLECOIN_MIN_USD = Number(process.env.STABLECOIN_MIN_USD || "0.25");
const STABLECOIN_SYMBOLS = new Set(
  (process.env.STABLECOIN_SYMBOLS || "USDC,USDT,TUSD,BUSD").split(",").map(s => s.trim()).filter(Boolean)
);

if (!Number.isFinite(MATERIAL_WALLET_USD) || MATERIAL_WALLET_USD < 0) throw new Error("MATERIAL_WALLET_USD must be >= 0");
if (!Number.isFinite(MIN_USD_PER_TX) || MIN_USD_PER_TX < 0) throw new Error("MIN_USD_PER_TX must be >= 0");
if (!Number.isFinite(STABLECOIN_MIN_USD) || STABLECOIN_MIN_USD < 0) throw new Error("STABLECOIN_MIN_USD must be >= 0");

const MAP_PATH = path.join(ROOT, "execute", "asset_to_coingecko.json");
const OVERRIDE_PATH = path.join(ROOT, "execute", "price_overrides_usd.json");
const CACHE_PATH = path.join(ROOT, "execute", "last_prices_usd.json");

const assetToCg = fs.existsSync(MAP_PATH) ? JSON.parse(fs.readFileSync(MAP_PATH, "utf8")) : {};
const overrides = fs.existsSync(OVERRIDE_PATH) ? JSON.parse(fs.readFileSync(OVERRIDE_PATH, "utf8")) : {};
const cachedPrices = fs.existsSync(CACHE_PATH) ? JSON.parse(fs.readFileSync(CACHE_PATH, "utf8")) : {};

function isStable(assetId) {
  // treat by prefix/symbol presence; user can tune via STABLECOIN_SYMBOLS
  const upper = String(assetId || "").toUpperCase();
  for (const sym of STABLECOIN_SYMBOLS) {
    if (upper === sym) return true;
    if (upper.startsWith(sym + "_")) return true;
  }
  return false;
}

// Gas dependency map: tokenGasMap in policy.json: { "USDC": "ETH", "USDC_POLYGON": "MATIC", ... }
// Some assets may have "" or null; treat as no gas.
const tokenGasMap = policy?.tokenGasMap || {};
// Optional per-gas-asset minimum reserve to consider “gas ready”
const gasReserveMin = policy?.gasReserveMin || {
  ETH: 0.0005,
  MATIC: 0.2,
  BNB: 0.002,
  SOL: 0.01,
  FTM: 0.5,
  TRX: 5,
  XLM: 1,
  ALGO: 0.001
};

function httpGetJson(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { "User-Agent": "fb-analysis/1.0" } }, (res) => {
      let data = "";
      res.on("data", (c) => (data += c));
      res.on("end", () => {
        if (res.statusCode < 200 || res.statusCode >= 300) {
          return reject(new Error(`HTTP ${res.statusCode}: ${data.slice(0, 500)}`));
        }
        try { resolve(JSON.parse(data)); }
        catch (e) { reject(e); }
      });
    }).on("error", reject);
  });
}

async function loadPricesUsd(assetIds) {
  // 1) apply overrides first
  const prices = {};
  for (const a of assetIds) {
    if (overrides[a] !== undefined) prices[a] = Number(overrides[a]);
  }

  if (PRICE_SOURCE === "offline") {
    // Only overrides are used
    return prices;
  }

  if (PRICE_SOURCE === "cache") {
    for (const a of assetIds) {
      if (prices[a] !== undefined) continue;
      if (cachedPrices[a] !== undefined) prices[a] = Number(cachedPrices[a]);
    }
    return prices;
  }

  // coingecko: map to CG ids where possible; otherwise fall back to cache if present
  const ids = [];
  const cgToAsset = {};
  for (const a of assetIds) {
    if (prices[a] !== undefined) continue;
    const cg = assetToCg[a];
    if (cg) {
      ids.push(cg);
      cgToAsset[cg] = cgToAsset[cg] || [];
      cgToAsset[cg].push(a);
    } else if (cachedPrices[a] !== undefined) {
      prices[a] = Number(cachedPrices[a]);
    }
  }

  const uniqIds = Array.from(new Set(ids));
  if (uniqIds.length === 0) return prices;

  // batch coingecko calls (avoid huge URL)
  const chunkSize = 150;
  for (let i = 0; i < uniqIds.length; i += chunkSize) {
    const chunk = uniqIds.slice(i, i + chunkSize);
    const url = "https://api.coingecko.com/api/v3/simple/price?vs_currencies=usd&ids=" + encodeURIComponent(chunk.join(","));
    const json = await httpGetJson(url);
    for (const cgId of Object.keys(json || {})) {
      const usd = json?.[cgId]?.usd;
      if (usd === undefined || usd === null) continue;
      const mappedAssets = cgToAsset[cgId] || [];
      for (const a of mappedAssets) prices[a] = Number(usd);
    }
  }

  // update cache
  const newCache = { ...(cachedPrices || {}) };
  for (const [k, v] of Object.entries(prices)) {
    if (Number.isFinite(v)) newCache[k] = v;
  }
  fs.writeFileSync(CACHE_PATH, JSON.stringify(newCache, null, 2));
  return prices;
}

function num(s) {
  const n = Number(String(s ?? "").trim());
  return Number.isFinite(n) ? n : 0;
}
function csvEscape(v) {
  if (v === null || v === undefined) return "";
  const s = String(v);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

// Read inventory.csv
const lines = fs.readFileSync(INV, "utf8").split(/\r?\n/).filter(Boolean);
const hdr = lines[0].split(",");
const I = {
  v: hdr.indexOf("vaultId"),
  n: hdr.indexOf("vaultName"),
  a: hdr.indexOf("assetId"),
  av: hdr.indexOf("available"),
  t: hdr.indexOf("total"),
  // columns may exist, but not required for this analysis:
};
if (I.v < 0 || I.a < 0 || I.av < 0 || I.t < 0) throw new Error("inventory.csv missing required columns");

const perAsset = new Map();  // assetId -> {rowCount, vaultSet, sumAvail, sumTotal, totals[]}
const perVault = new Map();  // vaultId -> {name, hidden, assets: Map(assetId -> {avail,total}), usdKnown, usdUnknownAssets: Set}
const assetSet = new Set();

for (let i = 1; i < lines.length; i++) {
  const row = lines[i].split(",");
  const vaultId = row[I.v];
  const vaultName = row[I.n] || (vaultMap[vaultId]?.name ?? "");
  const assetId = row[I.a];
  const avail = num(row[I.av]);
  const total = num(row[I.t]);

  if (!(total > 0 || avail > 0)) continue;

  assetSet.add(assetId);

  // per-asset aggregation
  if (!perAsset.has(assetId)) perAsset.set(assetId, { rowCount: 0, vaultSet: new Set(), sumAvail: 0, sumTotal: 0, totals: [] });
  const A = perAsset.get(assetId);
  A.rowCount++;
  A.vaultSet.add(vaultId);
  A.sumAvail += avail;
  A.sumTotal += total;
  A.totals.push(total);

  // per-vault aggregation
  if (!perVault.has(vaultId)) {
    perVault.set(vaultId, {
      vaultId,
      name: vaultName,
      hiddenOnUI: vaultMap[vaultId]?.hiddenOnUI === true,
      assets: new Map(),
      usdKnown: 0,
      usdUnknownAssets: new Set()
    });
  }
  const V = perVault.get(vaultId);
  V.name = V.name || vaultName;
  const prev = V.assets.get(assetId) || { avail: 0, total: 0 };
  V.assets.set(assetId, { avail: prev.avail + avail, total: prev.total + total });
}

function percentile(arr, p) {
  if (!arr.length) return 0;
  const a = Array.from(arr).sort((x, y) => x - y);
  const idx = (a.length - 1) * p;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return a[lo];
  const w = idx - lo;
  return a[lo] * (1 - w) + a[hi] * w;
}

(async () => {
  const assets = Array.from(assetSet);
  const prices = await loadPricesUsd(assets);

  // Asset coverage output
  const assetCoveragePath = path.join(ANALYSIS_DIR, "asset_coverage.csv");
  const assetCoverage = fs.createWriteStream(assetCoveragePath, { encoding: "utf8" });
  assetCoverage.write([
    "assetId","vaultCount","rowCount",
    "sumAvailable","sumTotal",
    "priceUsd","priceUsdMethod",
    "usdTotalKnownPrices","unknownPriceRows",
    "medianTotal","p90Total","maxTotal"
  ].join(",") + "\n");

  // Wallet summary output
  const walletSummaryPath = path.join(ANALYSIS_DIR, "wallet_summary.csv");
  const walletSummary = fs.createWriteStream(walletSummaryPath, { encoding: "utf8" });
  walletSummary.write([
    "vaultId","vaultName","hiddenOnUI",
    "assetCount","usdKnown","unknownAssetCount",
    "classification","notes"
  ].join(",") + "\n");

  // Gas needs output
  const gasNeedsPath = path.join(ANALYSIS_DIR, "wallets_needing_gas.csv");
  const gasNeeds = fs.createWriteStream(gasNeedsPath, { encoding: "utf8" });
  gasNeeds.write([
    "vaultId","vaultName","hiddenOnUI",
    "gasAsset","gasAvailable","gasReserveMin",
    "blockedAssets","blockedUsdKnown"
  ].join(",") + "\n");

  // Compute per-asset stats
  for (const [assetId, A] of perAsset.entries()) {
    const price = prices[assetId];
    const hasPrice = Number.isFinite(price);
    const usdTotal = hasPrice ? A.sumTotal * price : 0;
    const method = overrides[assetId] !== undefined ? "override" :
                   assetToCg[assetId] ? "coingecko" :
                   cachedPrices[assetId] !== undefined ? "cache" : "unknown";
    const unknownRows = hasPrice ? 0 : A.rowCount;

    assetCoverage.write([
      assetId,
      A.vaultSet.size,
      A.rowCount,
      A.sumAvail,
      A.sumTotal,
      hasPrice ? price : "",
      method,
      hasPrice ? usdTotal : "",
      unknownRows,
      percentile(A.totals, 0.5),
      percentile(A.totals, 0.9),
      Math.max(...A.totals)
    ].map(csvEscape).join(",") + "\n");
  }
  assetCoverage.end();

  // Compute per-vault USD totals and classifications + gas needs
  let materialWallets = 0, immaterialWallets = 0;
  let materialUsd = 0, immaterialUsd = 0;
  let unknownPriceWallets = 0;

  for (const V of perVault.values()) {
    let usdKnown = 0;
    let unknown = 0;

    // gas check: determine blocked assets by missing reserve
    const blockedByGas = new Map(); // gasAsset -> [{assetId, usd}]
    const gasAvailByAsset = new Map();

    for (const [assetId, bal] of V.assets.entries()) {
      const price = prices[assetId];
      if (Number.isFinite(price)) usdKnown += bal.total * price;
      else unknown++;

      // Track gas balances available in wallet
      gasAvailByAsset.set(assetId, (gasAvailByAsset.get(assetId) || 0) + bal.available);
    }

    // Identify gas-blocked assets (based on tokenGasMap)
    let blockedUsdKnown = 0;
    for (const [assetId, bal] of V.assets.entries()) {
      const gasAsset = tokenGasMap[assetId];
      if (!gasAsset) continue; // no gas dependency defined

      const requiredReserve = Number(gasReserveMin[gasAsset] ?? 0);
      const gasAvail = Number(gasAvailByAsset.get(gasAsset) ?? 0);

      // If gas not present or under reserve, treat as blocked
      if (gasAvail < requiredReserve) {
        const price = prices[assetId];
        const usd = Number.isFinite(price) ? bal.total * price : 0;
        blockedUsdKnown += usd;

        if (!blockedByGas.has(gasAsset)) blockedByGas.set(gasAsset, []);
        blockedByGas.get(gasAsset).push({ assetId, usd });
      }
    }

    const classification = usdKnown >= MATERIAL_WALLET_USD ? "MATERIAL" : "IMMATERIAL";
    const notes = [];
    if (unknown > 0) { unknownPriceWallets++; notes.push(`unknown_assets=${unknown}`); }
    if (blockedByGas.size > 0) notes.push(`needs_gas=${blockedByGas.size}`);

    walletSummary.write([
      V.vaultId,
      V.name || "",
      V.hiddenOnUI ? "true" : "false",
      V.assets.size,
      usdKnown,
      unknown,
      classification,
      notes.join(";")
    ].map(csvEscape).join(",") + "\n");

    if (classification === "MATERIAL") { materialWallets++; materialUsd += usdKnown; }
    else { immaterialWallets++; immaterialUsd += usdKnown; }

    // write gas-needs rows
    for (const [gasAsset, items] of blockedByGas.entries()) {
      const gasAvail = Number(gasAvailByAsset.get(gasAsset) ?? 0);
      const reserve = Number(gasReserveMin[gasAsset] ?? 0);
      const blockedAssets = items
        .sort((a,b) => (b.usd - a.usd))
        .slice(0, 25)
        .map(x => `${x.assetId}${Number.isFinite(x.usd) && x.usd>0 ? `($${x.usd.toFixed(2)})` : ""}`)
        .join(" | ");
      gasNeeds.write([
        V.vaultId,
        V.name || "",
        V.hiddenOnUI ? "true" : "false",
        gasAsset,
        gasAvail,
        reserve,
        blockedAssets,
        blockedUsdKnown
      ].map(csvEscape).join(",") + "\n");
    }
  }

  walletSummary.end();
  gasNeeds.end();

  // High-level rollup
  const rollupPath = path.join(ANALYSIS_DIR, "inventory_rollup.txt");
  const rollup = [
    "Fireblocks Inventory Rollup (Refreshed)",
    "======================================",
    `Inventory file: ${INV}`,
    `Vault map:       ${VAULTS_JSON}`,
    "",
    `Pricing source: ${PRICE_SOURCE}`,
    `Material wallet threshold (USD): ${MATERIAL_WALLET_USD}`,
    `Per-tx floors: MIN_USD_PER_TX=${MIN_USD_PER_TX} STABLECOIN_MIN_USD=${STABLECOIN_MIN_USD}`,
    "",
    `Wallets with any nonzero assets: ${perVault.size}`,
    `Material wallets (>= threshold): ${materialWallets}  (usdKnown=$${materialUsd.toFixed(2)})`,
    `Immaterial wallets (< threshold): ${immaterialWallets} (usdKnown=$${immaterialUsd.toFixed(2)})`,
    `Wallets containing unknown-priced assets: ${unknownPriceWallets}`,
    "",
    "Outputs:",
    `- ${assetCoveragePath}`,
    `- ${walletSummaryPath}`,
    `- ${gasNeedsPath}`,
    `- ${CACHE_PATH} (price cache)`
  ].join("\n");
  fs.writeFileSync(rollupPath, rollup);
  console.log(rollup);
})().catch(e => {
  console.error("ERROR:", e && e.stack ? e.stack : String(e));
  process.exit(1);
});
