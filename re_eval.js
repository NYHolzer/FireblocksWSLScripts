/**
 * Re-evaluate Fireblocks vaults:
 * - fetch live prices (CoinGecko) using execute/asset_to_coingecko.json mapping
 * - compute wallet USD totals from inventory.csv
 * - classify wallets into MATERIAL vs IMMATERIAL
 * - compute gas-needs wallets (requiresGas && !gasReady) with vault names
 *
 * Inputs:
 *  - inventory/inventory.csv
 *  - plan/plan.csv
 *  - execute/asset_to_coingecko.json
 *
 * Outputs:
 *  - execute/last_prices_usd.json
 *  - analysis/re_eval_summary.txt
 *  - analysis/prices_used.csv
 *  - analysis/wallets_material.csv
 *  - analysis/wallets_immaterial.csv
 *  - analysis/gas_needs_wallets_with_names.csv
 */

const fs = require("fs");
const path = require("path");
const https = require("https");

const ROOT = process.cwd();
const INV = path.join(ROOT, "inventory", "inventory.csv");
const PLAN = path.join(ROOT, "plan", "plan.csv");
const MAP = path.join(ROOT, "execute", "asset_to_coingecko.json");
const OUT_DIR = path.join(ROOT, "analysis");
const PRICE_CACHE = path.join(ROOT, "execute", "last_prices_usd.json");

fs.mkdirSync(OUT_DIR, { recursive: true });

const MIN_USD_PER_TX = Number(process.env.MIN_USD_PER_TX || "0.01");        // non-stable
const STABLECOIN_MIN_USD = Number(process.env.STABLECOIN_MIN_USD || "0.25"); // stablecoins (you can tune)
const MATERIAL_WALLET_USD = Number(process.env.MATERIAL_WALLET_USD || "0.01"); // if wallet total >= this -> material bucket

if (!Number.isFinite(MIN_USD_PER_TX) || MIN_USD_PER_TX < 0) throw new Error("MIN_USD_PER_TX must be >= 0");
if (!Number.isFinite(STABLECOIN_MIN_USD) || STABLECOIN_MIN_USD < 0) throw new Error("STABLECOIN_MIN_USD must be >= 0");
if (!Number.isFinite(MATERIAL_WALLET_USD) || MATERIAL_WALLET_USD < 0) throw new Error("MATERIAL_WALLET_USD must be >= 0");

function readCsv(filePath) {
  const txt = fs.readFileSync(filePath, "utf8");
  const lines = txt.split(/\r?\n/).filter(Boolean);
  const header = lines[0].split(",").map(s => s.trim());
  const rows = [];
  for (let i = 1; i < lines.length; i++) rows.push(lines[i].split(","));
  return { header, rows };
}

function num(x) {
  if (x === null || x === undefined) return 0;
  const s = String(x).trim();
  if (!s) return 0;
  const n = Number(s);
  return Number.isFinite(n) ? n : 0;
}

function getIdx(header, candidates) {
  for (const c of candidates) {
    const i = header.indexOf(c);
    if (i >= 0) return i;
  }
  return -1;
}

function httpGetJson(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { "User-Agent": "fireblocks-re-eval/1.0" } }, (res) => {
      let data = "";
      res.on("data", (chunk) => data += chunk);
      res.on("end", () => {
        try {
          const json = JSON.parse(data);
          if (res.statusCode && res.statusCode >= 200 && res.statusCode < 300) resolve(json);
          else reject(new Error(`HTTP ${res.statusCode}: ${data.slice(0, 2000)}`));
        } catch (e) {
          reject(new Error(`Bad JSON: ${e.message}; body=${data.slice(0, 500)}`));
        }
      });
    }).on("error", reject);
  });
}

function isStable(assetId) {
  // pragmatic rule; tune as you like
  const a = assetId.toUpperCase();
  return a.includes("USDC") || a.includes("USDT") || a.includes("TUSD") || a.includes("BUSD");
}

async function fetchPricesUsd(assetToCg) {
  const cgIds = [...new Set(Object.values(assetToCg).filter(Boolean))];
  if (cgIds.length === 0) return { prices: {}, unmappedAssets: [] };

  // CoinGecko simple price endpoint (public)
  // docs: https://docs.coingecko.com/reference/simple-price
  const chunks = [];
  const chunkSize = 250; // safe chunking
  for (let i = 0; i < cgIds.length; i += chunkSize) chunks.push(cgIds.slice(i, i + chunkSize));

  const prices = {};
  for (let i = 0; i < chunks.length; i++) {
    const ids = encodeURIComponent(chunks[i].join(","));
    const url = `https://api.coingecko.com/api/v3/simple/price?ids=${ids}&vs_currencies=usd`;
    const json = await httpGetJson(url);
    for (const [id, obj] of Object.entries(json || {})) {
      const p = obj && typeof obj.usd === "number" ? obj.usd : null;
      if (p !== null) prices[id] = p;
    }
  }

  const nowIso = new Date().toISOString();
  fs.writeFileSync(PRICE_CACHE, JSON.stringify({ asOf: nowIso, source: "coingecko/simple/price", prices }, null, 2));
  return { prices, asOf: nowIso };
}

(async () => {
  if (!fs.existsSync(INV)) throw new Error(`Missing ${INV}`);
  if (!fs.existsSync(PLAN)) throw new Error(`Missing ${PLAN}`);
  if (!fs.existsSync(MAP)) throw new Error(`Missing ${MAP}`);

  const assetToCg = JSON.parse(fs.readFileSync(MAP, "utf8"));
  const { prices: cgPrices, asOf } = await fetchPricesUsd(assetToCg);

  // Build Fireblocks assetId -> USD price map
  const priceByAsset = {};
  for (const [assetId, cgId] of Object.entries(assetToCg)) {
    if (!cgId) continue;
    const p = cgPrices[cgId];
    if (typeof p === "number") priceByAsset[assetId] = p;
  }

  // Read inventory
  const inv = readCsv(INV);
  const I = {
    vaultId: getIdx(inv.header, ["vaultAccountId", "vaultId", "id"]),
    vaultName: getIdx(inv.header, ["vaultAccountName", "name", "vaultName"]),
    assetId: getIdx(inv.header, ["assetId", "asset", "id.asset"]),
    available: getIdx(inv.header, ["available"]),
    total: getIdx(inv.header, ["total", "balance"])
  };

  if (I.vaultId < 0) throw new Error(`inventory.csv missing vault id column (expected vaultAccountId/vaultId/id). Header: ${inv.header.join(",")}`);
  if (I.assetId < 0) throw new Error(`inventory.csv missing assetId column. Header: ${inv.header.join(",")}`);
  if (I.total < 0 && I.available < 0) throw new Error(`inventory.csv missing total/balance/available columns. Header: ${inv.header.join(",")}`);

  // Aggregate per wallet
  const wallets = new Map(); // vaultId -> {name, totalUsd, assets: Map(assetId -> amountTotal)}
  const unknownPriceAssets = new Set();

  for (const r of inv.rows) {
    const vaultId = r[I.vaultId];
    const vaultName = I.vaultName >= 0 ? (r[I.vaultName] || "") : "";
    const assetId = r[I.assetId];
    const totalAmt = I.total >= 0 ? num(r[I.total]) : 0;
    const availAmt = I.available >= 0 ? num(r[I.available]) : 0;

    const amt = totalAmt > 0 ? totalAmt : availAmt;
    if (!(amt > 0)) continue;

    if (!wallets.has(vaultId)) wallets.set(vaultId, { vaultId, name: vaultName, totalUsd: 0, assets: new Map() });
    const w = wallets.get(vaultId);

    w.assets.set(assetId, (w.assets.get(assetId) || 0) + amt);

    const p = priceByAsset[assetId];
    if (typeof p === "number") {
      w.totalUsd += amt * p;
    } else {
      unknownPriceAssets.add(assetId);
      // no USD addition for unknown; we will track separately
    }
  }

  // Read plan and compute gas-needs wallets (requiresGas && gasReady==false)
  const plan = readCsv(PLAN);
  const P = {
    source: getIdx(plan.header, ["sourceVaultId"]),
    asset: getIdx(plan.header, ["assetId"]),
    amount: getIdx(plan.header, ["amount"]),
    dest: getIdx(plan.header, ["destinationVaultId"]),
    requiresGas: getIdx(plan.header, ["requiresGas"]),
    gasAsset: getIdx(plan.header, ["gasAssetId"]),
    gasReady: getIdx(plan.header, ["gasReady"])
  };
  for (const k of Object.values(P)) if (k < 0) throw new Error(`plan.csv missing columns; header=${plan.header.join(",")}`);

  // Build walletId -> set of required gas assets + dependent assets + dependent USD (where price known)
  const gasNeeds = new Map(); // vaultId -> {name, gasAssets:Set, deps:Set, depsUsd:number}
  for (const r of plan.rows) {
    const source = r[P.source];
    const assetId = r[P.asset];
    const amt = num(r[P.amount]);
    const reqGas = String(r[P.requiresGas]).trim().toLowerCase() === "true";
    const gasReady = String(r[P.gasReady]).trim().toLowerCase() === "true";
    const gasAsset = (r[P.gasAsset] || "").trim();

    if (!(reqGas && !gasReady)) continue;

    const w = wallets.get(source);
    const name = w ? w.name : "";

    if (!gasNeeds.has(source)) gasNeeds.set(source, { vaultId: source, name, gasAssets: new Set(), deps: new Set(), depsUsd: 0 });
    const g = gasNeeds.get(source);
    if (gasAsset) g.gasAssets.add(gasAsset);
    g.deps.add(assetId);

    const p = priceByAsset[assetId];
    if (typeof p === "number") g.depsUsd += amt * p;
  }

  // Material vs immaterial wallets
  const material = [];
  const immaterial = [];

  for (const w of wallets.values()) {
    const bucket = (w.totalUsd >= MATERIAL_WALLET_USD) ? material : immaterial;
    bucket.push(w);
  }

  // Sort biggest first
  material.sort((a,b)=> b.totalUsd - a.totalUsd);
  immaterial.sort((a,b)=> b.totalUsd - a.totalUsd);

  // Summaries
  const sumUsd = (arr)=> arr.reduce((s,x)=> s + (x.totalUsd||0), 0);

  const summary = [];
  summary.push("Fireblocks Re-Evaluation Summary");
  summary.push("================================");
  summary.push("");
  summary.push(`Prices as-of: ${asOf} (CoinGecko /simple/price)`);
  summary.push(`Material wallet threshold (USD): ${MATERIAL_WALLET_USD}`);
  summary.push("");
  summary.push(`Wallets with ANY nonzero inventory: ${wallets.size}`);
  summary.push("");
  summary.push(`Material wallets (>= threshold): ${material.length}`);
  summary.push(`Material total USD (known prices only): ${sumUsd(material).toFixed(2)}`);
  summary.push("");
  summary.push(`Immaterial wallets (< threshold): ${immaterial.length}`);
  summary.push(`Immaterial total USD (known prices only): ${sumUsd(immaterial).toFixed(2)}`);
  summary.push("");
  summary.push(`Wallets needing gas for planned moves: ${gasNeeds.size}`);
  const gasBreak = {};
  for (const g of gasNeeds.values()) {
    for (const a of g.gasAssets) gasBreak[a] = (gasBreak[a]||0) + 1;
  }
  summary.push(`Gas needs breakdown (wallet count): ${JSON.stringify(gasBreak)}`);
  summary.push("");
  summary.push(`Unknown-price assets encountered in inventory: ${unknownPriceAssets.size}`);
  summary.push(`Unknown-price assets list (sample up to 100): ${[...unknownPriceAssets].slice(0,100).join(", ")}`);

  fs.writeFileSync(path.join(OUT_DIR, "re_eval_summary.txt"), summary.join("\n"));

  // prices_used.csv
  const pricesCsv = ["assetId,coingeckoId,priceUsd,asOf"];
  for (const assetId of Object.keys(assetToCg).sort()) {
    const cgId = assetToCg[assetId] || "";
    const p = (typeof priceByAsset[assetId] === "number") ? priceByAsset[assetId] : "";
    pricesCsv.push([assetId, cgId, p, asOf].join(","));
  }
  fs.writeFileSync(path.join(OUT_DIR, "prices_used.csv"), pricesCsv.join("\n"));

  // wallets_material.csv + wallets_immaterial.csv
  function walletsToCsv(arr, outFile) {
    const lines = ["vaultId,vaultName,totalUsdKnownPrices,assetCount"];
    for (const w of arr) {
      lines.push([w.vaultId, (w.name||"").replaceAll(","," "), w.totalUsd.toFixed(6), w.assets.size].join(","));
    }
    fs.writeFileSync(path.join(OUT_DIR, outFile), lines.join("\n"));
  }
  walletsToCsv(material, "wallets_material.csv");
  walletsToCsv(immaterial, "wallets_immaterial.csv");

  // gas_needs_wallets_with_names.csv
  const gasLines = ["vaultId,vaultName,gasAssetsNeeded,dependentAssetsCount,dependentUsdKnownPrices"];
  const gasArr = [...gasNeeds.values()];
  gasArr.sort((a,b)=> b.depsUsd - a.depsUsd);
  for (const g of gasArr) {
    gasLines.push([
      g.vaultId,
      (g.name||"").replaceAll(","," "),
      [...g.gasAssets].sort().join("|"),
      g.deps.size,
      g.depsUsd.toFixed(6)
    ].join(","));
  }
  fs.writeFileSync(path.join(OUT_DIR, "gas_needs_wallets_with_names.csv"), gasLines.join("\n"));

  console.log("✅ Re-evaluation complete");
  console.log("- analysis/re_eval_summary.txt");
  console.log("- analysis/wallets_material.csv");
  console.log("- analysis/wallets_immaterial.csv");
  console.log("- analysis/gas_needs_wallets_with_names.csv");
  console.log("- analysis/prices_used.csv");
  console.log("");
  console.log("Unknown-price assets encountered:", unknownPriceAssets.size);
})();
