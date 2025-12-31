const fs = require("fs");

const INV = "inventory/inventory.csv";
const VAULTS = "inventory/vaults.json";
const PRICES = "execute/last_prices_usd.json";
const GAS_MAP = "analysis/gas_needs_wallets.csv";

const MATERIAL_WALLET_USD = Number(process.env.MATERIAL_WALLET_USD || "1.00");
const MATERIAL_ASSET_USD  = Number(process.env.MATERIAL_ASSET_USD  || "0.25");

if (![INV, VAULTS, PRICES].every(fs.existsSync)) {
  throw new Error(`Missing required input files. Need: ${INV}, ${VAULTS}, ${PRICES}`);
}

const vaultNames = JSON.parse(fs.readFileSync(VAULTS, "utf8"));
const prices = JSON.parse(fs.readFileSync(PRICES, "utf8"));

const gasNeeds = new Map(); // vaultId -> Set(gasAsset)
if (fs.existsSync(GAS_MAP)) {
  const rows = fs.readFileSync(GAS_MAP, "utf8").split(/\r?\n/);
  for (const r of rows) {
    if (!r.trim()) continue;
    const [v, gas] = r.split(",");
    if (!v || !gas) continue;
    if (!gasNeeds.has(v)) gasNeeds.set(v, new Set());
    gasNeeds.get(v).add(gas);
  }
}

const lines = fs.readFileSync(INV, "utf8").split(/\r?\n/).filter(Boolean);
if (lines.length < 2) throw new Error("inventory/inventory.csv has no data rows");

const hdr = lines[0].split(",");
const idx = Object.fromEntries(hdr.map((h, i) => [h, i]));

for (const col of ["vaultId","assetId","total"]) {
  if (idx[col] === undefined) throw new Error(`inventory.csv missing required column: ${col}`);
}

function num(x) {
  const n = Number(String(x ?? "").trim());
  return Number.isFinite(n) ? n : 0;
}

const wallets = new Map();

for (let i = 1; i < lines.length; i++) {
  const r = lines[i].split(",");
  const vaultId = r[idx.vaultId];
  const assetId = r[idx.assetId];
  const total = num(r[idx.total]);
  if (!vaultId || !assetId) continue;
  if (total <= 0) continue;

  const price = prices[assetId];
  const usd = (typeof price === "number" && Number.isFinite(price)) ? total * price : 0;

  if (!wallets.has(vaultId)) {
    wallets.set(vaultId, {
      vaultId,
      vaultName: (vaultNames[vaultId] && vaultNames[vaultId].name) ? vaultNames[vaultId].name : "",
      hiddenOnUI: Boolean(vaultNames[vaultId] && vaultNames[vaultId].hiddenOnUI === true),
      totalUsd: 0,
      assets: [],
      maxAssetUsd: 0,
      maxAsset: ""
    });
  }

  const w = wallets.get(vaultId);
  w.totalUsd += usd;
  w.assets.push({ assetId, usd });

  if (usd > w.maxAssetUsd) {
    w.maxAssetUsd = usd;
    w.maxAsset = assetId;
  }
}

const rows = [];
const gasRows = [];
const immaterial = [];

let materialUsd = 0;
let immaterialUsd = 0;

for (const w of wallets.values()) {
  const needsGasAssets = gasNeeds.get(w.vaultId);
  const needsGas = Boolean(needsGasAssets && needsGasAssets.size > 0);

  const hasMaterialAsset = w.assets.some(a => a.usd >= MATERIAL_ASSET_USD);

  const isMaterial =
    w.totalUsd >= MATERIAL_WALLET_USD ||
    (needsGas && w.totalUsd >= MATERIAL_ASSET_USD) ||
    hasMaterialAsset;

  if (isMaterial) materialUsd += w.totalUsd;
  else immaterialUsd += w.totalUsd;

  const row = {
    vaultId: w.vaultId,
    vaultName: w.vaultName,
    hiddenOnUI: w.hiddenOnUI,
    assetCount: w.assets.length,
    totalUsdValue: w.totalUsd.toFixed(2),
    materiality: isMaterial ? "MATERIAL" : "IMMATERIAL",
    needsGas,
    gasAssetsNeeded: needsGasAssets ? [...needsGasAssets].join("|") : "",
    largestAsset: w.maxAsset,
    largestAssetUsd: w.maxAssetUsd.toFixed(2)
  };

  rows.push(row);
  if (needsGas && isMaterial) gasRows.push(row);
  if (!isMaterial) immaterial.push(row);
}

fs.mkdirSync("analysis", { recursive: true });

function writeCsv(p, arr) {
  // Always write a CSV even if empty, with a stable header.
  const header = [
    "vaultId","vaultName","hiddenOnUI","assetCount","totalUsdValue",
    "materiality","needsGas","gasAssetsNeeded","largestAsset","largestAssetUsd"
  ];
  const out = [header.join(",")];

  for (const r of arr) {
    out.push(header.map(k => {
      const v = r[k];
      // basic CSV escaping
      const s = (v === null || v === undefined) ? "" : String(v);
      return /[",\n]/.test(s) ? `"${s.replace(/"/g,'""')}"` : s;
    }).join(","));
  }
  fs.writeFileSync(p, out.join("\n"));
}

writeCsv("analysis/wallet_materiality.csv", rows);
writeCsv("analysis/wallets_needing_gas.csv", gasRows);
writeCsv("analysis/wallets_immaterial.csv", immaterial);

fs.writeFileSync(
  "analysis/wallet_materiality_summary.txt",
  [
    "Wallet Materiality Summary",
    "===========================",
    `Total wallets analyzed: ${rows.length}`,
    "",
    `Material wallets: ${rows.filter(r => r.materiality==="MATERIAL").length}`,
    `Immaterial wallets: ${rows.filter(r => r.materiality==="IMMATERIAL").length}`,
    "",
    `Material USD total: $${materialUsd.toFixed(2)}`,
    `Immaterial USD total: $${immaterialUsd.toFixed(2)}`,
    "",
    `Wallets needing gas (material): ${gasRows.length}`,
    "",
    "Inputs:",
    `- ${INV}`,
    `- ${VAULTS}`,
    `- ${PRICES}`,
    `- ${fs.existsSync(GAS_MAP) ? GAS_MAP : "(no gas map found; needsGas will be false)"}`,
    "",
    "Thresholds:",
    `- MATERIAL_WALLET_USD=${MATERIAL_WALLET_USD}`,
    `- MATERIAL_ASSET_USD=${MATERIAL_ASSET_USD}`
  ].join("\n")
);

console.log("✅ Wallet materiality analysis complete");
console.log(`wallets=${rows.length} material=${rows.filter(r=>r.materiality==="MATERIAL").length} immaterial=${rows.filter(r=>r.materiality==="IMMATERIAL").length}`);
console.log(`needsGas(material)=${gasRows.length}`);
