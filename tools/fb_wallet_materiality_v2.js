const fs = require("fs");
const path = require("path");

const ROOT = process.cwd();
const INV = path.join(ROOT, "inventory", "inventory.csv");

// Price file priority:
// 1) execute/prices_usd.json (explicit)
// 2) execute/last_prices_usd.json (cached)
// If neither exists, script runs but marks all USD as unknown.
const PRICE_PATH =
  process.env.PRICES_JSON ||
  (fs.existsSync(path.join(ROOT, "execute", "prices_usd.json")) ? path.join(ROOT, "execute", "prices_usd.json") :
   fs.existsSync(path.join(ROOT, "execute", "last_prices_usd.json")) ? path.join(ROOT, "execute", "last_prices_usd.json") :
   null);

const OUT_DIR = path.join(ROOT, "analysis");
fs.mkdirSync(OUT_DIR, { recursive: true });

const MIN_USD_WALLET = Number(process.env.MIN_USD_WALLET || "1"); // wallet materiality threshold
if (!Number.isFinite(MIN_USD_WALLET) || MIN_USD_WALLET < 0) throw new Error("MIN_USD_WALLET must be >= 0");

// optional stable policy summary (for receiver narrative)
const STABLE_WALLET_FLOOR = Number(process.env.STABLE_WALLET_FLOOR || "0"); // set if you want

function parseCsvLine(line) {
  // inventory.csv is simple (no quoted commas). If you ever introduce quotes, swap to a robust CSV parser.
  return line.split(",");
}
function num(x) {
  const n = Number(String(x ?? "").trim());
  return Number.isFinite(n) ? n : 0;
}
function writeCsv(filePath, rows) {
  if (!rows || rows.length === 0) {
    fs.writeFileSync(filePath, ""); // empty but valid
    return;
  }
  const header = Object.keys(rows[0]);
  const out = [header.join(",")];
  for (const r of rows) out.push(header.map(k => String(r[k] ?? "")).join(","));
  fs.writeFileSync(filePath, out.join("\n"));
}

if (!fs.existsSync(INV)) throw new Error(`Missing ${INV}. Run your inventory refresh first.`);

const prices = PRICE_PATH ? JSON.parse(fs.readFileSync(PRICE_PATH, "utf8")) : {};
// prices format expected: { "USDC": 1, "ETH": 3000.12, ... }

const lines = fs.readFileSync(INV, "utf8").split(/\r?\n/).filter(Boolean);
const hdr = parseCsvLine(lines[0]);

const I = {
  vaultId: hdr.indexOf("vaultId"),
  vaultName: hdr.indexOf("vaultName"),
  assetId: hdr.indexOf("assetId"),
  total: hdr.indexOf("total"),
  available: hdr.indexOf("available"),
};

for (const k of Object.keys(I)) {
  if (I[k] < 0) throw new Error(`inventory.csv missing column: ${k}`);
}

// Aggregate wallet totals in USD
const wallets = new Map(); // vaultId -> {vaultId,vaultName,usdKnown,usdUnknownRowCount,assetRowCount,assetNonzeroCount}
const unknownAssets = new Set();

for (let i = 1; i < lines.length; i++) {
  const row = parseCsvLine(lines[i]);
  const vaultId = row[I.vaultId];
  const vaultName = row[I.vaultName];
  const assetId = row[I.assetId];

  const total = num(row[I.total]);
  const avail = num(row[I.available]);

  // consider nonzero balances only
  const bal = total > 0 ? total : avail;
  if (!(bal > 0)) continue;

  if (!wallets.has(vaultId)) {
    wallets.set(vaultId, {
      vaultId,
      vaultName,
      assetRowCount: 0,
      assetNonzeroCount: 0,
      usdKnown: 0,
      unknownUsdRows: 0,
    });
  }

  const w = wallets.get(vaultId);
  w.assetRowCount++;
  w.assetNonzeroCount++;

  const px = prices[assetId];
  if (px === undefined || px === null || !Number.isFinite(Number(px))) {
    w.unknownUsdRows++;
    unknownAssets.add(assetId);
  } else {
    w.usdKnown += bal * Number(px);
  }
}

// Build material/immaterial sets
const material = [];
const immaterial = [];
let materialUsd = 0, immaterialUsd = 0;
let materialWallets = 0, immaterialWallets = 0;

for (const w of wallets.values()) {
  const isMaterial = w.usdKnown >= MIN_USD_WALLET;
  if (isMaterial) {
    material.push(w);
    materialUsd += w.usdKnown;
    materialWallets++;
  } else {
    immaterial.push(w);
    immaterialUsd += w.usdKnown;
    immaterialWallets++;
  }
}

// Sort for readability
material.sort((a,b)=> b.usdKnown - a.usdKnown);
immaterial.sort((a,b)=> b.usdKnown - a.usdKnown);

writeCsv(path.join(OUT_DIR, "wallets_material.csv"), material);
writeCsv(path.join(OUT_DIR, "wallets_immaterial.csv"), immaterial);

const summary = {
  inputInventory: INV,
  pricesFile: PRICE_PATH || "NONE",
  minUsdWallet: MIN_USD_WALLET,
  walletCountWithNonzeroAssets: wallets.size,
  materialWalletCount: materialWallets,
  immaterialWalletCount: immaterialWallets,
  materialUsdKnownPrices: Number(materialUsd.toFixed(2)),
  immaterialUsdKnownPrices: Number(immaterialUsd.toFixed(2)),
  unknownAssets: Array.from(unknownAssets).sort(),
};

fs.writeFileSync(path.join(OUT_DIR, "wallet_materiality_summary.json"), JSON.stringify(summary, null, 2));
console.log("✅ Wallet materiality generated");
console.log(summary);
