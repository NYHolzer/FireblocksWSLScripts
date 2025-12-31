const fs = require("fs");
const path = require("path");
const https = require("https");

const ROOT = process.cwd();
const EXEC_DIR = path.join(ROOT, "execute");
const ANALYSIS_DIR = path.join(ROOT, "analysis");
fs.mkdirSync(EXEC_DIR, { recursive: true });
fs.mkdirSync(ANALYSIS_DIR, { recursive: true });

const INVENTORY_CSV = path.join(ROOT, "inventory", "inventory.csv");
if (!fs.existsSync(INVENTORY_CSV)) throw new Error("Missing inventory/inventory.csv. Run fb_refresh_inventory.js first.");

const MAP_PATH = path.join(EXEC_DIR, "asset_to_coingecko.json");
const OVERRIDES_PATH = path.join(EXEC_DIR, "price_overrides_usd.json");
const BASIS_PATH = path.join(EXEC_DIR, "asset_price_basis.json");

const OUT_PRICES = path.join(EXEC_DIR, "last_prices_usd.json");
const OUT_ASSUMPTIONS = path.join(ANALYSIS_DIR, "price_assumptions.csv");
const OUT_UNMAPPED = path.join(ANALYSIS_DIR, "unmapped_assets.txt");

const MIN_USD_PER_TX = Number(process.env.MIN_USD_PER_TX || "0.01"); // informational only here
const STABLECOIN_MIN_USD = Number(process.env.STABLECOIN_MIN_USD || "0.25"); // informational only here

function readJson(p, fallback) {
  try { return JSON.parse(fs.readFileSync(p, "utf8")); } catch { return fallback; }
}

const assetToCg = readJson(MAP_PATH, {});
const overrides = readJson(OVERRIDES_PATH, {});
const basis = readJson(BASIS_PATH, {}); // e.g. { "USDC": {"method":"basis_symbol","priceUsd":1}, ... }

function httpGetJson(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { "User-Agent": "fireblocks-inventory-pricer/1.0" } }, (res) => {
      let data = "";
      res.on("data", (c) => (data += c));
      res.on("end", () => {
        if (res.statusCode < 200 || res.statusCode >= 300) {
          return reject(new Error(`HTTP ${res.statusCode}: ${data.slice(0, 500)}`));
        }
        try { resolve(JSON.parse(data)); }
        catch (e) { reject(new Error(`Bad JSON: ${e.message}. Body head: ${data.slice(0,200)}`)); }
      });
    }).on("error", reject);
  });
}

function uniqueAssetsFromInventory(csvPath) {
  const text = fs.readFileSync(csvPath, "utf8");
  const lines = text.split(/\r?\n/).filter(Boolean);
  const hdr = lines[0].split(",");
  const idx = {
    assetId: hdr.indexOf("assetId"),
  };
  if (idx.assetId < 0) throw new Error("inventory.csv missing assetId column");

  const set = new Set();
  for (let i = 1; i < lines.length; i++) {
    const row = lines[i].split(",");
    const a = row[idx.assetId];
    if (a) set.add(a);
  }
  return Array.from(set).sort();
}

(async () => {
  const assets = uniqueAssetsFromInventory(INVENTORY_CSV);
  const prices = {};        // assetId -> priceUsd
  const method = {};        // assetId -> method string
  const notes = {};         // assetId -> note

  // 1) Overrides (highest priority)
  for (const [k, v] of Object.entries(overrides)) {
    const n = Number(v);
    if (Number.isFinite(n) && n > 0) {
      prices[k] = n;
      method[k] = "override_usd";
    }
  }

  // 2) Basis (e.g., stables = 1)
  for (const a of assets) {
    if (prices[a] != null) continue;
    const b = basis[a];
    if (b && b.method === "basis_symbol" && Number.isFinite(Number(b.priceUsd))) {
      prices[a] = Number(b.priceUsd);
      method[a] = "basis_symbol";
    }
  }

  // 3) CoinGecko for anything remaining that is mapped
  const needCg = assets.filter(a => prices[a] == null && assetToCg[a]);
  const cgIds = Array.from(new Set(needCg.map(a => assetToCg[a]).filter(Boolean)));

  // CoinGecko /simple/price batching
  // docs: /simple/price?ids=...&vs_currencies=usd
  // https://docs.coingecko.com/reference/endpoint-overview
  const CHUNK = 200;
  for (let i = 0; i < cgIds.length; i += CHUNK) {
    const chunk = cgIds.slice(i, i + CHUNK);
    const url = `https://api.coingecko.com/api/v3/simple/price?ids=${encodeURIComponent(chunk.join(","))}&vs_currencies=usd`;
    const json = await httpGetJson(url);
    for (const [cgId, payload] of Object.entries(json || {})) {
      const p = payload && Number(payload.usd);
      if (!Number.isFinite(p) || p <= 0) continue;
      // assign back to all assetIds mapped to this cgId
      for (const a of needCg) {
        if (assetToCg[a] === cgId && prices[a] == null) {
          prices[a] = p;
          method[a] = "coingecko_simple_price";
        }
      }
    }
  }

  // 4) Remaining unmapped/unpriced
  const unmapped = assets.filter(a => prices[a] == null);
  fs.writeFileSync(OUT_UNMAPPED, unmapped.join("\n") + (unmapped.length ? "\n" : ""));
  for (const a of unmapped) {
    method[a] = "unknown";
    notes[a] = "Add mapping in execute/asset_to_coingecko.json or override in execute/price_overrides_usd.json";
  }

  // Write prices cache
  fs.writeFileSync(OUT_PRICES, JSON.stringify(prices, null, 2));

  // Write assumptions CSV (receiver-friendly)
  const rows = [];
  rows.push(["assetId","priceUsd","method","note"].join(","));
  for (const a of assets) {
    const p = prices[a];
    const m = method[a] || "unknown";
    const n = notes[a] || "";
    rows.push([a, (p==null?"":String(p)), m, n.replace(/"/g,'""')].map(v => {
      const s = String(v ?? "");
      return /[",\n]/.test(s) ? `"${s}"` : s;
    }).join(","));
  }
  fs.writeFileSync(OUT_ASSUMPTIONS, rows.join("\n") + "\n");

  console.log("✅ Prices cache written:", OUT_PRICES);
  console.log("✅ Price assumptions written:", OUT_ASSUMPTIONS);
  console.log("✅ Unmapped/unpriced assets:", unmapped.length, "->", OUT_UNMAPPED);
  console.log("Policy (info): MIN_USD_PER_TX =", MIN_USD_PER_TX, "STABLECOIN_MIN_USD =", STABLECOIN_MIN_USD);
})().catch(e => {
  console.error("ERROR:", e && e.stack ? e.stack : String(e));
  process.exit(1);
});
