/**
 * Fireblocks Consolidation/Liquidation Analysis
 *
 * Inputs (expected to exist):
 * - inventory/inventory.csv
 * - plan/plan.csv
 * - execute/completed_all.txt (and/or other completed ledgers)
 * Optional:
 * - execute/min_by_asset.json (for thresholds)
 * - execute/asset_to_coingecko.json (mapping)
 * - execute/last_prices_usd.json (cached prices to avoid web calls)
 *
 * Outputs:
 * - analysis/report_summary.txt
 * - analysis/remaining_rows.csv
 * - analysis/remaining_rows.jsonl
 * - analysis/by_asset.csv
 * - analysis/by_reason.csv
 */

const fs = require("fs");
const path = require("path");

const ROOT = process.cwd();
const OUTDIR = path.join(ROOT, "analysis");
fs.mkdirSync(OUTDIR, { recursive: true });

const INVENTORY_CSV = path.join(ROOT, "inventory", "inventory.csv");
const PLAN_CSV = path.join(ROOT, "plan", "plan.csv");

// Completed ledgers: we’ll load anything matching execute/completed*.txt
const EXEC_DIR = path.join(ROOT, "execute");

function mustExist(p) {
  if (!fs.existsSync(p)) throw new Error("Missing required file: " + p);
}

mustExist(INVENTORY_CSV);
mustExist(PLAN_CSV);
if (!fs.existsSync(EXEC_DIR)) throw new Error("Missing execute/ directory: " + EXEC_DIR);

const MIN_PATH = path.join(EXEC_DIR, "min_by_asset.json");
const MAP_PATH = path.join(EXEC_DIR, "asset_to_coingecko.json");
const LAST_PRICES_PATH = path.join(EXEC_DIR, "last_prices_usd.json");

const OFFLINE = process.env.OFFLINE === "1"; // set OFFLINE=1 to skip web price fetch
const MIN_USD_PER_TX = Number(process.env.MIN_USD_PER_TX || "0.01");       // non-stable default
const STABLECOIN_MIN_USD = Number(process.env.STABLECOIN_MIN_USD || "0.25");
const BATCH = Number(process.env.BATCH || "20");
const APPROVALS_PER_MIN = Number(process.env.APPROVALS_PER_MIN || "8");     // tune your realistic phone approval rate

function num(x) {
  const n = Number(String(x ?? "").trim());
  return Number.isFinite(n) ? n : 0;
}

function readCsvHeader(p) {
  const first = fs.readFileSync(p, "utf8").split(/\r?\n/)[0];
  return first.split(",").map(s => s.trim());
}

function detectCols(header) {
  const lower = header.map(h => h.toLowerCase());
  const pick = (cands) => {
    for (const c of cands) {
      const i = lower.indexOf(c.toLowerCase());
      if (i >= 0) return i;
    }
    return -1;
  };

  return {
    // inventory
    inv_vault: pick(["vaultaccountid","vault_account_id","vaultid","vault_id","accountid","account_id","id","sourcevaultid","source_vault_id"]),
    inv_asset: pick(["assetid","asset_id","asset","currency","token"]),
    inv_avail: pick(["available","avail","availablebalance","available_balance","spendable","spendablebalance"]),
    inv_total: pick(["total","balance","bal","totalbalance","total_balance"]),

    // plan
    p_source: pick(["sourcevaultid"]),
    p_asset: pick(["assetid"]),
    p_amount: pick(["amount"]),
    p_dest: pick(["destinationvaultid"]),
    p_requiresGas: pick(["requiresgas"]),
    p_gasAsset: pick(["gasassetid"]),
    p_gasReady: pick(["gasready"]),
  };
}

function loadCompletedLedgers() {
  const files = fs.readdirSync(EXEC_DIR).filter(f => /^completed.*\.txt$/i.test(f));
  const set = new Set();
  for (const f of files) {
    const p = path.join(EXEC_DIR, f);
    const lines = fs.readFileSync(p, "utf8").split(/\r?\n/).filter(Boolean);
    for (const line of lines) set.add(line.trim());
  }
  return { files, set };
}

function isStable(assetId) {
  // conservative stable detection (extend if needed)
  const a = assetId.toUpperCase();
  return (
    a.includes("USDC") ||
    a.includes("USDT") ||
    a === "TUSD" || a.includes("TUSD") ||
    a.includes("BUSD")
  );
}

function loadJsonMaybe(p) {
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

// --- Prices: stablecoins treated as $1, others via cached or CoinGecko (optional) ---
async function getPricesUSD(assetsInScope) {
  const prices = {};

  // stables fixed to 1
  for (const a of assetsInScope) if (isStable(a)) prices[a] = 1;

  // load cached last prices
  const cached = loadJsonMaybe(LAST_PRICES_PATH) || {};
  for (const [k, v] of Object.entries(cached)) {
    if (assetsInScope.has(k) && prices[k] == null && num(v) > 0) prices[k] = num(v);
  }

  if (OFFLINE) return prices;

  // if mapping exists, fetch from CoinGecko simple price
  const map = loadJsonMaybe(MAP_PATH) || {};
  const toFetch = [];
  for (const a of assetsInScope) {
    if (prices[a] != null) continue;
    const cg = map[a];
    if (cg) toFetch.push([a, cg]);
  }

  if (toFetch.length === 0) return prices;

  // batch coingecko ids
  const uniqIds = Array.from(new Set(toFetch.map(x => x[1])));
  // coingecko API limit is usually fine for 50-250 ids; we’ll chunk
  const chunks = [];
  const CHUNK = 150;
  for (let i = 0; i < uniqIds.length; i += CHUNK) chunks.push(uniqIds.slice(i, i + CHUNK));

  // minimal fetch helper
  async function fetchJson(url) {
    const res = await fetch(url, { headers: { "accept": "application/json" } });
    if (!res.ok) throw new Error(`CoinGecko HTTP ${res.status}`);
    return await res.json();
  }

  for (const ids of chunks) {
    const url = `https://api.coingecko.com/api/v3/simple/price?ids=${encodeURIComponent(ids.join(","))}&vs_currencies=usd`;
    let json;
    try {
      json = await fetchJson(url);
    } catch (e) {
      // If price fetch fails, we continue; analysis will show unknowns
      continue;
    }
    // map back to assets
    for (const [assetId, cgId] of toFetch) {
      if (prices[assetId] != null) continue;
      const p = json?.[cgId]?.usd;
      if (num(p) > 0) prices[assetId] = num(p);
    }
  }

  // persist updated prices
  const merged = { ...(loadJsonMaybe(LAST_PRICES_PATH) || {}), ...prices };
  fs.writeFileSync(LAST_PRICES_PATH, JSON.stringify(merged, null, 2));
  return prices;
}

function writeCsv(p, header, rows) {
  const esc = (v) => {
    const s = (v == null) ? "" : String(v);
    return /[",\n]/.test(s) ? `"${s.replace(/"/g,'""')}"` : s;
  };
  const out = [header.join(",")].concat(rows.map(r => r.map(esc).join(","))).join("\n") + "\n";
  fs.writeFileSync(p, out);
}

(async () => {
  const invHdr = readCsvHeader(INVENTORY_CSV);
  const planHdr = readCsvHeader(PLAN_CSV);
  const colsInv = detectCols(invHdr);
  const colsPlan = detectCols(planHdr);

  // validate inventory cols
  for (const k of ["inv_vault","inv_asset","inv_avail","inv_total"]) {
    if (colsInv[k] < 0) throw new Error("inventory.csv missing column for " + k + " header=" + invHdr.join("|"));
  }
  // validate plan cols
  for (const k of ["p_source","p_asset","p_amount","p_dest","p_requiresGas","p_gasAsset","p_gasReady"]) {
    if (colsPlan[k] < 0) throw new Error("plan.csv missing column for " + k + " header=" + planHdr.join("|"));
  }

  const { files: ledgerFiles, set: completedSet } = loadCompletedLedgers();

  // Load inventory: for each vault+asset, we know available/total
  const invLines = fs.readFileSync(INVENTORY_CSV, "utf8").split(/\r?\n/).filter(Boolean);
  const inv = new Map(); // key = vault|asset -> {avail,total}
  for (let i = 1; i < invLines.length; i++) {
    const row = invLines[i].split(",");
    const vault = row[colsInv.inv_vault];
    const asset = row[colsInv.inv_asset];
    const avail = num(row[colsInv.inv_avail]);
    const total = num(row[colsInv.inv_total]);
    if (!(avail > 0 || total > 0)) continue;
    inv.set(`${vault}|${asset}`, { avail, total });
  }

  // Load min_by_asset if present
  const minObj = loadJsonMaybe(MIN_PATH) || {};
  const minByAsset = minObj.minByAsset || {};

  // Read plan rows and determine what’s remaining
  const planLines = fs.readFileSync(PLAN_CSV, "utf8").split(/\r?\n/).filter(Boolean);
  const remaining = [];
  const remainingByReason = new Map();
  const assetsInScope = new Set();
  const walletsInScope = new Set();

  // Reasons
  // - COMPLETED
  // - NEEDS_GAS (requiresGas && !gasReady)
  // - BELOW_MIN (below min thresholds; we treat as not cost-effective)
  // - READY_TO_EXECUTE (eligible)
  // - UNKNOWN_PRICE (for later reporting)
  // - DEST_NOT_READY/UNSUPPORTED (not directly known from plan; we tag from known prior fails if you export them; leave for now)

  function addReason(reason, usdValue, walletId) {
    remainingByReason.set(reason, (remainingByReason.get(reason) || 0) + usdValue);
    if (walletId) walletsInScope.add(walletId);
  }

  // Collect candidates first to price them
  const candidates = [];
  for (let i = 1; i < planLines.length; i++) {
    const row = planLines[i].split(",");
    const sourceVaultId = row[colsPlan.p_source];
    const assetId = row[colsPlan.p_asset];
    const amount = num(row[colsPlan.p_amount]);
    const destinationVaultId = row[colsPlan.p_dest];
    const requiresGas = String(row[colsPlan.p_requiresGas]).toLowerCase() === "true";
    const gasAssetId = row[colsPlan.p_gasAsset] || "";
    const gasReady = String(row[colsPlan.p_gasReady]).toLowerCase() === "true";

    const rid = `${sourceVaultId}|${assetId}|${destinationVaultId}`;
    assetsInScope.add(assetId);

    candidates.push({
      rid, sourceVaultId, assetId, amount, destinationVaultId, requiresGas, gasAssetId, gasReady
    });
  }

  const prices = await getPricesUSD(assetsInScope);

  let completedCount = 0;
  let remainingCount = 0;

  // Evaluate each candidate
  for (const r of candidates) {
    const { rid, sourceVaultId, assetId, amount, destinationVaultId, requiresGas, gasAssetId, gasReady } = r;

    if (completedSet.has(rid)) {
      completedCount++;
      continue;
    }

    remainingCount++;

    const price = prices[assetId]; // may be undefined
    const usd = (price == null) ? 0 : amount * price;

    // Determine minimum thresholds
    let minAmt = minByAsset[assetId];
    if (minAmt == null) {
      // fallback:
      if (isStable(assetId)) {
        // stable min in USD (convert to token units ~1)
        minAmt = STABLECOIN_MIN_USD;
      } else {
        // non-stable: USD min floor -> token units depends on price; if unknown price, do not auto-skip
        if (price != null && price > 0) minAmt = MIN_USD_PER_TX / price;
      }
    }

    let reason = "READY_TO_EXECUTE";
    if (requiresGas && !gasReady) reason = "NEEDS_GAS";
    else if (minAmt != null && amount < minAmt) reason = "BELOW_MIN";

    // If price unknown, we still keep it but tag for reporting
    const priceKnown = price != null && price > 0;

    remaining.push({
      ...r,
      priceUSD: priceKnown ? price : "",
      estUSD: priceKnown ? usd : "",
      reason,
      priceKnown
    });

    addReason(reason, priceKnown ? usd : 0, sourceVaultId);
  }

  // Aggregate by asset for remaining
  const byAsset = new Map();
  for (const r of remaining) {
    const key = r.assetId;
    if (!byAsset.has(key)) byAsset.set(key, { rows: 0, wallets: new Set(), amt: 0, usd: 0, unknown: 0, needsGas: 0, ready: 0, belowMin: 0 });
    const o = byAsset.get(key);
    o.rows++;
    o.wallets.add(r.sourceVaultId);
    o.amt += r.amount;
    if (r.priceKnown) o.usd += (r.estUSD || 0);
    else o.unknown++;
    if (r.reason === "NEEDS_GAS") o.needsGas++;
    if (r.reason === "READY_TO_EXECUTE") o.ready++;
    if (r.reason === "BELOW_MIN") o.belowMin++;
  }

  // Time estimate
  // We assume: for READY rows, you can submit BATCH at a time, but phone approvals are bottleneck.
  const readyRows = remaining.filter(x => x.reason === "READY_TO_EXECUTE").length;
  const needsGasRows = remaining.filter(x => x.reason === "NEEDS_GAS").length;
  const belowMinRows = remaining.filter(x => x.reason === "BELOW_MIN").length;

  const approvalsPerHour = APPROVALS_PER_MIN * 60;
  const estHoursApproveReady = approvalsPerHour > 0 ? (readyRows / approvalsPerHour) : 0;

  // Write outputs
  writeCsv(path.join(OUTDIR, "remaining_rows.csv"),
    ["rowId","sourceVaultId","assetId","amount","destinationVaultId","requiresGas","gasAssetId","gasReady","priceUSD","estUSD","reason"],
    remaining.map(r => [
      r.rid, r.sourceVaultId, r.assetId, r.amount, r.destinationVaultId,
      r.requiresGas, r.gasAssetId, r.gasReady,
      r.priceUSD, r.estUSD, r.reason
    ])
  );

  fs.writeFileSync(path.join(OUTDIR, "remaining_rows.jsonl"),
    remaining.map(r => JSON.stringify(r)).join("\n") + "\n"
  );

  // by_asset.csv
  const byAssetRows = Array.from(byAsset.entries()).map(([asset, o]) => {
    return [
      asset,
      o.wallets.size,
      o.rows,
      o.amt,
      o.usd,
      o.unknown,
      o.ready,
      o.needsGas,
      o.belowMin
    ];
  }).sort((a,b)=> (b[4]-a[4]) || (b[2]-a[2]));

  writeCsv(path.join(OUTDIR, "by_asset.csv"),
    ["assetId","walletCount","rowCount","sumAmount","sumUSD_known","unknownPriceRows","readyRows","needsGasRows","belowMinRows"],
    byAssetRows
  );

  // by_reason.csv
  const reasons = ["READY_TO_EXECUTE","NEEDS_GAS","BELOW_MIN"];
  const byReasonRows = reasons.map(r => {
    const rows = remaining.filter(x => x.reason === r).length;
    const usd = remaining.filter(x => x.reason === r && x.priceKnown).reduce((s,x)=>s+(x.estUSD||0),0);
    const wallets = new Set(remaining.filter(x => x.reason === r).map(x=>x.sourceVaultId)).size;
    return [r, wallets, rows, usd];
  });

  writeCsv(path.join(OUTDIR, "by_reason.csv"),
    ["reason","walletCount","rowCount","sumUSD_known"],
    byReasonRows
  );

  // Summary text
  const totalRemainingUSD = remaining.filter(x=>x.priceKnown).reduce((s,x)=>s+(x.estUSD||0),0);
  const unknownPriceRows = remaining.filter(x=>!x.priceKnown).length;
  const unknownAssets = Array.from(new Set(remaining.filter(x=>!x.priceKnown).map(x=>x.assetId))).slice(0, 50);

  const summary = [];
  summary.push("Fireblocks Consolidation/Liquidation Analysis");
  summary.push("===========================================");
  summary.push("");
  summary.push(`Input files:`);
  summary.push(`- ${INVENTORY_CSV}`);
  summary.push(`- ${PLAN_CSV}`);
  summary.push(`- Completed ledgers loaded (${ledgerFiles.length}): ${ledgerFiles.join(", ") || "(none)"}`);
  summary.push("");
  summary.push(`Policy parameters:`);
  summary.push(`- MIN_USD_PER_TX (non-stable floor): $${MIN_USD_PER_TX}`);
  summary.push(`- STABLECOIN_MIN_USD: $${STABLECOIN_MIN_USD}`);
  summary.push(`- OFFLINE prices: ${OFFLINE ? "yes" : "no"} (cached at execute/last_prices_usd.json)`);
  summary.push("");
  summary.push(`What is left (not yet completed):`);
  summary.push(`- Remaining plan rows: ${remainingCount}`);
  summary.push(`- Unique source wallets represented (remaining): ${walletsInScope.size}`);
  summary.push(`- Estimated USD value (known prices only): $${totalRemainingUSD.toFixed(2)}`);
  summary.push(`- Rows with unknown prices: ${unknownPriceRows}`);
  if (unknownAssets.length) summary.push(`- Unknown-price assets (sample up to 50): ${unknownAssets.join(", ")}`);
  summary.push("");
  summary.push(`Breakdown (rows):`);
  summary.push(`- READY_TO_EXECUTE: ${readyRows}`);
  summary.push(`- NEEDS_GAS: ${needsGasRows}`);
  summary.push(`- BELOW_MIN (skipped as not cost-effective by policy): ${belowMinRows}`);
  summary.push("");
  summary.push(`Time estimate (very rough, approvals bottleneck):`);
  summary.push(`- Approval rate assumption: ${APPROVALS_PER_MIN} approvals/minute`);
  summary.push(`- Estimated hours to approve READY_TO_EXECUTE rows: ${estHoursApproveReady.toFixed(2)} hours`);
  summary.push(`- Note: NEEDS_GAS requires a funding phase first; time depends on donor availability and per-chain batching.`);
  summary.push("");
  summary.push(`Market appetite (high-level heuristic):`);
  summary.push(`- High: BTC, ETH, SOL, major stables (USDC/USDT)`);
  summary.push(`- Medium: Large-cap L1s/L2s and widely listed tokens (e.g., ADA, DOT, AVAX, MATIC, XRP)`);
  summary.push(`- Low/variable: microcaps, bridged/special wrapped assets, chain-specific receipts; may require specialized liquidity or may be illiquid.`);
  summary.push(`- Anything with unknown price or destination not enabled should be treated as high-friction and potentially low-recovery.`);
  summary.push("");
  summary.push(`Generated outputs in ./analysis/:`);
  summary.push(`- report_summary.txt`);
  summary.push(`- remaining_rows.csv`);
  summary.push(`- remaining_rows.jsonl`);
  summary.push(`- by_asset.csv`);
  summary.push(`- by_reason.csv`);
  summary.push("");

  fs.writeFileSync(path.join(OUTDIR, "report_summary.txt"), summary.join("\n") + "\n");

  console.log("✅ Analysis written to ./analysis/");
  console.log("- analysis/report_summary.txt");
  console.log("- analysis/by_reason.csv");
  console.log("- analysis/by_asset.csv");
  console.log("- analysis/remaining_rows.csv");
})().catch(e => {
  console.error("ERROR:", e && e.stack ? e.stack : String(e));
  process.exit(1);
});
