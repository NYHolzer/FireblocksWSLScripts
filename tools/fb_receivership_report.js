const fs = require("fs");
const path = require("path");

const ROOT = process.cwd();

const CONSOLIDATION_VAULTS = new Set(["94828","94797"]); // per your instruction
const SKIP_VAULTS = new Set(["94828","94797"]); // do not count these as "to transfer"

// ---- helpers
function readText(p){ return fs.readFileSync(p,"utf8"); }
function exists(p){ return fs.existsSync(p); }
function num(x){
  const n = Number(String(x ?? "").trim());
  return Number.isFinite(n) ? n : 0;
}
function csvParseLine(line){
  // inventory/plan files are simple enough here; if names contain commas they are quoted.
  // We'll do a minimal CSV parser that honors quotes.
  const out = [];
  let cur = "", inQ = false;
  for (let i=0;i<line.length;i++){
    const ch=line[i];
    if(ch === '"' ){
      if(inQ && line[i+1] === '"'){ cur+='"'; i++; }
      else inQ = !inQ;
    } else if(ch === ',' && !inQ){
      out.push(cur); cur="";
    } else cur+=ch;
  }
  out.push(cur);
  return out;
}
function loadCompletedLedgers(){
  const dir = path.join(ROOT,"execute");
  if(!exists(dir)) return new Set();
  const files = fs.readdirSync(dir).filter(f => f.startsWith("completed_") && f.endsWith(".txt"));
  const done = new Set();
  for(const f of files){
    const p = path.join(dir,f);
    const lines = readText(p).split(/\r?\n/).map(s=>s.trim()).filter(Boolean);
    for(const line of lines) done.add(line);
  }
  return done;
}

function loadPrices(){
  const p = path.join(ROOT,"execute","last_prices_usd.json");
  if(!exists(p)) throw new Error("Missing execute/last_prices_usd.json. Generate prices snapshot first.");
  const j = JSON.parse(readText(p));
  const prices = j.pricesUsdByAssetId || {};
  const missing = new Set(j.missingAssetIds || []);
  return { asOfIso: j.asOfIso || null, prices, missingList: Array.from(missing) };
}

function loadInventory(){
  const p = path.join(ROOT,"inventory","inventory.csv");
  if(!exists(p)) throw new Error("Missing inventory/inventory.csv. Run refresh inventory first.");
  const lines = readText(p).split(/\r?\n/).filter(Boolean);
  const hdr = csvParseLine(lines[0]);
  const idx = {
    vaultId: hdr.indexOf("vaultId"),
    vaultName: hdr.indexOf("vaultName"),
    assetId: hdr.indexOf("assetId"),
    total: hdr.indexOf("total"),
    available: hdr.indexOf("available")
  };
  for(const k of Object.keys(idx)) if(idx[k] < 0) throw new Error("inventory.csv missing column: "+k);

  const rows = [];
  for(let i=1;i<lines.length;i++){
    const c = csvParseLine(lines[i]);
    rows.push({
      vaultId: c[idx.vaultId],
      vaultName: c[idx.vaultName],
      assetId: c[idx.assetId],
      total: num(c[idx.total]),
      available: num(c[idx.available])
    });
  }
  return rows;
}

function loadPlanCsv(){
  const p = path.join(ROOT,"plan","plan.csv");
  if(!exists(p)) throw new Error("Missing plan/plan.csv. Rebuild plan first.");
  const lines = readText(p).split(/\r?\n/).filter(Boolean);
  const hdr = csvParseLine(lines[0]);
  const idx = {
    sourceVaultId: hdr.indexOf("sourceVaultId"),
    assetId: hdr.indexOf("assetId"),
    amount: hdr.indexOf("amount"),
    destinationVaultId: hdr.indexOf("destinationVaultId"),
    requiresGas: hdr.indexOf("requiresGas"),
    gasAssetId: hdr.indexOf("gasAssetId"),
    gasReady: hdr.indexOf("gasReady"),
  };
  for(const k of Object.keys(idx)) if(idx[k] < 0) throw new Error("plan.csv missing column: "+k);

  const rows = [];
  for(let i=1;i<lines.length;i++){
    const c = csvParseLine(lines[i]);
    rows.push({
      sourceVaultId: c[idx.sourceVaultId],
      assetId: c[idx.assetId],
      amount: num(c[idx.amount]),
      destinationVaultId: c[idx.destinationVaultId],
      requiresGas: c[idx.requiresGas] === "true",
      gasAssetId: c[idx.gasAssetId] || "",
      gasReady: c[idx.gasReady] === "true"
    });
  }
  return rows;
}

function loadGasFeePolicy(){
  // Optional; if missing, we still classify "not worth" using gas_fee_native.json if available
  const feePath = path.join(ROOT,"execute","gas_fee_native.json");
  const fee = exists(feePath) ? JSON.parse(readText(feePath)) : {};
  const policyPath = path.join(ROOT,"execute","gas_policy.json");
  const policy = exists(policyPath) ? JSON.parse(readText(policyPath)) : {};
  // Defaults (you can adjust in env or in gas_policy.json)
  const MIN_USD_PER_TX = num(process.env.MIN_USD_PER_TX ?? policy.MIN_USD_PER_TX ?? 0.01);
  const STABLECOIN_MIN_USD = num(process.env.STABLECOIN_MIN_USD ?? policy.STABLECOIN_MIN_USD ?? 0.25);
  return { feeNative: fee, MIN_USD_PER_TX, STABLECOIN_MIN_USD };
}

function isStable(assetId){
  // Conservative stable check; extend as needed
  return /^USDC/.test(assetId) || /^USDT/.test(assetId) || assetId === "TUSD" || /^BUSD/.test(assetId);
}

function walletBucket(usd){
  if(usd >= 100) return ">=100";
  if(usd >= 50) return "50-99.99";
  if(usd >= 10) return "10-49.99";
  if(usd >= 1) return "1-9.99";
  return "<1";
}

function writeCsv(filePath, arr){
  if(arr.length === 0){
    fs.writeFileSync(filePath, ""); // empty file
    return;
  }
  const h = Object.keys(arr[0]);
  const esc = (v)=>{
    const s = String(v ?? "");
    return /[",\n]/.test(s) ? `"${s.replace(/"/g,'""')}"` : s;
  };
  const lines = [h.join(",")];
  for(const r of arr) lines.push(h.map(k=>esc(r[k])).join(","));
  fs.writeFileSync(filePath, lines.join("\n"));
}

function main(){
  const completed = loadCompletedLedgers();
  const { asOfIso, prices, missingList } = loadPrices();
  const inventory = loadInventory();
  const plan = loadPlanCsv();
  const gas = loadGasFeePolicy();

  // Map vault -> name
  const vaultName = new Map();
  for(const r of inventory){
    if(!vaultName.has(r.vaultId)) vaultName.set(r.vaultId, r.vaultName || "");
  }

  // Wallet USD totals from inventory (all assets)
  const walletUsdKnown = new Map();
  const walletUsdUnknownAssetRows = new Map(); // count of rows with missing price for that wallet
  const walletHasNonzero = new Map();

  for(const r of inventory){
    const bal = r.total;
    if(!(bal > 0)) continue;
    walletHasNonzero.set(r.vaultId, true);

    const p = prices[r.assetId];
    if(typeof p === "number" && Number.isFinite(p)){
      walletUsdKnown.set(r.vaultId, (walletUsdKnown.get(r.vaultId) || 0) + bal * p);
    } else {
      walletUsdUnknownAssetRows.set(r.vaultId, (walletUsdUnknownAssetRows.get(r.vaultId) || 0) + 1);
    }
  }

  // Consolidated total: sum of vault 94828 + 94797
  let consolidatedUsdKnown = 0;
  let consolidatedUnknownRows = 0;
  for(const vid of CONSOLIDATION_VAULTS){
    consolidatedUsdKnown += walletUsdKnown.get(vid) || 0;
    consolidatedUnknownRows += walletUsdUnknownAssetRows.get(vid) || 0;
  }

  // Remaining plan rows = plan rows not in completed ledger
  // rowId format used in your journals: source|asset|dest
  function rowIdOf(p){ return `${p.sourceVaultId}|${p.assetId}|${p.destinationVaultId}`; }

  const remaining = [];
  for(const r of plan){
    const rid = rowIdOf(r);
    if(completed.has(rid)) continue;
    remaining.push({ ...r, rowId: rid });
  }

  // Classify each remaining row
  // - READY_NOW if (requiresGas==false) OR (requiresGas==true AND gasReady==true)
  // - NEEDS_GAS if requiresGas==true AND gasReady==false
  // - NOT_WORTH if below min policy OR gas cost >= value (when needs gas)
  const perRow = [];
  const notWorthRows = new Set();

  function priceOf(assetId){
    const p = prices[assetId];
    return (typeof p === "number" && Number.isFinite(p)) ? p : null;
  }

  for(const r of remaining){
    // Ignore skip vaults as sources (you said skip 94828 and skip 94797 as destination/consolidation)
    if(SKIP_VAULTS.has(String(r.sourceVaultId))) continue;

    const assetPx = priceOf(r.assetId);
    const valueUsd = assetPx === null ? null : (r.amount * assetPx);

    // Minimum policy (stable vs non-stable)
    const minUsd = isStable(r.assetId) ? gas.STABLECOIN_MIN_USD : gas.MIN_USD_PER_TX;
    const belowMin = (valueUsd !== null && valueUsd < minUsd);

    let estGasUsd = null;
    if(r.requiresGas){
      // estimate gas in USD using execute/gas_fee_native.json and gasAsset price
      const gasNative = gas.feeNative[r.gasAssetId];
      const gasPx = priceOf(r.gasAssetId);
      if(typeof gasNative === "number" && Number.isFinite(gasNative) && gasPx !== null){
        estGasUsd = gasNative * gasPx;
      }
    }

    const gasNotWorth = (valueUsd !== null && estGasUsd !== null && estGasUsd >= valueUsd && r.requiresGas);

    let status = "READY_NOW";
    if(r.requiresGas && !r.gasReady) status = "NEEDS_GAS";

    if(belowMin || gasNotWorth){
      status = "NOT_WORTH";
      notWorthRows.add(r.rowId);
    }

    perRow.push({
      rowId: r.rowId,
      sourceVaultId: r.sourceVaultId,
      sourceVaultName: vaultName.get(r.sourceVaultId) || "",
      assetId: r.assetId,
      amount: r.amount,
      destinationVaultId: r.destinationVaultId,
      requiresGas: r.requiresGas,
      gasAssetId: r.gasAssetId,
      gasReady: r.gasReady,
      valueUsd: valueUsd === null ? "" : valueUsd.toFixed(6),
      minUsdPolicy: minUsd,
      belowMin: belowMin,
      estGasUsd: estGasUsd === null ? "" : estGasUsd.toFixed(6),
      gasNotWorth: gasNotWorth,
      status
    });
  }

  // Wallet sets by category (based on remaining rows)
  const walletsNeedsGas = new Set();
  const walletsReadyNow = new Set();
  const walletsNotWorth = new Set();

  for(const r of perRow){
    if(!r.sourceVaultId) continue;
    if(r.status === "NEEDS_GAS") walletsNeedsGas.add(String(r.sourceVaultId));
    else if(r.status === "READY_NOW") walletsReadyNow.add(String(r.sourceVaultId));
    else if(r.status === "NOT_WORTH") walletsNotWorth.add(String(r.sourceVaultId));
  }

  // Wallet USD totals for those sets (known prices only; unknown rows tracked separately)
  function sumWallets(set){
    let sum = 0;
    let unknownWallets = 0;
    for(const vid of set){
      sum += walletUsdKnown.get(vid) || 0;
      if((walletUsdUnknownAssetRows.get(vid) || 0) > 0) unknownWallets++;
    }
    return { sum, unknownWallets };
  }

  const needsGasTotals = sumWallets(walletsNeedsGas);
  const readyNowTotals = sumWallets(walletsReadyNow);
  const notWorthTotals = sumWallets(walletsNotWorth);

  // Wallet bucket breakdown for "wallets that need to be transferred"
  // => wallets participating in either NEEDS_GAS or READY_NOW (exclude NOT_WORTH-only wallets)
  const walletsToTransfer = new Set([...walletsNeedsGas, ...walletsReadyNow]);
  const buckets = { ">=100":0, "50-99.99":0, "10-49.99":0, "1-9.99":0, "<1":0 };
  const bucketUsd = { ">=100":0, "50-99.99":0, "10-49.99":0, "1-9.99":0, "<1":0 };

  for(const vid of walletsToTransfer){
    const usd = walletUsdKnown.get(vid) || 0;
    const b = walletBucket(usd);
    buckets[b] += 1;
    bucketUsd[b] += usd;
  }

  // Unknown-priced assets (from the price snapshot)
  const unknownAssets = missingList.slice().sort();

  // Output directory
  const outDir = path.join(ROOT,"analysis");
  fs.mkdirSync(outDir,{recursive:true});

  // Write detail CSVs
  writeCsv(path.join(outDir,"receivership_remaining_rows.csv"), perRow);
  writeCsv(path.join(outDir,"receivership_wallets_needs_gas.csv"),
    Array.from(walletsNeedsGas).sort((a,b)=>Number(a)-Number(b)).map(vid=>({
      vaultId: vid,
      vaultName: vaultName.get(vid) || "",
      walletUsdKnown: (walletUsdKnown.get(vid) || 0).toFixed(6),
      unknownPriceRowCount: walletUsdUnknownAssetRows.get(vid) || 0
    }))
  );
  writeCsv(path.join(outDir,"receivership_wallets_ready_now.csv"),
    Array.from(walletsReadyNow).sort((a,b)=>Number(a)-Number(b)).map(vid=>({
      vaultId: vid,
      vaultName: vaultName.get(vid) || "",
      walletUsdKnown: (walletUsdKnown.get(vid) || 0).toFixed(6),
      unknownPriceRowCount: walletUsdUnknownAssetRows.get(vid) || 0
    }))
  );
  writeCsv(path.join(outDir,"receivership_wallets_not_worth.csv"),
    Array.from(walletsNotWorth).sort((a,b)=>Number(a)-Number(b)).map(vid=>({
      vaultId: vid,
      vaultName: vaultName.get(vid) || "",
      walletUsdKnown: (walletUsdKnown.get(vid) || 0).toFixed(6),
      unknownPriceRowCount: walletUsdUnknownAssetRows.get(vid) || 0
    }))
  );

  // Summary JSON + TXT for easy emailing
  const summary = {
    asOfIso,
    consolidationVaults: Array.from(CONSOLIDATION_VAULTS),
    consolidatedUsdKnownPrices: consolidatedUsdKnown,
    consolidatedUnknownPriceRows: consolidatedUnknownRows,

    remainingPlanRowsCount: perRow.length,

    notConsolidated: {
      walletsNeedingGas: {
        walletCount: walletsNeedsGas.size,
        totalUsdKnownPrices: needsGasTotals.sum,
        walletsWithUnknownPricedAssets: needsGasTotals.unknownWallets
      },
      walletsReadyNow: {
        walletCount: walletsReadyNow.size,
        totalUsdKnownPrices: readyNowTotals.sum,
        walletsWithUnknownPricedAssets: readyNowTotals.unknownWallets
      },
      walletsNotWorthTransferring: {
        walletCount: walletsNotWorth.size,
        totalUsdKnownPrices: notWorthTotals.sum,
        walletsWithUnknownPricedAssets: notWorthTotals.unknownWallets
      }
    },

    walletsToTransferBuckets: {
      counts: buckets,
      usdTotalsKnownPrices: bucketUsd
    },

    policy: {
      MIN_USD_PER_TX: gas.MIN_USD_PER_TX,
      STABLECOIN_MIN_USD: gas.STABLECOIN_MIN_USD,
      gasFeeNativeFileUsed: exists(path.join(ROOT,"execute","gas_fee_native.json")),
      gasPolicyFileUsed: exists(path.join(ROOT,"execute","gas_policy.json"))
    },

    unknownPricedAssetsInSnapshot: unknownAssets
  };

  fs.writeFileSync(path.join(outDir,"receivership_summary.json"), JSON.stringify(summary,null,2));

  const lines = [];
  lines.push("Fireblocks Receivership Snapshot");
  lines.push("==============================");
  lines.push("");
  lines.push(`As-of (prices snapshot): ${asOfIso || "unknown"}`);
  lines.push("");
  lines.push("Consolidated holdings (vaults 94828 + 94797):");
  lines.push(`- USD (known prices): ${consolidatedUsdKnown.toFixed(2)}`);
  lines.push(`- Unknown-priced inventory rows in those vaults: ${consolidatedUnknownRows}`);
  lines.push("");
  lines.push("Not yet consolidated (based on remaining plan rows, excluding sources 94828/94797):");
  lines.push(`- Requires gas (not gas-ready): wallets=${walletsNeedsGas.size}, USD known=${needsGasTotals.sum.toFixed(2)}, wallets with unknown-priced assets=${needsGasTotals.unknownWallets}`);
  lines.push(`- Ready now (no gas needed OR gas-ready): wallets=${walletsReadyNow.size}, USD known=${readyNowTotals.sum.toFixed(2)}, wallets with unknown-priced assets=${readyNowTotals.unknownWallets}`);
  lines.push(`- Not worth transferring (gas >= value or below min policy): wallets=${walletsNotWorth.size}, USD known=${notWorthTotals.sum.toFixed(2)}, wallets with unknown-priced assets=${notWorthTotals.unknownWallets}`);
  lines.push("");
  lines.push("Wallet value ranges (wallets that still require action: needs-gas + ready-now):");
  for(const k of [">=100","50-99.99","10-49.99","1-9.99","<1"]){
    lines.push(`- ${k}: wallets=${buckets[k]}, USD known=${bucketUsd[k].toFixed(2)}`);
  }
  lines.push("");
  lines.push("Unknown-priced assets in current price snapshot (must be mapped/overridden for full valuation):");
  if(unknownAssets.length === 0) lines.push("- (none)");
  else lines.push("- " + unknownAssets.join(", "));
  lines.push("");
  lines.push("Outputs:");
  lines.push("- analysis/receivership_summary.json");
  lines.push("- analysis/receivership_remaining_rows.csv");
  lines.push("- analysis/receivership_wallets_needs_gas.csv");
  lines.push("- analysis/receivership_wallets_ready_now.csv");
  lines.push("- analysis/receivership_wallets_not_worth.csv");

  fs.writeFileSync(path.join(outDir,"receivership_summary.txt"), lines.join("\n"));

  console.log("✅ Receivership report generated:");
  console.log(" - analysis/receivership_summary.txt");
  console.log(" - analysis/receivership_summary.json");
  console.log(" - analysis/receivership_remaining_rows.csv");
  console.log(" - analysis/receivership_wallets_needs_gas.csv");
  console.log(" - analysis/receivership_wallets_ready_now.csv");
  console.log(" - analysis/receivership_wallets_not_worth.csv");
}

main();
