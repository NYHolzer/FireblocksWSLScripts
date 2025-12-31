const fs = require("fs");

const INV = "inventory/inventory.csv";
const VAULTS = "inventory/vaults.json";
const PRICES = "execute/last_prices_usd.json";
const POLICY = "execute/gas_policy.json";

if (![INV, VAULTS, PRICES, POLICY].every(fs.existsSync)) {
  throw new Error("Missing required files. Need inventory/inventory.csv, inventory/vaults.json, execute/last_prices_usd.json, execute/gas_policy.json");
}

const vaultMap = JSON.parse(fs.readFileSync(VAULTS, "utf8"));
const prices = JSON.parse(fs.readFileSync(PRICES, "utf8"));
const policy = JSON.parse(fs.readFileSync(POLICY, "utf8"));

const OUTDIR = "move_plan";
fs.mkdirSync(OUTDIR, { recursive: true });

const skipVaults = new Set((policy.skipVaultIds || []).map(String));
const destVaultId = String(policy.destinationVaultId || "");
if (!destVaultId) throw new Error("destinationVaultId missing in execute/gas_policy.json");

const tokenGasMap = policy.tokenGasMap || {};
const minGasBalance = policy.minGasBalance || {};
const feeUsdByGas = policy.estimatedFeeUsdByGasAsset || {};
const minTxAmountByAsset = policy.minTxAmountByAsset || {};
const retainMinByAsset = policy.retainMinByAsset || {};

const MATERIAL_WALLET_USD = Number(policy.materialWalletUsd ?? 1.0);
const MIN_USD_NON_STABLE = Number(policy.minUsdPerTxNonStable ?? 0.25);
const MIN_USD_STABLE = Number(policy.minUsdPerTxStable ?? 0.25);
const SINGLE_ASSET_MOVE_ANYWAY = policy.singleAssetWalletMoveAnyway === true;

const stablePrefixes = policy.stablecoinAssetPrefixes || ["USDC","USDT","BUSD","TUSD"];

function isStable(assetId){
  return stablePrefixes.some(p => assetId === p || assetId.startsWith(p + "_") || assetId.startsWith(p));
}
function num(x){
  const n = Number(String(x ?? "").trim());
  return Number.isFinite(n) ? n : 0;
}
function fmt(n){
  // preserve precision for small values; Fireblocks accepts numeric strings
  if (!Number.isFinite(n)) return "0";
  return (Math.abs(n) < 1e-6) ? n.toFixed(18).replace(/0+$/,"").replace(/\.$/,"") : String(n);
}

const lines = fs.readFileSync(INV, "utf8").split(/\r?\n/).filter(Boolean);
const hdr = lines[0].split(",");
const idx = Object.fromEntries(hdr.map((h,i)=>[h,i]));
for (const c of ["vaultId","vaultName","assetId","available","total"]) {
  if (idx[c] === undefined) throw new Error("inventory.csv missing column: " + c);
}

const wallets = new Map(); // vaultId -> {vaultId,name,hidden,assets,totalUsd}
for (let i=1;i<lines.length;i++){
  const r = lines[i].split(",");
  const vaultId = String(r[idx.vaultId] || "");
  if (!vaultId) continue;
  if (skipVaults.has(vaultId)) continue;

  const assetId = String(r[idx.assetId] || "");
  if (!assetId) continue;

  const total = num(r[idx.total]);
  const avail = num(r[idx.available]);
  if (!(total > 0 || avail > 0)) continue;

  const px = prices[assetId];
  const usd = (typeof px === "number" && Number.isFinite(px)) ? total * px : 0;

  if (!wallets.has(vaultId)) {
    wallets.set(vaultId, {
      vaultId,
      vaultName: String(r[idx.vaultName] || (vaultMap[vaultId]?.name ?? "")),
      hiddenOnUI: Boolean(vaultMap[vaultId]?.hiddenOnUI === true),
      assets: [],
      totalUsd: 0
    });
  }
  const w = wallets.get(vaultId);
  w.assets.push({assetId, total, avail, usd});
  w.totalUsd += usd;
}

function gasReadyForWallet(w, gasAsset){
  if (!gasAsset) return true;
  const min = Number(minGasBalance[gasAsset] ?? 0);
  const gasRow = w.assets.find(a => a.assetId === gasAsset);
  const available = gasRow ? gasRow.avail : 0;
  return available >= min;
}

function getGasAsset(assetId){
  // direct mapping first
  if (tokenGasMap[assetId]) return tokenGasMap[assetId];

  // stable prefix fallback
  for (const p of stablePrefixes){
    if (assetId === p || assetId.startsWith(p+"_") || assetId.startsWith(p)){
      return tokenGasMap[p] || "";
    }
  }
  return "";
}

const materialWallets = [];
const moveRows = [];
const needsGasRows = [];
const skippedFeeGtValue = [];
const skippedMinTx = [];
const skippedRetainAll = [];

for (const w of wallets.values()){
  if (w.vaultId === destVaultId) continue;

  if (w.totalUsd < MATERIAL_WALLET_USD) continue;

  materialWallets.push({
    vaultId: w.vaultId,
    vaultName: w.vaultName,
    hiddenOnUI: w.hiddenOnUI,
    totalUsdValue: w.totalUsd
  });

  const singleAsset = w.assets.length === 1;

  for (const a of w.assets){
    const stable = isStable(a.assetId);
    const minUsd = stable ? MIN_USD_STABLE : MIN_USD_NON_STABLE;

    const px = prices[a.assetId];
    const priceKnown = (typeof px === "number" && Number.isFinite(px));

    // retain logic
    const retain = Number(retainMinByAsset[a.assetId] ?? 0);
    const sendable = Math.max(0, a.avail - retain);

    if (sendable <= 0){
      skippedRetainAll.push({
        vaultId:w.vaultId, vaultName:w.vaultName, assetId:a.assetId,
        available:a.avail, retain, reason:"RETAIN_ALL"
      });
      continue;
    }

    // chain min tx amount logic
    const minTx = Number(minTxAmountByAsset[a.assetId] ?? 0);
    if (minTx > 0 && sendable < minTx && !(SINGLE_ASSET_MOVE_ANYWAY && singleAsset)){
      skippedMinTx.push({
        vaultId:w.vaultId, vaultName:w.vaultName, assetId:a.assetId,
        sendable, minTx, reason:"BELOW_CHAIN_MIN_TX"
      });
      continue;
    }

    // USD value for the *sendable* portion (not total)
    const usdVal = priceKnown ? (sendable * px) : 0;

    // gas requirement
    const gasAsset = getGasAsset(a.assetId);
    const requiresGas = Boolean(gasAsset);
    const readyGas = requiresGas ? gasReadyForWallet(w, gasAsset) : true;
    const feeUsd = requiresGas ? Number(feeUsdByGas[gasAsset] ?? 0) : 0;

    // eligibility rules
    let eligible = true;
    let reason = "";

    if (!priceKnown) { eligible = false; reason = "UNKNOWN_PRICE"; }
    else if (usdVal < minUsd && !(SINGLE_ASSET_MOVE_ANYWAY && singleAsset)) { eligible = false; reason = "BELOW_MIN_USD"; }
    else if (requiresGas && feeUsd > usdVal && !(SINGLE_ASSET_MOVE_ANYWAY && singleAsset)) { eligible = false; reason = "FEE_GT_VALUE"; }
    else if (requiresGas && !readyGas) { eligible = false; reason = "NEEDS_GAS"; }

    const row = {
      vaultId: w.vaultId,
      vaultName: w.vaultName,
      hiddenOnUI: w.hiddenOnUI,
      assetId: a.assetId,
      amount: fmt(sendable),
      usdValue: usdVal,
      destinationVaultId: destVaultId,
      requiresGas,
      gasAssetId: requiresGas ? gasAsset : "",
      gasReady: readyGas,
      estimatedFeeUsd: feeUsd,
      retainMinApplied: retain,
      minTxAmountApplied: minTx,
      singleAssetWallet: singleAsset,
      eligible,
      ineligibleReason: reason
    };

    if (eligible) moveRows.push(row);
    else if (reason === "NEEDS_GAS") needsGasRows.push(row);
    else if (reason === "FEE_GT_VALUE") skippedFeeGtValue.push(row);
  }
}

moveRows.sort((a,b)=> (b.usdValue - a.usdValue) || (String(a.vaultId).localeCompare(String(b.vaultId))) );

function writeCsv(file, rows, header){
  const out = [header.join(",")];
  for (const r of rows){
    out.push(header.map(h=>{
      const v = r[h];
      const s = (v===null||v===undefined) ? "" : String(v);
      return /[",\n]/.test(s) ? `"${s.replace(/"/g,'""')}"` : s;
    }).join(","));
  }
  fs.writeFileSync(file, out.join("\n"));
}

writeCsv(`${OUTDIR}/material_wallets.csv`, materialWallets, ["vaultId","vaultName","hiddenOnUI","totalUsdValue"]);

writeCsv(`${OUTDIR}/move_plan.csv`, moveRows, [
  "vaultId","vaultName","hiddenOnUI","assetId","amount","usdValue",
  "destinationVaultId","requiresGas","gasAssetId","gasReady","estimatedFeeUsd",
  "retainMinApplied","minTxAmountApplied","singleAssetWallet"
]);

const walletGasNeed = new Map();
for (const r of needsGasRows){
  if (!walletGasNeed.has(r.vaultId)){
    walletGasNeed.set(r.vaultId, {vaultId:r.vaultId, vaultName:r.vaultName, hiddenOnUI:r.hiddenOnUI, gas:new Set()});
  }
  if (r.gasAssetId) walletGasNeed.get(r.vaultId).gas.add(r.gasAssetId);
}
const needsGasWallets = [...walletGasNeed.values()].map(x=>({
  vaultId:x.vaultId,
  vaultName:x.vaultName,
  hiddenOnUI:x.hiddenOnUI,
  gasAssetsNeeded:[...x.gas].join("|")
}));
needsGasWallets.sort((a,b)=> a.gasAssetsNeeded.localeCompare(b.gasAssetsNeeded) || (Number(a.vaultId)-Number(b.vaultId)));

writeCsv(`${OUTDIR}/needs_gas_wallets.csv`, needsGasWallets, ["vaultId","vaultName","hiddenOnUI","gasAssetsNeeded"]);

writeCsv(`${OUTDIR}/skip_fee_gt_value.csv`, skippedFeeGtValue.sort((a,b)=>b.usdValue-a.usdValue), [
  "vaultId","vaultName","assetId","amount","usdValue","gasAssetId","estimatedFeeUsd","retainMinApplied","minTxAmountApplied","singleAssetWallet"
]);

writeCsv(`${OUTDIR}/skip_below_chain_min_tx.csv`, skippedMinTx, [
  "vaultId","vaultName","assetId","sendable","minTx","reason"
]);

writeCsv(`${OUTDIR}/skip_retain_all.csv`, skippedRetainAll, [
  "vaultId","vaultName","assetId","available","retain","reason"
]);

const totalMoveUsd = moveRows.reduce((s,r)=>s+Number(r.usdValue||0),0);
const totalMaterialUsd = materialWallets.reduce((s,r)=>s+Number(r.totalUsdValue||0),0);

fs.writeFileSync(`${OUTDIR}/summary.txt`,
`Move Plan Summary (with chain minimums + retain minimums)
=========================================================
Material wallets (>= $${MATERIAL_WALLET_USD}): ${materialWallets.length}
Material USD total (known prices): $${totalMaterialUsd.toFixed(2)}

Eligible transfers (ready now): ${moveRows.length}
Eligible USD total (known prices): $${totalMoveUsd.toFixed(2)}

Wallets needing gas (material): ${needsGasWallets.length}
Skipped because fee > value: ${skippedFeeGtValue.length}
Skipped because below chain min tx: ${skippedMinTx.length}
Skipped because retain consumes all available: ${skippedRetainAll.length}

Destination vault: ${destVaultId}
Skipped vaults: ${[...skipVaults].join(", ")}
Outputs:
- ${OUTDIR}/move_plan.csv
- ${OUTDIR}/needs_gas_wallets.csv
- ${OUTDIR}/skip_fee_gt_value.csv
- ${OUTDIR}/skip_below_chain_min_tx.csv
- ${OUTDIR}/skip_retain_all.csv
`);

console.log("✅ move_plan generated with minimums/retain:");
console.log(`- ${OUTDIR}/summary.txt`);
console.log(`- ${OUTDIR}/move_plan.csv`);
console.log(`- ${OUTDIR}/needs_gas_wallets.csv`);
console.log(`- ${OUTDIR}/skip_below_chain_min_tx.csv`);
console.log(`- ${OUTDIR}/skip_retain_all.csv`);
