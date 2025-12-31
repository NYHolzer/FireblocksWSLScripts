const fs=require("fs");
const path=require("path");

const ROOT=process.cwd();
const OUTDIR=path.join(ROOT,"analysis");
fs.mkdirSync(OUTDIR,{recursive:true});

const INVENTORY_CSV=path.join(ROOT,"inventory","inventory.csv");
const PLAN_CSV=path.join(ROOT,"plan","plan.csv");
const EXEC_DIR=path.join(ROOT,"execute");

const MIN_PATH=path.join(EXEC_DIR,"min_by_asset.json");
const MAP_PATH=path.join(EXEC_DIR,"asset_to_coingecko.json");
const LAST_PRICES_PATH=path.join(EXEC_DIR,"last_prices_usd.json");

const OFFLINE=process.env.OFFLINE==="1";
const MIN_USD_PER_TX=Number(process.env.MIN_USD_PER_TX||"0.01");
const STABLECOIN_MIN_USD=Number(process.env.STABLECOIN_MIN_USD||"0.25");
const APPROVALS_PER_MIN=Number(process.env.APPROVALS_PER_MIN||"8");

function mustExist(p){ if(!fs.existsSync(p)) throw new Error("Missing: "+p); }
mustExist(INVENTORY_CSV); mustExist(PLAN_CSV); mustExist(EXEC_DIR);

function num(x){ const n=Number(String(x??"").trim()); return Number.isFinite(n)?n:0; }

function readCsvLines(p){
  return fs.readFileSync(p,"utf8").split(/\r?\n/).filter(Boolean);
}
function parseHeader(line){ return line.split(",").map(s=>s.trim()); }

function detectCols(header){
  const lower=header.map(h=>h.toLowerCase());
  const pick=(cands)=>{
    for(const c of cands){
      const i=lower.indexOf(c.toLowerCase());
      if(i>=0) return i;
    }
    return -1;
  };
  return {
    inv_vault: pick(["vaultaccountid","vault_account_id","vaultid","vault_id","accountid","account_id","id","sourcevaultid","source_vault_id"]),
    inv_asset: pick(["assetid","asset_id","asset","currency","token"]),
    inv_avail: pick(["available","avail","availablebalance","available_balance","spendable","spendablebalance"]),
    inv_total: pick(["total","balance","bal","totalbalance","total_balance"]),
    p_source: pick(["sourcevaultid"]),
    p_asset: pick(["assetid"]),
    p_amount: pick(["amount"]),
    p_dest: pick(["destinationvaultid"]),
    p_requiresGas: pick(["requiresgas"]),
    p_gasAsset: pick(["gasassetid"]),
    p_gasReady: pick(["gasready"]),
  };
}

function loadJsonMaybe(p){ return fs.existsSync(p) ? JSON.parse(fs.readFileSync(p,"utf8")) : null; }

function loadCompletedLedgers(){
  const files=fs.readdirSync(EXEC_DIR).filter(f=>/^completed.*\.txt$/i.test(f));
  const set=new Set();
  for(const f of files){
    const lines=fs.readFileSync(path.join(EXEC_DIR,f),"utf8").split(/\r?\n/).filter(Boolean);
    for(const line of lines) set.add(line.trim());
  }
  return {files,set};
}

function isStable(assetId){
  const a=String(assetId||"").toUpperCase();
  return a.includes("USDC") || a.includes("USDT") || a.includes("BUSD") || a.includes("TUSD");
}

async function getPrices(assets){
  const prices={};
  const sources={}; // asset -> stable|cached|coingecko|unknown

  for(const a of assets){
    if(isStable(a)){ prices[a]=1; sources[a]="stable"; }
  }

  const cached=loadJsonMaybe(LAST_PRICES_PATH) || {};
  for(const a of assets){
    if(prices[a]!=null) continue;
    if(num(cached[a])>0){ prices[a]=num(cached[a]); sources[a]="cached"; }
  }

  if(OFFLINE) {
    for(const a of assets) if(prices[a]==null) sources[a]="unknown";
    return {prices,sources};
  }

  const map=loadJsonMaybe(MAP_PATH) || {};
  const pairs=[];
  for(const a of assets){
    if(prices[a]!=null) continue;
    if(map[a]) pairs.push([a,map[a]]);
  }
  const ids=[...new Set(pairs.map(p=>p[1]))];
  const chunks=[];
  for(let i=0;i<ids.length;i+=150) chunks.push(ids.slice(i,i+150));

  async function fetchJson(url){
    const res=await fetch(url,{headers:{"accept":"application/json"}});
    if(!res.ok) throw new Error("CoinGecko HTTP "+res.status);
    return await res.json();
  }

  for(const chunk of chunks){
    const url=`https://api.coingecko.com/api/v3/simple/price?ids=${encodeURIComponent(chunk.join(","))}&vs_currencies=usd`;
    let json=null;
    try { json=await fetchJson(url); } catch(e){ continue; }
    for(const [asset,cgid] of pairs){
      if(prices[asset]!=null) continue;
      const p=json?.[cgid]?.usd;
      if(num(p)>0){ prices[asset]=num(p); sources[asset]="coingecko"; }
    }
  }

  // mark unknown + persist merged cache
  for(const a of assets) if(prices[a]==null) sources[a]="unknown";
  const merged={...(loadJsonMaybe(LAST_PRICES_PATH)||{}),...prices};
  fs.writeFileSync(LAST_PRICES_PATH, JSON.stringify(merged,null,2));

  return {prices,sources};
}

function writeCsv(p, header, rows){
  const esc=(v)=>{
    const s=(v==null)?"":String(v);
    return /[",\n]/.test(s) ? `"${s.replace(/"/g,'""')}"` : s;
  };
  fs.writeFileSync(p, [header.join(","), ...rows.map(r=>r.map(esc).join(","))].join("\n")+"\n");
}

(async()=>{
  const runAtUTC=new Date().toISOString();

  const invLines=readCsvLines(INVENTORY_CSV);
  const planLines=readCsvLines(PLAN_CSV);
  const invHdr=parseHeader(invLines[0]);
  const planHdr=parseHeader(planLines[0]);
  const cols=detectCols([...new Set([...invHdr,...planHdr])]); // not used directly

  // Separate detection for each file
  const invCols=detectCols(invHdr);
  const planCols=detectCols(planHdr);

  for(const k of ["inv_vault","inv_asset","inv_avail","inv_total"]) if(invCols[k]<0) throw new Error("inventory.csv missing col "+k+" header="+invHdr.join("|"));
  for(const k of ["p_source","p_asset","p_amount","p_dest","p_requiresGas","p_gasAsset","p_gasReady"]) if(planCols[k]<0) throw new Error("plan.csv missing col "+k+" header="+planHdr.join("|"));

  const {files:ledgerFiles,set:completed}=loadCompletedLedgers();

  const minObj=loadJsonMaybe(MIN_PATH) || {};
  const minByAsset=(minObj.minByAsset)||{};
  const minByAssetCount=Object.keys(minByAsset).length;

  // collect remaining candidates, compute prices
  const candidates=[];
  const assets=new Set();

  for(let i=1;i<planLines.length;i++){
    const row=planLines[i].split(",");
    const source=row[planCols.p_source];
    const asset=row[planCols.p_asset];
    const amount=num(row[planCols.p_amount]);
    const dest=row[planCols.p_dest];
    const requiresGas=String(row[planCols.p_requiresGas]).toLowerCase()==="true";
    const gasAsset=row[planCols.p_gasAsset]||"";
    const gasReady=String(row[planCols.p_gasReady]).toLowerCase()==="true";

    const rid=`${source}|${asset}|${dest}`;
    assets.add(asset);

    if(completed.has(rid)) continue;

    candidates.push({rid,sourceVaultId:source,assetId:asset,amount,destinationVaultId:dest,requiresGas,gasAssetId:gasAsset,gasReady});
  }

  const {prices,sources}=await getPrices(assets);

  // policy helpers
  function minAmountFor(assetId){
    if(minByAsset[assetId]!=null) return {minAmt:minByAsset[assetId], basis:"min_by_asset"};
    if(isStable(assetId)) return {minAmt:STABLECOIN_MIN_USD, basis:"stable_usd_floor_$"+STABLECOIN_MIN_USD};
    const p=prices[assetId];
    if(num(p)>0) return {minAmt:MIN_USD_PER_TX / p, basis:"usd_floor_$"+MIN_USD_PER_TX};
    return {minAmt:null, basis:"no_price_no_floor"};
  }

  // classify + totals by reason
  const remaining=[];
  const totalsByReason={READY_TO_EXECUTE:0, NEEDS_GAS:0, BELOW_MIN:0, UNKNOWN_PRICE:0};
  const rowsByReason={READY_TO_EXECUTE:0, NEEDS_GAS:0, BELOW_MIN:0, UNKNOWN_PRICE:0};
  const walletsByReason={READY_TO_EXECUTE:new Set(), NEEDS_GAS:new Set(), BELOW_MIN:new Set(), UNKNOWN_PRICE:new Set()};

  for(const r of candidates){
    const p=prices[r.assetId];
    const priceKnown=num(p)>0 || isStable(r.assetId);
    const estUSD=priceKnown ? r.amount * (isStable(r.assetId)?1:p) : null;

    const {minAmt,basis}=minAmountFor(r.assetId);

    let reason="READY_TO_EXECUTE";
    if(r.requiresGas && !r.gasReady) reason="NEEDS_GAS";
    else if(minAmt!=null && r.amount < minAmt) reason="BELOW_MIN";
    if(!priceKnown) reason = (reason==="READY_TO_EXECUTE" ? "UNKNOWN_PRICE" : reason); // keep original if needs gas/below min even if unknown

    remaining.push({...r, priceUSD: priceKnown ? (isStable(r.assetId)?1:p) : null, priceSource: sources[r.assetId]||"unknown", estUSD, minAmt, minBasis:basis, reason});

    if(estUSD!=null) totalsByReason[reason]+=estUSD;
    rowsByReason[reason]++;

    walletsByReason[reason].add(r.sourceVaultId);
  }

  const totalKnownUSD=remaining.filter(x=>x.estUSD!=null).reduce((s,x)=>s+x.estUSD,0);
  const totalActionableNowUSD=remaining.filter(x=>x.reason==="READY_TO_EXECUTE" && x.estUSD!=null).reduce((s,x)=>s+x.estUSD,0);
  const totalActionableAfterFundingUSD=remaining.filter(x=>(x.reason==="READY_TO_EXECUTE"||x.reason==="NEEDS_GAS") && x.estUSD!=null).reduce((s,x)=>s+x.estUSD,0);
  const totalBelowMinUSD=remaining.filter(x=>x.reason==="BELOW_MIN" && x.estUSD!=null).reduce((s,x)=>s+x.estUSD,0);

  // prices_used.csv
  const pricesUsedRows=[...assets].sort().map(a=>[
    a,
    (prices[a]==null && !isStable(a)) ? "" : (isStable(a)?1:prices[a]),
    sources[a]||"unknown",
    runAtUTC
  ]);
  writeCsv(path.join(OUTDIR,"prices_used.csv"), ["assetId","priceUSD","priceSource","usedAtUTC"], pricesUsedRows);

  // policy_used.txt
  const policyTxt = [
    "Policy Used for BELOW_MIN Classification",
    "=======================================",
    "",
    `Run at (UTC): ${runAtUTC}`,
    "",
    "Order of precedence for minimum transfer amount:",
    "1) execute/min_by_asset.json (minByAsset[assetId]) if present",
    "2) Stablecoins: STABLECOIN_MIN_USD threshold interpreted as token units at $1 (USDC/USDT/TUSD/BUSD family)",
    "3) Non-stables: MIN_USD_PER_TX converted to token units using priceUSD (minAmt = MIN_USD_PER_TX / priceUSD)",
    "4) If price is unknown and no per-asset min exists, we do NOT auto-skip via USD floor.",
    "",
    `Configured parameters:`,
    `- MIN_USD_PER_TX: $${MIN_USD_PER_TX}`,
    `- STABLECOIN_MIN_USD: $${STABLECOIN_MIN_USD}`,
    `- OFFLINE: ${OFFLINE ? "yes" : "no"}`,
    `- min_by_asset entries loaded: ${minByAssetCount}`,
    "",
    "Price sources:",
    "- stable: forced to $1",
    "- cached: execute/last_prices_usd.json",
    "- coingecko: fetched at runtime via execute/asset_to_coingecko.json mapping",
    "- unknown: no mapping or fetch failure",
    ""
  ].join("\n");
  fs.writeFileSync(path.join(OUTDIR,"policy_used.txt"), policyTxt + "\n");

  // by_reason_detailed.csv
  const byReasonRows = ["READY_TO_EXECUTE","NEEDS_GAS","BELOW_MIN","UNKNOWN_PRICE"].map(k=>[
    k,
    walletsByReason[k].size,
    rowsByReason[k],
    totalsByReason[k].toFixed(2)
  ]);
  writeCsv(path.join(OUTDIR,"by_reason_detailed.csv"), ["reason","walletCount","rowCount","sumUSD_known"], byReasonRows);

  // remaining_rows.csv with policy fields
  writeCsv(path.join(OUTDIR,"remaining_rows_v2.csv"),
    ["rowId","sourceVaultId","assetId","amount","destinationVaultId","requiresGas","gasAssetId","gasReady","priceUSD","priceSource","estUSD","minAmt","minBasis","reason"],
    remaining.map(r=>[
      r.rid,r.sourceVaultId,r.assetId,r.amount,r.destinationVaultId,r.requiresGas,r.gasAssetId,r.gasReady,
      r.priceUSD==null?"":r.priceUSD,r.priceSource,r.estUSD==null?"":r.estUSD,
      r.minAmt==null?"":r.minAmt,r.minBasis,r.reason
    ])
  );

  // human summary
  const approvalsPerHour = APPROVALS_PER_MIN*60;
  const readyRows = rowsByReason.READY_TO_EXECUTE;
  const estHours = approvalsPerHour>0 ? (readyRows/approvalsPerHour) : 0;

  const summary = [
    "Fireblocks Consolidation/Liquidation Analysis (More Specific)",
    "============================================================",
    "",
    `Run at (UTC): ${runAtUTC}`,
    "",
    "Inputs:",
    `- inventory: ${INVENTORY_CSV}`,
    `- plan:      ${PLAN_CSV}`,
    `- ledgers (${ledgerFiles.length}): ${ledgerFiles.join(", ")}`,
    "",
    "Totals (known prices):",
    `- Total remaining USD (READY + NEEDS_GAS + BELOW_MIN): $${totalKnownUSD.toFixed(2)}`,
    `- Actionable now (READY only):                         $${totalActionableNowUSD.toFixed(2)}`,
    `- Actionable after funding gas (READY + NEEDS_GAS):     $${totalActionableAfterFundingUSD.toFixed(2)}`,
    `- Not cost-effective by policy (BELOW_MIN):             $${totalBelowMinUSD.toFixed(2)}`,
    "",
    "Counts:",
    `- Remaining rows: ${remaining.length}`,
    `- Unique wallets represented: ${new Set(remaining.map(x=>x.sourceVaultId)).size}`,
    "",
    "Breakdown:",
    ...byReasonRows.map(r=>`- ${r[0]}: wallets=${r[1]} rows=${r[2]} USD=${r[3]}`),
    "",
    "Time estimate (approvals bottleneck):",
    `- Assumption: ${APPROVALS_PER_MIN} approvals/min`,
    `- READY rows: ${readyRows} => ~${estHours.toFixed(2)} hours of approvals`,
    "",
    "Outputs:",
    "- analysis/policy_used.txt",
    "- analysis/prices_used.csv",
    "- analysis/by_reason_detailed.csv",
    "- analysis/remaining_rows_v2.csv",
    ""
  ].join("\n");

  fs.writeFileSync(path.join(OUTDIR,"report_summary_v2.txt"), summary + "\n");
  console.log("✅ Wrote analysis/report_summary_v2.txt and detailed outputs.");
})().catch(e=>{
  console.error("ERROR:", e&&e.stack?e.stack:String(e));
  process.exit(1);
});
