const fs=require("fs");
const path=require("path");
const https=require("https");

const ROOT=process.cwd();
const INV=path.join(ROOT,"inventory","inventory.csv");
const PLAN=path.join(ROOT,"plan","plan.csv");
const GAS_FEE=path.join(ROOT,"execute","gas_fee_native.json");

if(!fs.existsSync(INV)) throw new Error("Missing inventory/inventory.csv");
if(!fs.existsSync(PLAN)) throw new Error("Missing plan/plan.csv");
if(!fs.existsSync(GAS_FEE)) throw new Error("Missing execute/gas_fee_native.json");

function readJsonIfExists(p){ try{ if(fs.existsSync(p)) return JSON.parse(fs.readFileSync(p,"utf8")); }catch{} return null; }
const assetToCg = readJsonIfExists(path.join(ROOT,"execute","asset_to_coingecko.json")) || {};
const assetToBasis = readJsonIfExists(path.join(ROOT,"execute","asset_price_basis.json")) || {};
const basisPrices = readJsonIfExists(path.join(ROOT,"execute","last_prices_usd.json")) || {};
const gasFeeNative = JSON.parse(fs.readFileSync(GAS_FEE,"utf8"));

const OUT_DIR=path.join(ROOT,"analysis");
fs.mkdirSync(OUT_DIR,{recursive:true});

const USE_LIVE = process.env.USE_LIVE_PRICES !== "0"; // default yes
const STRICT = process.env.STRICT_PRICING === "1";
const STABLE_FALLBACK = (!STRICT);
const FEE_MULT = Number(process.env.FEE_MULT||"1.0");           // safety factor on gas fee
const MIN_NET_USD = Number(process.env.MIN_NET_USD||"0.00");    // require (value - feeUSD) >= this
const MIN_GROSS_USD = Number(process.env.MIN_GROSS_USD||"0.00"); // require value >= this regardless of fee
const MIN_TX_POLICY_USD = Number(process.env.MIN_TX_POLICY_USD||"0.00"); // optional hard floor (e.g. 0.01)

function num(x){ const n=Number(String(x??"").trim()); return Number.isFinite(n)?n:0; }
function csvEscape(v){ if(v==null) return ""; const s=String(v); return /[",\n]/.test(s)? `"${s.replace(/"/g,'""')}"`:s; }
function isStableLike(a){ a=String(a||"").toUpperCase(); return a.includes("USDC")||a.includes("USDT")||a.includes("TUSD")||a.includes("BUSD"); }

function httpGetJson(url){
  return new Promise((resolve,reject)=>{
    https.get(url,{headers:{"User-Agent":"fb-breakeven/1.0"}},res=>{
      let data=""; res.on("data",c=>data+=c);
      res.on("end",()=>{ try{
        const j=JSON.parse(data);
        if(res.statusCode>=200&&res.statusCode<300) resolve(j);
        else reject(new Error(`HTTP ${res.statusCode}: ${data.slice(0,2000)}`));
      }catch(e){ reject(new Error(`Bad JSON: ${e.message}`)); }});
    }).on("error",reject);
  });
}

function getIdx(h,cands){ for(const c of cands){ const i=h.indexOf(c); if(i>=0) return i; } return -1; }

function parsePlanGasMap(){
  const lines=fs.readFileSync(PLAN,"utf8").split(/\r?\n/).filter(Boolean);
  const h=lines[0].split(",").map(s=>s.trim());
  const I={
    asset:getIdx(h,["assetId"]),
    req:getIdx(h,["requiresGas"]),
    gas:getIdx(h,["gasAssetId"])
  };
  for(const k of Object.keys(I)) if(I[k]<0) throw new Error(`plan.csv missing column: ${k}`);
  const m=new Map(); // assetId -> gasAssetId (if requiresGas)
  for(let i=1;i<lines.length;i++){
    const r=lines[i].split(",");
    const asset=r[I.asset];
    const req=String(r[I.req]).toLowerCase()==="true";
    const gas=r[I.gas]||"";
    if(req && gas) m.set(asset,gas);
  }
  return m;
}

function parseInventory(){
  const lines=fs.readFileSync(INV,"utf8").split(/\r?\n/).filter(Boolean);
  const h=lines[0].split(",").map(s=>s.trim());
  const I={
    vault:getIdx(h,["vaultAccountId","vaultId","id"]),
    asset:getIdx(h,["assetId","asset"]),
    total:getIdx(h,["total","balance"]),
    avail:getIdx(h,["available"])
  };
  if(I.vault<0) throw new Error("inventory missing vaultAccountId/vaultId");
  if(I.asset<0) throw new Error("inventory missing assetId");
  if(I.total<0 && I.avail<0) throw new Error("inventory missing total/balance/available");
  const out=[];
  for(let i=1;i<lines.length;i++){
    const r=lines[i].split(",");
    const vault=r[I.vault];
    const asset=r[I.asset];
    const total= I.total>=0 ? num(r[I.total]) : num(r[I.avail]);
    if(!(total>0)) continue;
    out.push({vault,asset,total});
  }
  return out;
}

function percentile(sorted, p){
  if(sorted.length===0) return 0;
  const idx = (sorted.length-1)*p;
  const lo=Math.floor(idx), hi=Math.ceil(idx);
  if(lo===hi) return sorted[lo];
  const w=idx-lo;
  return sorted[lo]*(1-w)+sorted[hi]*w;
}

(async()=>{
  const gasMap=parsePlanGasMap();
  const inv=parseInventory();

  // Determine which assets we need prices for (assets + gas assets)
  const assets=new Set(inv.map(r=>r.asset));
  for(const g of gasMap.values()) assets.add(g);

  const neededCg=new Set();
  for(const a of assets){
    const cg=assetToCg[a];
    if(cg) neededCg.add(cg);
  }

  let cgPrices={};
  if(USE_LIVE && neededCg.size){
    const ids=[...neededCg];
    const chunk=250;
    for(let i=0;i<ids.length;i+=chunk){
      const part=ids.slice(i,i+chunk);
      const url=`https://api.coingecko.com/api/v3/simple/price?ids=${encodeURIComponent(part.join(","))}&vs_currencies=usd`;
      const j=await httpGetJson(url);
      for(const [id,obj] of Object.entries(j||{})){
        if(obj && typeof obj.usd==="number") cgPrices[id]=obj.usd;
      }
    }
  }

  const priceUsed={};
  function priceUsd(assetId){
    const cg=assetToCg[assetId];
    if(cg && typeof cgPrices[cg]==="number"){
      priceUsed[assetId]={method:"coingecko",ref:cg,usd:cgPrices[cg]};
      return cgPrices[cg];
    }
    const basis=assetToBasis[assetId];
    if(basis && typeof basisPrices[basis]==="number"){
      priceUsed[assetId]={method:"basis_symbol",ref:basis,usd:basisPrices[basis]};
      return basisPrices[basis];
    }
    if(STABLE_FALLBACK && isStableLike(assetId)){
      priceUsed[assetId]={method:"stable_fallback",ref:"$1",usd:1};
      return 1;
    }
    priceUsed[assetId]={method:"unknown",ref:"",usd:null};
    return null;
  }

  // Group per asset: list of per-vault USD values
  const perAsset=new Map(); // asset -> {vaults:Set, usdVals:number[], totalNative:number, totalUsdKnown:number, unknownRows:number}
  for(const r of inv){
    if(!perAsset.has(r.asset)){
      perAsset.set(r.asset,{vaults:new Set(), usdVals:[], totalNative:0, totalUsdKnown:0, unknownRows:0});
    }
    const g=perAsset.get(r.asset);
    g.vaults.add(r.vault);
    g.totalNative += r.total;
    const p=priceUsd(r.asset);
    if(typeof p==="number"){
      const usd=r.total*p;
      g.usdVals.push(usd);
      g.totalUsdKnown += usd;
    }else{
      g.unknownRows++;
    }
  }

  // compute feeUSD by asset using gasMap + gasFeeNative
  function feeUsdForAsset(assetId){
    const gasAsset = gasMap.get(assetId) || null;
    if(!gasAsset) return {gasAsset:null, feeUsd:null, feeNative:null};
    const feeNative = gasFeeNative[gasAsset];
    if(!(typeof feeNative==="number")) return {gasAsset, feeUsd:null, feeNative:null};
    const gp = priceUsd(gasAsset);
    if(!(typeof gp==="number")) return {gasAsset, feeUsd:null, feeNative};
    const feeUsd = feeNative * gp * FEE_MULT;
    return {gasAsset, feeUsd, feeNative};
  }

  // output
  const outLines=[];
  outLines.push([
    "assetId","vaultCount","rowCount",
    "totalUsdKnown","avgUsd","medianUsd","p90Usd","maxUsd",
    "requiresGas","gasAssetId","feeUsdAssumed","breakevenUsd",
    "countAboveBreakeven","usdAboveBreakeven","unknownPriceRows",
    "recommendation"
  ].join(","));

  // helpers: breakeven rule
  // eligible if (usd >= max(MIN_GROSS_USD, MIN_TX_POLICY_USD, feeUsd + MIN_NET_USD)) when feeUsd known
  // if feeUsd unknown but requiresGas => we cannot compute breakeven -> recommendation "NEEDS_FEE_ASSUMPTION"
  const rows=[];
  for(const [asset,g] of perAsset.entries()){
    const vals=g.usdVals.slice().sort((a,b)=>a-b);
    const vaultCount=g.vaults.size;
    const rowCount = (g.usdVals.length + g.unknownRows);
    const totalUsd=g.totalUsdKnown;
    const avg = vals.length? (totalUsd/vals.length) : 0;
    const med = percentile(vals,0.5);
    const p90 = percentile(vals,0.9);
    const max = vals.length? vals[vals.length-1] : 0;

    const {gasAsset, feeUsd} = feeUsdForAsset(asset);
    const requiresGas = !!gasAsset;

    let breakevenUsd=null;
    let countAbove=0;
    let usdAbove=0;
    let rec="";

    if(requiresGas){
      if(typeof feeUsd==="number"){
        breakevenUsd = Math.max(MIN_GROSS_USD, MIN_TX_POLICY_USD, feeUsd + MIN_NET_USD);
        for(const u of vals){
          if(u >= breakevenUsd){
            countAbove++;
            usdAbove += u;
          }
        }
        // recommendation heuristic
        const ratio = (vaultCount>0) ? (totalUsd/vaultCount) : 0;
        if(totalUsd===0) rec="SKIP";
        else if(countAbove===0) rec="SKIP_ALL_BELOW_BREAKEVEN";
        else if(ratio<=1) rec="REVIEW_DISTRIBUTION_LOW_AVG";
        else rec="EXECUTE_TOP_DOWN";
      } else {
        rec="NEEDS_FEE_ASSUMPTION_OR_PRICE";
      }
    } else {
      // non-gas asset: still apply policy floors if desired
      breakevenUsd = Math.max(MIN_GROSS_USD, MIN_TX_POLICY_USD);
      for(const u of vals){
        if(u >= breakevenUsd){
          countAbove++;
          usdAbove += u;
        }
      }
      rec = (countAbove>0) ? "EXECUTE" : "SKIP_ALL_BELOW_POLICY";
    }

    rows.push({
      asset,
      vaultCount,
      rowCount,
      totalUsdKnown: totalUsd,
      avgUsd: avg,
      medianUsd: med,
      p90Usd: p90,
      maxUsd: max,
      requiresGas,
      gasAssetId: gasAsset || "",
      feeUsdAssumed: (typeof feeUsd==="number") ? feeUsd : "",
      breakevenUsd: (typeof breakevenUsd==="number") ? breakevenUsd : "",
      countAbove,
      usdAbove,
      unknownPriceRows: g.unknownRows,
      recommendation: rec
    });
  }

  // prioritize “avg <= $1” assets first (your ask), then by totalUsd desc
  rows.sort((a,b)=>{
    const aAvg = a.vaultCount? (a.totalUsdKnown/a.vaultCount):0;
    const bAvg = b.vaultCount? (b.totalUsdKnown/b.vaultCount):0;
    const aLow = (aAvg<=1)?0:1;
    const bLow = (bAvg<=1)?0:1;
    if(aLow!==bLow) return aLow-bLow;
    return (b.totalUsdKnown-a.totalUsdKnown);
  });

  for(const r of rows){
    outLines.push([
      r.asset,
      r.vaultCount,
      r.rowCount,
      r.totalUsdKnown.toFixed(6),
      r.avgUsd.toFixed(6),
      r.medianUsd.toFixed(6),
      r.p90Usd.toFixed(6),
      r.maxUsd.toFixed(6),
      r.requiresGas ? "true":"false",
      r.gasAssetId,
      (r.feeUsdAssumed===""?"":Number(r.feeUsdAssumed).toFixed(6)),
      (r.breakevenUsd===""?"":Number(r.breakevenUsd).toFixed(6)),
      r.countAbove,
      r.usdAbove.toFixed(6),
      r.unknownPriceRows,
      r.recommendation
    ].map(csvEscape).join(","));
  }

  fs.writeFileSync(path.join(OUT_DIR,"breakeven_by_asset.csv"), outLines.join("\n"));
  fs.writeFileSync(path.join(OUT_DIR,"prices_used_breakeven.json"), JSON.stringify({asOf:new Date().toISOString(), priceUsed},null,2));

  console.log("✅ Wrote:");
  console.log("- analysis/breakeven_by_asset.csv");
  console.log("- analysis/prices_used_breakeven.json");
  console.log("");
  console.log("Tip: view the ‘low avg USD’ assets first:");
  console.log("  column -s, -t analysis/breakeven_by_asset.csv | sed -n '1,80p'");
})();
