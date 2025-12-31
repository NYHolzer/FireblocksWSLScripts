/**
 * Asset Coverage Analysis (focused list)
 *
 * Reads:
 *  - inventory/inventory.csv
 *  - execute/asset_to_coingecko.json          (optional but recommended)
 *  - execute/asset_price_basis.json           (optional; maps Fireblocks assetId -> basis symbol like ETH/USDC)
 *  - execute/last_prices_usd.json             (optional; basis symbol -> USD)
 *
 * Writes:
 *  - analysis/asset_coverage.csv
 *  - analysis/asset_coverage_top_wallets.json
 *  - analysis/prices_used_for_coverage.json
 */

const fs=require("fs");
const path=require("path");
const https=require("https");

const ROOT=process.cwd();
const INV=path.join(ROOT,"inventory","inventory.csv");
const MAP_CG=path.join(ROOT,"execute","asset_to_coingecko.json");
const MAP_BASIS=path.join(ROOT,"execute","asset_price_basis.json");
const BASIS_PRICES=path.join(ROOT,"execute","last_prices_usd.json");

if(!fs.existsSync(INV)) throw new Error("Missing inventory/inventory.csv");

function readJsonIfExists(p){
  try{ if(fs.existsSync(p)) return JSON.parse(fs.readFileSync(p,"utf8")); } catch(e){}
  return null;
}
const assetToCg = readJsonIfExists(MAP_CG) || {};
const assetToBasis = readJsonIfExists(MAP_BASIS) || {};
const basisPrices = readJsonIfExists(BASIS_PRICES) || {};

const ASSETS = (process.env.ASSET_LIST || "").split(",").map(s=>s.trim()).filter(Boolean);
if(ASSETS.length===0){
  console.error("ERROR: Provide ASSET_LIST env var (comma-separated Fireblocks assetIds).");
  process.exit(2);
}
const wanted=new Set(ASSETS);

function num(x){
  if(x===null||x===undefined) return 0;
  const s=String(x).trim();
  if(!s) return 0;
  const n=Number(s);
  return Number.isFinite(n)?n:0;
}

function getIdx(header, candidates){
  for(const c of candidates){
    const i=header.indexOf(c);
    if(i>=0) return i;
  }
  return -1;
}

function httpGetJson(url){
  return new Promise((resolve,reject)=>{
    https.get(url,{headers:{"User-Agent":"fireblocks-coverage/1.0"}},res=>{
      let data="";
      res.on("data",ch=>data+=ch);
      res.on("end",()=>{
        try{
          const json=JSON.parse(data);
          if(res.statusCode>=200 && res.statusCode<300) resolve(json);
          else reject(new Error(`HTTP ${res.statusCode}: ${data.slice(0,2000)}`));
        }catch(e){
          reject(new Error(`Bad JSON: ${e.message}; body=${data.slice(0,500)}`));
        }
      });
    }).on("error",reject);
  });
}

function isStableLike(assetId){
  const a=assetId.toUpperCase();
  return a.includes("USDC") || a.includes("USDT") || a.includes("TUSD") || a.includes("BUSD");
}

async function fetchCgPricesUsdForWanted(){
  // map wanted assetIds -> cg ids
  const ids=new Set();
  const used={}; // assetId -> {method, value, ref}
  for(const a of wanted){
    const cg=assetToCg[a];
    if(cg) ids.add(cg);
  }
  const cgIds=[...ids];
  const prices={};

  // chunk to be safe
  const chunkSize=250;
  for(let i=0;i<cgIds.length;i+=chunkSize){
    const chunk=cgIds.slice(i,i+chunkSize);
    const url=`https://api.coingecko.com/api/v3/simple/price?ids=${encodeURIComponent(chunk.join(","))}&vs_currencies=usd`;
    const json=await httpGetJson(url);
    for(const [id,obj] of Object.entries(json||{})){
      if(obj && typeof obj.usd==="number") prices[id]=obj.usd;
    }
  }
  return prices;
}

(async()=>{
  // Read inventory CSV
  const txt=fs.readFileSync(INV,"utf8");
  const lines=txt.split(/\r?\n/).filter(Boolean);
  const header=lines[0].split(",").map(s=>s.trim());

  const I={
    vaultId: getIdx(header, ["vaultAccountId","vaultId","id"]),
    vaultName: getIdx(header, ["vaultAccountName","name","vaultName"]),
    assetId: getIdx(header, ["assetId","asset"]),
    available: getIdx(header, ["available"]),
    total: getIdx(header, ["total","balance"])
  };
  if(I.vaultId<0) throw new Error("inventory.csv missing vault id column (vaultAccountId/vaultId/id)");
  if(I.assetId<0) throw new Error("inventory.csv missing assetId column");
  if(I.available<0 && I.total<0) throw new Error("inventory.csv missing available/total columns");

  // live CG prices for mapped assets
  let cgPrices={};
  try{
    cgPrices=await fetchCgPricesUsdForWanted();
  }catch(e){
    console.error("WARN: CoinGecko price fetch failed; will rely on basisPrices / stable heuristics. Error:", String(e.message||e));
  }

  // Build price resolver
  const priceUsed={}; // assetId -> {method, usd, ref}
  function priceUsdFor(assetId){
    // 1) CoinGecko direct mapping
    const cg=assetToCg[assetId];
    if(cg && typeof cgPrices[cg]==="number"){
      const p=cgPrices[cg];
      priceUsed[assetId]={method:"coingecko_id", usd:p, ref:cg};
      return p;
    }

    // 2) basis mapping (assetId -> basis symbol -> USD)
    const basis=assetToBasis[assetId];
    if(basis && typeof basisPrices[basis]==="number"){
      const p=basisPrices[basis];
      priceUsed[assetId]={method:"basis_symbol", usd:p, ref:basis};
      return p;
    }

    // 3) stable-like fallback (optimistic)
    if(isStableLike(assetId)){
      priceUsed[assetId]={method:"stable_fallback", usd:1.0, ref:"$1.00"};
      return 1.0;
    }

    // 4) unknown
    priceUsed[assetId]={method:"unknown", usd:null, ref:""};
    return null;
  }

  // Aggregate
  const agg={}; // assetId -> {vaultSet, rowCount, sumAvail, sumTotal, usdKnown, usdUnknownRows, top:[]}
  for(const a of wanted){
    agg[a]={vaultSet:new Set(), rowCount:0, sumAvail:0, sumTotal:0, usdKnown:0, usdUnknownRows:0, topMap:new Map()};
  }

  for(let i=1;i<lines.length;i++){
    const r=lines[i].split(",");
    const asset=r[I.assetId];
    if(!wanted.has(asset)) continue;

    const vaultId=r[I.vaultId];
    const vaultName=(I.vaultName>=0 ? (r[I.vaultName]||"") : "");
    const available=I.available>=0 ? num(r[I.available]) : 0;
    const total=I.total>=0 ? num(r[I.total]) : available;

    const a=agg[asset];
    a.rowCount++;
    a.vaultSet.add(vaultId);
    a.sumAvail += available;
    a.sumTotal += total;

    // top wallet totals by asset
    const key = vaultId;
    a.topMap.set(key, (a.topMap.get(key)||0) + total);
    // store name separately (best effort)
    // we'll join names in output later using a map
  }

  // vaultId->name map (for top wallets output)
  const vaultNameById=new Map();
  if(I.vaultName>=0){
    for(let i=1;i<lines.length;i++){
      const r=lines[i].split(",");
      const vid=r[I.vaultId];
      const vn=r[I.vaultName]||"";
      if(vn && !vaultNameById.has(vid)) vaultNameById.set(vid,vn);
    }
  }

  // Finalize USD
  const topWalletsOut={}; // assetId -> [{vaultId,name,amountTotal,usdValue?}]
  for(const asset of wanted){
    const a=agg[asset];
    const p=priceUsdFor(asset);

    if(typeof p==="number"){
      a.usdKnown = a.sumTotal * p;
    }else{
      a.usdUnknownRows = a.rowCount;
    }

    // compute top 10 wallets
    const top=[...a.topMap.entries()]
      .map(([vaultId,amt])=>({
        vaultId,
        vaultName: vaultNameById.get(vaultId)||"",
        amountTotal: amt,
        usdValue: (typeof p==="number") ? (amt*p) : null
      }))
      .sort((x,y)=> (y.amountTotal-x.amountTotal))
      .slice(0,10);

    topWalletsOut[asset]=top;
  }

  // Write CSV
  const outCsv=["assetId,vaultCount,rowCount,sumAvailable,sumTotal,priceUsdMethod,priceUsd,usdTotalKnownPrices,unknownPriceRows"];
  const assetsSorted=[...wanted].sort((a,b)=>a.localeCompare(b));
  for(const asset of assetsSorted){
    const a=agg[asset];
    const pu=priceUsed[asset] || {method:"unknown", usd:null};
    outCsv.push([
      asset,
      a.vaultSet.size,
      a.rowCount,
      a.sumAvail,
      a.sumTotal,
      pu.method,
      (pu.usd===null ? "" : pu.usd),
      (typeof pu.usd==="number" ? a.usdKnown : ""),
      (pu.usd===null ? a.rowCount : 0)
    ].join(","));
  }

  fs.writeFileSync(path.join(ROOT,"analysis","asset_coverage.csv"), outCsv.join("\n"));
  fs.writeFileSync(path.join(ROOT,"analysis","asset_coverage_top_wallets.json"), JSON.stringify(topWalletsOut,null,2));
  fs.writeFileSync(path.join(ROOT,"analysis","prices_used_for_coverage.json"), JSON.stringify({
    asOf: new Date().toISOString(),
    priceUsed,
    basisPricesLoaded: Object.keys(basisPrices).length>0,
    coingeckoFetched: Object.keys(cgPrices).length>0
  },null,2));

  console.log("✅ Wrote:");
  console.log("- analysis/asset_coverage.csv");
  console.log("- analysis/asset_coverage_top_wallets.json");
  console.log("- analysis/prices_used_for_coverage.json");
})();
