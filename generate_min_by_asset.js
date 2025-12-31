const fs=require("fs");
const path=require("path");

const MIN_USD=Number(process.env.MIN_USD_PER_TX||"0.25");
if(!Number.isFinite(MIN_USD) || MIN_USD<=0){
  throw new Error("MIN_USD_PER_TX must be a positive number");
}

const PLAN=path.join(process.cwd(),"plan","plan.jsonl");
if(!fs.existsSync(PLAN)) throw new Error("Missing plan/plan.jsonl");

const MAP_PATH=path.join(process.cwd(),"execute","asset_to_coingecko.json");
if(!fs.existsSync(MAP_PATH)) throw new Error("Missing execute/asset_to_coingecko.json");
const MAP=JSON.parse(fs.readFileSync(MAP_PATH,"utf8"));

const RULES_PATH=path.join(process.cwd(),"execute","min_rules.json");
if(!fs.existsSync(RULES_PATH)) throw new Error("Missing execute/min_rules.json");
const RULES=JSON.parse(fs.readFileSync(RULES_PATH,"utf8"));

function isStable(a){ return a.startsWith("USDC") || a.startsWith("USDT"); }

const assetIds=new Set();
for(const line of fs.readFileSync(PLAN,"utf8").split(/\r?\n/)){
  if(!line.trim()) continue;
  assetIds.add(JSON.parse(line).assetId);
}

// Build CoinGecko ID set
const cgIds=new Set();
for(const a of assetIds){
  if(isStable(a)) continue;
  if(RULES.SELF_GAS_WITH_MINIMUM?.[a]) continue;
  const cg=MAP[a];
  if(cg) cgIds.add(cg);
}

async function fetchPrices(ids){
  if(!ids.size) return {};
  const url=new URL("https://api.coingecko.com/api/v3/simple/price");
  url.searchParams.set("ids", Array.from(ids).join(","));
  url.searchParams.set("vs_currencies","usd");
  const res=await fetch(url.toString());
  if(!res.ok) throw new Error(`CoinGecko HTTP ${res.status}`);
  return res.json();
}

(async()=>{
  const prices=await fetchPrices(cgIds);
  const minByAsset={};
  const reasonByAsset={};

  for(const a of assetIds){

    // Hard minimum assets (ATOM, DOT, XRP, ADA, etc.)
    if(RULES.SELF_GAS_WITH_MINIMUM?.[a]){
      minByAsset[a]=RULES.SELF_GAS_WITH_MINIMUM[a];
      reasonByAsset[a]="self_gas_minimum";
      continue;
    }

    // Forced USD minimums (stables, WETH)
    if(RULES.FORCE_MIN_USD?.[a]){
      minByAsset[a]=Number((RULES.FORCE_MIN_USD[a]).toFixed(12));
      reasonByAsset[a]="forced_min_usd";
      continue;
    }

    let usdMin=RULES.DEFAULT_MIN_USD;

    if(RULES.SELF_GAS_CHEAP?.includes(a)) usdMin=RULES.SELF_GAS_MIN_USD;
    if(RULES.EXPENSIVE_SELF_GAS?.includes(a)) usdMin=RULES.EXPENSIVE_SELF_GAS_MIN_USD;

    if(isStable(a)){
      minByAsset[a]=Number((usdMin).toFixed(12));
      reasonByAsset[a]="stablecoin";
      continue;
    }

    const cg=MAP[a];
    const price=prices?.[cg]?.usd;
    if(!price){
      minByAsset[a]=Infinity;
      reasonByAsset[a]="no_price_mapping";
      continue;
    }

    minByAsset[a]=Number((usdMin/price).toFixed(12));
    reasonByAsset[a]="usd_based";
  }

  const out={
    generatedAt:new Date().toISOString(),
    minUsdDefault:MIN_USD,
    minByAsset,
    reasonByAsset
  };

  const outPath=path.join(process.cwd(),"execute","min_by_asset.json");
  fs.writeFileSync(outPath, JSON.stringify(out,null,2));
  console.log("Wrote:", outPath);
})();
