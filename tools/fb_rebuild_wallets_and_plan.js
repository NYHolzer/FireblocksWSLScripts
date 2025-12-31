const fs=require("fs");
const path=require("path");

const ROOT=process.cwd();
const INV=path.join(ROOT,"inventory","inventory.csv");
if(!fs.existsSync(INV)) throw new Error("Missing inventory/inventory.csv");

const SKIP_VAULTS=new Set((process.env.SKIP_VAULTS||"94828,94797").split(",").map(s=>s.trim()).filter(Boolean));

function num(x){ const n=Number(x); return Number.isFinite(n)?n:0; }

// prices snapshot
const PRICES_PATH=path.join(ROOT,"execute","last_prices_usd.json");
let PRICES={};
if(fs.existsSync(PRICES_PATH)){
  try{ PRICES=JSON.parse(fs.readFileSync(PRICES_PATH,"utf8")); }catch{}
}
function priceUsd(assetId){
  if(PRICES[assetId]!=null){
    const v=Number(PRICES[assetId]);
    if(Number.isFinite(v)) return v;
  }
  if (/^(USDC|USDT|DAI|TUSD|BUSD)(_|$)/.test(assetId)) return 1;
  return null;
}

const lines=fs.readFileSync(INV,"utf8").trim().split(/\r?\n/);
const H=lines[0].split(",");

const I={
  vaultId: H.indexOf("vaultId"),
  vaultName: H.indexOf("vaultName"),
  assetId: H.indexOf("assetId"),
  available: H.indexOf("available"),
  total: H.indexOf("total"),
  hiddenOnUI: H.indexOf("hiddenOnUI"), // may exist in some outputs
};
for(const k of ["vaultId","vaultName","assetId","available","total"]){
  if(I[k]<0) throw new Error(`inventory.csv missing column: ${k}`);
}

// aggregate per wallet
const wallets=new Map(); // vaultId -> {vaultId,vaultName,assets:[],usdKnown,unknownRows,assetCount,sumAvailableRows,hiddenOnUI?}
for(let i=1;i<lines.length;i++){
  const r=lines[i].split(",");
  const vaultId=r[I.vaultId];
  if(!vaultId) continue;
  if(SKIP_VAULTS.has(String(vaultId))) continue;

  const avail=num(r[I.available]);
  const tot=num(r[I.total]);
  if(!(avail>0 || tot>0)) continue;

  const assetId=r[I.assetId];
  if(!wallets.has(vaultId)){
    wallets.set(vaultId,{
      vaultId,
      vaultName:r[I.vaultName]||"",
      hiddenOnUI: I.hiddenOnUI>=0 ? (r[I.hiddenOnUI]||"") : "",
      usdKnown:0,
      unknownRows:0,
      assetCount:0,
    });
  }
  const w=wallets.get(vaultId);
  w.assetCount++;

  const p=priceUsd(assetId);
  if(p==null) w.unknownRows++;
  else w.usdKnown += avail * p;
}

// build wallet_totals.csv
const walletTotals=[...wallets.values()].map(w=>({
  vaultId:w.vaultId,
  vaultName:w.vaultName,
  assetRowCount:w.assetCount,
  usdKnown: w.usdKnown.toFixed(2),
  unknownAssetRows:w.unknownRows,
  hiddenOnUI: w.hiddenOnUI
}));
walletTotals.sort((a,b)=>Number(b.usdKnown)-Number(a.usdKnown));

// zero wallets: from inventory.csv we cannot enumerate empty vaults (only vaults with rows appear).
// So we output "zero rows in inventory" as empty file for now, and handle UI hiding via a separate UI sync step.
const zeroWallets=[];

fs.mkdirSync("analysis",{recursive:true});
function writeCsv(file, rows){
  if(!rows.length){ fs.writeFileSync(file,"EMPTY\n"); return; }
  const keys=Object.keys(rows[0]);
  const out=[keys.join(",")];
  for(const row of rows){
    out.push(keys.map(k=>String(row[k]).replaceAll("\n"," ")).join(","));
  }
  fs.writeFileSync(file,out.join("\n")+"\n");
}

writeCsv("analysis/wallet_totals.csv", walletTotals);
writeCsv("analysis/zero_wallets_to_hide.csv", zeroWallets);

console.log("✅ Rebuild successful (inventory.csv only)");
console.log("Wallets with nonzero inventory (excluding skip vaults):", walletTotals.length);
console.log("Output: analysis/wallet_totals.csv");
console.log("Note: zero_wallets_to_hide.csv is EMPTY because inventory.csv cannot list truly-empty vaults.");
