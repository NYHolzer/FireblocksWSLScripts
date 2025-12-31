const fs=require("fs");
const path=require("path");
const crypto=require("crypto");

const API_KEY=process.env.FIREBLOCKS_API_KEY;
const PK_PATH=process.env.FIREBLOCKS_PRIVATE_KEY;
if(!API_KEY) throw new Error("Missing FIREBLOCKS_API_KEY");
if(!PK_PATH) throw new Error("Missing FIREBLOCKS_PRIVATE_KEY");

const BASE=process.env.FIREBLOCKS_BASE_URL||"https://api.fireblocks.io";
const EXECUTE=process.env.EXECUTE==="1";
const BATCH=Math.max(1, Math.min(500, Number(process.env.BATCH||"50")||50));

const ROOT=process.cwd();
const OUTDIR=path.join(ROOT,"execute");
fs.mkdirSync(OUTDIR,{recursive:true});

const INPUT=path.join(ROOT,"analysis","remaining_rows_v2.csv");
if(!fs.existsSync(INPUT)) throw new Error(`Missing ${INPUT}. Run analysis v2 first.`);

const COMPLETED=path.join(OUTDIR,"completed_ready.txt");
if(!fs.existsSync(COMPLETED)) fs.writeFileSync(COMPLETED,"");

const RUN_ID=process.env.RUN_ID || `ready_${Date.now()}_${crypto.randomUUID()}`;
const JOURNAL=path.join(OUTDIR,`journal_${RUN_ID}.jsonl`);

function base64url(buf){
  return Buffer.from(buf).toString("base64").replace(/=/g,"").replace(/\+/g,"-").replace(/\//g,"_");
}
function sha256Hex(s){
  return crypto.createHash("sha256").update(s).digest("hex");
}
function signJwtRs256(privateKeyPem,payload){
  const header={alg:"RS256",typ:"JWT"};
  const h=base64url(JSON.stringify(header));
  const p=base64url(JSON.stringify(payload));
  const data=`${h}.${p}`;
  const signer=crypto.createSign("RSA-SHA256");
  signer.update(data); signer.end();
  const sig=signer.sign(privateKeyPem);
  return `${data}.${base64url(sig)}`;
}
function readCsvLines(p){
  return fs.readFileSync(p,"utf8").split(/\r?\n/).filter(Boolean);
}
function num(x){
  const n=Number(String(x??"").replace(/"/g,"").trim());
  return Number.isFinite(n)?n:0;
}
function appendLine(p,line){ fs.appendFileSync(p, line+"\n"); }

function loadCompletedSet(){
  const lines=fs.readFileSync(COMPLETED,"utf8").split(/\r?\n/).filter(Boolean);
  return new Set(lines.map(s=>s.trim()));
}

async function fbRequest(method, urlPath, bodyObj){
  const privateKeyPem=fs.readFileSync(PK_PATH,"utf8");
  const now=Math.floor(Date.now()/1000);
  const bodyStr = bodyObj ? JSON.stringify(bodyObj) : "";
  const token=signJwtRs256(privateKeyPem,{
    uri: urlPath,
    nonce: crypto.randomUUID(),
    iat: now,
    exp: now+55,
    sub: API_KEY,
    bodyHash: sha256Hex(bodyStr || "")
  });

  const res=await fetch(`${BASE}${urlPath}`,{
    method,
    headers:{
      "X-API-Key": API_KEY,
      "Authorization": `Bearer ${token}`,
      "Content-Type":"application/json"
    },
    body: bodyObj ? bodyStr : undefined
  });

  const text=await res.text();
  let json=null;
  try{ json=text?JSON.parse(text):null; }catch{ json={raw:text}; }
  return {ok:res.ok, status:res.status, json};
}

function journal(obj){
  fs.appendFileSync(JOURNAL, JSON.stringify({...obj, ts:new Date().toISOString()})+"\n");
}

(async()=>{
  const lines=readCsvLines(INPUT);
  const hdr=lines[0].split(",").map(s=>s.trim());

  const idx=(name)=>{
    const i=hdr.indexOf(name);
    if(i<0) throw new Error(`Missing column ${name} in ${INPUT}`);
    return i;
  };

  const I={
    rowId: idx("rowId"),
    source: idx("sourceVaultId"),
    asset: idx("assetId"),
    amount: idx("amount"),
    dest: idx("destinationVaultId"),
    requiresGas: idx("requiresGas"),
    gasReady: idx("gasReady"),
    reason: idx("reason"),
    estUSD: idx("estUSD"),
  };

  const completed=loadCompletedSet();

  const candidates=[];
  for(let i=1;i<lines.length;i++){
    const row=lines[i].split(",");
    const rid=row[I.rowId];
    const reason=row[I.reason];
    const requiresGas=String(row[I.requiresGas]).toLowerCase()==="true";
    const gasReady=String(row[I.gasReady]).toLowerCase()==="true";
    const estUSD=row[I.estUSD] ? num(row[I.estUSD]) : null;

    if(reason!=="READY_TO_EXECUTE") continue;
    if(requiresGas && !gasReady) continue; // belt & suspenders
    if(completed.has(rid)) continue;

    candidates.push({
      rowId: rid,
      sourceVaultId: row[I.source],
      assetId: row[I.asset],
      amount: row[I.amount], // keep as string for precision
      destinationVaultId: row[I.dest],
      estUSD
    });
  }

  candidates.sort((a,b)=>(b.estUSD||0)-(a.estUSD||0));

  const batch=candidates.slice(0,BATCH);

  console.log(`Mode: ${EXECUTE ? "EXECUTE (live)" : "DRY RUN"}`);
  console.log(`Batch size: ${BATCH}`);
  console.log(`Input: ${INPUT}`);
  console.log(`Journal: ${JOURNAL}`);
  console.log(`Completed ledger: ${COMPLETED}`);
  console.log(`Eligible READY rows remaining: ${candidates.length}`);
  console.log(`Attempting this batch: ${batch.length}`);
  console.log("");

  let okCount=0, failCount=0;

  for(let i=0;i<batch.length;i++){
    const x=batch[i];
    const preview=`${x.sourceVaultId}|${x.assetId}|${x.destinationVaultId}|${x.amount}`;
    if(!EXECUTE){
      console.log(`DRYRUN ${i+1}/${batch.length}: ${preview} estUSD=${x.estUSD ?? "?"}`);
      journal({event:"DRYRUN", rowId:x.rowId, preview, body:{
        operation:"TRANSFER", assetId:x.assetId,
        source:{type:"VAULT_ACCOUNT", id:String(x.sourceVaultId)},
        destination:{type:"VAULT_ACCOUNT", id:String(x.destinationVaultId)},
        amount:String(x.amount),
        externalTxId:`${RUN_ID}:${x.rowId}`
      }});
      okCount++;
      continue;
    }

    const body={
      operation:"TRANSFER",
      assetId:x.assetId,
      source:{type:"VAULT_ACCOUNT", id:String(x.sourceVaultId)},
      destination:{type:"VAULT_ACCOUNT", id:String(x.destinationVaultId)},
      amount:String(x.amount),
      externalTxId:`${RUN_ID}:${x.rowId}`
    };

    const resp=await fbRequest("POST","/v1/transactions",body);

    if(resp.ok){
      okCount++;
      const txId=resp.json?.id || resp.json?.txId || "(no-id)";
      console.log(`SUBMIT_OK ${okCount}/${batch.length}: ${x.rowId} txId=${txId}`);
      journal({event:"SUBMIT_OK", rowId:x.rowId, txId, body});
      appendLine(COMPLETED, x.rowId);
    }else{
      failCount++;
      const msg=JSON.stringify(resp.json);
      console.log(`SUBMIT_FAIL: ${x.rowId} :: HTTP ${resp.status} ${msg}`);
      journal({event:"SUBMIT_FAIL", rowId:x.rowId, status:resp.status, error:resp.json, body});
      // NOTE: we do NOT mark completed on failure
    }
  }

  console.log("");
  console.log("✅ READY batch complete");
  console.log(`attempted=${batch.length}`);
  console.log(`ok=${okCount}`);
  console.log(`failed=${failCount}`);
  console.log("");
  console.log("Re-run the same command to submit the next batch (it will skip completed).");
})().catch(e=>{
  console.error("ERROR:", e&&e.stack?e.stack:String(e));
  process.exit(1);
});
