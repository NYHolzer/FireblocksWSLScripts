const fs = require("fs");
const crypto = require("crypto");
const path = require("path");

const API_KEY = process.env.FIREBLOCKS_API_KEY;
const PK_PATH = process.env.FIREBLOCKS_PRIVATE_KEY;
if (!API_KEY) throw new Error("Missing FIREBLOCKS_API_KEY");
if (!PK_PATH) throw new Error("Missing FIREBLOCKS_PRIVATE_KEY");

const BASE = process.env.FIREBLOCKS_BASE_URL || "https://api.fireblocks.io";
const EXECUTE = process.env.EXECUTE === "1"; // set to 1 for live
const BATCH = Math.max(1, Math.min(200, Number(process.env.BATCH || "20") || 20));

const PLAN = "move_plan/move_plan.csv";
if (!fs.existsSync(PLAN)) throw new Error("Missing move_plan/move_plan.csv. Run fb_build_move_plan.js first.");

const EXEC_DIR = "execute";
fs.mkdirSync(EXEC_DIR, { recursive: true });

const RUN_ID = process.env.RUN_ID || `move_${Date.now()}_${crypto.randomUUID()}`;
const JOURNAL = path.join(EXEC_DIR, `journal_${RUN_ID}.jsonl`);
const COMPLETED = path.join(EXEC_DIR, "completed_move_plan.txt");

function base64url(x){
  return Buffer.from(x).toString("base64").replace(/=/g,"").replace(/\+/g,"-").replace(/\//g,"_");
}
function sha256Hex(s){
  return crypto.createHash("sha256").update(s).digest("hex");
}
function signJwtRs256(privateKeyPem, payload){
  const header={alg:"RS256",typ:"JWT"};
  const h=base64url(JSON.stringify(header));
  const p=base64url(JSON.stringify(payload));
  const data=`${h}.${p}`;
  const signer=crypto.createSign("RSA-SHA256");
  signer.update(data); signer.end();
  const sig=signer.sign(privateKeyPem);
  return `${data}.${base64url(sig)}`;
}

const privateKeyPem = fs.readFileSync(PK_PATH, "utf8");

async function fbRequest(method, uri, bodyObj){
  const body = bodyObj ? JSON.stringify(bodyObj) : "";
  const now = Math.floor(Date.now()/1000);
  const token = signJwtRs256(privateKeyPem, {
    uri,
    nonce: crypto.randomUUID(),
    iat: now,
    exp: now + 55,
    sub: API_KEY,
    bodyHash: sha256Hex(body)
  });

  const res = await fetch(`${BASE}${uri}`, {
    method,
    headers: {
      "X-API-Key": API_KEY,
      "Authorization": `Bearer ${token}`,
      "Content-Type": "application/json"
    },
    body: body || undefined
  });

  const text = await res.text();
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${text}`);
  return JSON.parse(text);
}

function parseCsvLine(line){
  // Minimal CSV parser (handles quoted commas)
  const out=[]; let cur=""; let q=false;
  for (let i=0;i<line.length;i++){
    const c=line[i];
    if (c === '"' ){
      if (q && line[i+1] === '"'){ cur+='"'; i++; }
      else q = !q;
    } else if (c === ',' && !q){
      out.push(cur); cur="";
    } else cur+=c;
  }
  out.push(cur);
  return out;
}

const lines = fs.readFileSync(PLAN,"utf8").split(/\r?\n/).filter(Boolean);
const hdr = parseCsvLine(lines[0]);
const idx = Object.fromEntries(hdr.map((h,i)=>[h,i]));

for (const c of ["vaultId","assetId","amount","destinationVaultId"]) {
  if (idx[c] === undefined) throw new Error("move_plan.csv missing column: " + c);
}

const completed = new Set();
if (fs.existsSync(COMPLETED)){
  for (const l of fs.readFileSync(COMPLETED,"utf8").split(/\r?\n/)) {
    if (l.trim()) completed.add(l.trim());
  }
}

function rowIdOf(vaultId, assetId, dest){
  return `${vaultId}|${assetId}|${dest}`;
}

let attempted = 0, ok = 0, fail = 0, skippedDone = 0;

console.log(`Mode: ${EXECUTE ? "EXECUTE (live)" : "DRY RUN"}`);
console.log(`Batch size: ${BATCH}`);
console.log(`Journal: ${JOURNAL}`);
console.log(`Completed ledger: ${COMPLETED}`);
console.log(`Already completed loaded: ${completed.size}`);

const journal = fs.createWriteStream(JOURNAL, { flags:"a" });

(async()=>{
  for (let i=1;i<lines.length;i++){
    const row = parseCsvLine(lines[i]);
    const vaultId = row[idx.vaultId];
    const assetId = row[idx.assetId];
    const amount = row[idx.amount];
    const dest = row[idx.destinationVaultId];

    const rid = rowIdOf(vaultId, assetId, dest);
    if (completed.has(rid)) { skippedDone++; continue; }

    // only execute rows that were generated as eligible (move_plan contains only eligible rows)
    const body = {
      operation: "TRANSFER",
      assetId,
      source: { type: "VAULT_ACCOUNT", id: vaultId },
      destination: { type: "VAULT_ACCOUNT", id: dest },
      amount: String(amount),
      note: `consolidation ${rid}`,
      externalTxId: rid
    };

    attempted++;
    const preview = { rid, assetId, vaultId, dest, amount };

    if (!EXECUTE){
      journal.write(JSON.stringify({ts:Date.now(),event:"DRYRUN",...preview,body})+"\n");
      ok++;
    } else {
      try{
        const resp = await fbRequest("POST", "/v1/transactions", body);
        const txId = resp?.id || resp?.txId || "";
        journal.write(JSON.stringify({ts:Date.now(),event:"SUBMIT_OK",...preview,txId})+"\n");
        fs.appendFileSync(COMPLETED, rid + "\n");
        completed.add(rid);
        ok++;
        console.log(`SUBMIT_OK ${ok}/${attempted}: ${rid} txId=${txId}`);
      } catch(e){
        fail++;
        journal.write(JSON.stringify({ts:Date.now(),event:"SUBMIT_FAIL",...preview,error:String(e&&e.message?e.message:e)})+"\n");
        console.log(`SUBMIT_FAIL: ${rid} :: ${String(e&&e.message?e.message:e).slice(0,500)}`);
      }
    }

    if (attempted >= BATCH) break;
  }

  journal.end();
  console.log("\n✅ Batch complete");
  console.log(`attempted_this_batch=${attempted}`);
  console.log(`ok_this_batch=${ok}`);
  console.log(`failed_this_batch=${fail}`);
  console.log(`skipped_already_done=${skippedDone}`);
  console.log("\nNext: re-run the same command to submit the next batch of 20.");
})().catch(e=>{
  journal.end();
  console.error("FATAL:", e && e.stack ? e.stack : String(e));
  process.exit(1);
});
