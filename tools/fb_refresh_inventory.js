const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const API_KEY = process.env.FIREBLOCKS_API_KEY;
const PK_PATH = process.env.FIREBLOCKS_PRIVATE_KEY;
const BASE = process.env.FIREBLOCKS_BASE_URL || "https://api.fireblocks.io";

if (!API_KEY) throw new Error("Missing FIREBLOCKS_API_KEY");
if (!PK_PATH) throw new Error("Missing FIREBLOCKS_PRIVATE_KEY");
if (!fs.existsSync(PK_PATH)) throw new Error("Private key file not found: " + PK_PATH);

const privateKeyPem = fs.readFileSync(PK_PATH, "utf8");

function base64url(bufOrStr) {
  const b = Buffer.isBuffer(bufOrStr) ? bufOrStr : Buffer.from(bufOrStr);
  return b.toString("base64").replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
}
function sha256Hex(s) {
  return crypto.createHash("sha256").update(s).digest("hex");
}
function signJwtRs256(payload) {
  const header = { alg: "RS256", typ: "JWT" };
  const h = base64url(JSON.stringify(header));
  const p = base64url(JSON.stringify(payload));
  const data = `${h}.${p}`;
  const signer = crypto.createSign("RSA-SHA256");
  signer.update(data);
  signer.end();
  const sig = signer.sign(privateKeyPem);
  return `${data}.${base64url(sig)}`;
}
async function fbRequest(method, uriPath, bodyObj) {
  const now = Math.floor(Date.now() / 1000);
  const bodyStr = bodyObj ? JSON.stringify(bodyObj) : "";
  const token = signJwtRs256({
    uri: uriPath,
    nonce: crypto.randomUUID(),
    iat: now,
    exp: now + 55,
    sub: API_KEY,
    bodyHash: sha256Hex(bodyStr || "")
  });

  const res = await fetch(`${BASE}${uriPath}`, {
    method,
    headers: {
      "X-API-Key": API_KEY,
      "Authorization": `Bearer ${token}`,
      "Content-Type": "application/json"
    },
    body: bodyObj ? bodyStr : undefined
  });

  const text = await res.text();
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${text.slice(0, 2000)}`);
  return text ? JSON.parse(text) : {};
}

function csvEscape(v) {
  if (v === null || v === undefined) return "";
  const s = String(v);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

(async () => {
  const outCsv = path.resolve("inventory/inventory.csv");
  const outVaults = path.resolve("inventory/vaults.json");

  fs.mkdirSync(path.dirname(outCsv), { recursive: true });

  const csv = fs.createWriteStream(outCsv, { encoding: "utf8" });
  csv.write([
    "vaultId","vaultName","hiddenOnUI",
    "assetId","available","total","pending","frozen","lockedAmount","staked"
  ].join(",") + "\n");

  const vaultMap = {}; // vaultId -> {name, hiddenOnUI}

  let after = undefined;
  let page = 0;
  let vaultCount = 0;
  let rowCount = 0;

  const limit = 200;

  while (true) {
    page++;
    const qs = new URLSearchParams({ limit: String(limit) });
    if (after) qs.set("after", after);

    const uri = `/v1/vault/accounts_paged?${qs.toString()}`;
    const json = await fbRequest("GET", uri);

    const accounts = json?.accounts || [];
    const pagingAfter = json?.paging?.after || null;

    vaultCount += accounts.length;

    for (const v of accounts) {
      const vid = v?.id ?? "";
      const vname = v?.name ?? "";
      const hidden = v?.hiddenOnUI === true;

      vaultMap[vid] = { name: vname, hiddenOnUI: hidden };

      const assets = Array.isArray(v?.assets) ? v.assets : [];
      if (assets.length === 0) continue;

      for (const a of assets) {
        const assetId = a?.id ?? "";
        const total = a?.total ?? a?.balance ?? "";
        const available = a?.available ?? "";
        const pending = a?.pending ?? "";
        const frozen = a?.frozen ?? "";
        const lockedAmount = a?.lockedAmount ?? "";
        const staked = a?.staked ?? "";

        csv.write([
          vid, vname, hidden ? "true" : "false",
          assetId, available, total, pending, frozen, lockedAmount, staked
        ].map(csvEscape).join(",") + "\n");
        rowCount++;
      }
    }

    if (page % 25 === 0) {
      console.error(`progress: pages=${page} vaults=${vaultCount} rows=${rowCount} next_after=${pagingAfter ? "yes" : "no"}`);
    }

    if (!pagingAfter) break;
    after = pagingAfter;
  }

  csv.end();
  fs.writeFileSync(outVaults, JSON.stringify(vaultMap, null, 2));

  console.log("✅ Refresh complete");
  console.log("- CSV:", outCsv);
  console.log("- Vault map:", outVaults);
  console.log("- Vault accounts scanned:", vaultCount);
  console.log("- Asset rows written:", rowCount);
})().catch(e => {
  console.error("ERROR:", e && e.stack ? e.stack : String(e));
  process.exit(1);
});
