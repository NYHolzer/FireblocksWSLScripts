# Fireblocks Scripts (Node.js utilities)

A collection of Node.js scripts that support Fireblocks inventory refresh, pricing, wallet materiality, consolidation planning, and execution workflows. The scripts read and write CSV/JSON files in a few standard folders (`inventory/`, `plan/`, `execute/`, `analysis/`) so you can chain them together.



## Prerequisites

- **Node.js 18+** (uses `fetch` and modern JS)
- Fireblocks API credentials for any script that calls the Fireblocks API
  - `FIREBLOCKS_API_KEY`
  - `FIREBLOCKS_PRIVATE_KEY` (path to your private key file)
  - Optional: `FIREBLOCKS_BASE_URL` (default `https://api.fireblocks.io`)

## Directory layout

```
.
├── analysis/   # Generated analysis outputs
├── execute/    # Execution journals, completed ledgers, cached prices
├── inventory/  # Inventory CSV + vault metadata
├── plan/       # Generated move plans (CSV/JSONL)
└── tools/      # Helper utilities
```

Most scripts will create missing output folders automatically, but **inputs must exist**.

## Quick start workflow (common path)

1. **Refresh inventory** from Fireblocks.
   ```bash
   node tools/fb_refresh_inventory.js
   ```
   Outputs `inventory/inventory.csv` and `inventory/vaults.json`.

2. **Update prices** for assets in inventory.
   ```bash
   node tools/fb_update_prices.js
   ```
   Outputs `execute/last_prices_usd.json` and analysis summaries.

3. **Analyze inventory & materiality**.
   ```bash
   node tools/fb_analyze_inventory.js
   node tools/fb_wallet_materiality_v2.js
   ```

4. **Build a move plan** (example workflow).
   ```bash
   node tools/fb_build_move_plan.js
   ```

5. **Execute a plan** (dry run unless `EXECUTE=1`).
   ```bash
   node tools/fb_execute_move_plan.js
   ```

## Top-level scripts

| Script | Purpose | Key Inputs | Key Outputs |
| --- | --- | --- | --- |
| `fireblocks_analysis.js` | Consolidation/liquidation analysis with price lookups. | `inventory/inventory.csv`, `plan/plan.csv`, `execute/completed_*.txt` | `analysis/report_summary.txt`, `analysis/remaining_rows.csv`, `analysis/by_asset.csv` |
| `fireblocks_analysis_v2.js` | Updated analysis (simpler pricing flow). | Same as above | `analysis/remaining_rows_v2.csv` + summaries |
| `analysis_asset_coverage.js` | Coverage analysis for a focused list of assets. | `inventory/inventory.csv` + optional price maps | `analysis/asset_coverage.csv` |
| `analysis_breakeven_by_asset.js` | Per-asset breakeven with gas fee considerations. | `inventory/inventory.csv`, `plan/plan.csv`, `execute/gas_fee_native.json` | `analysis/breakeven_by_asset.csv` |
| `analysis_material_immaterial.js` | Material vs immaterial wallet analysis. | `inventory/inventory.csv`, `plan/plan.csv`, `execute/completed_*.txt` | `analysis/wallets_material.csv`, `analysis/wallets_immaterial.csv` |
| `generate_min_by_asset.js` | Compute minimum transfer amounts per asset. | `plan/plan.jsonl`, `execute/asset_to_coingecko.json`, `execute/min_rules.json` | `execute/min_by_asset.json` |
| `re_eval.js` | Re-evaluate wallets using live prices (CoinGecko). | `inventory/inventory.csv`, `plan/plan.csv`, `execute/asset_to_coingecko.json` | `analysis/re_eval_summary.txt` |
| `execute_ready.js` | Execute ready rows from analysis output. | `analysis/remaining_rows_v2.csv` | `execute/journal_*.jsonl`, `execute/completed_ready.txt` |

## Tools (`tools/`)

| Script | Purpose | Notes |
| --- | --- | --- |
| `fb_refresh_inventory.js` | Pull inventory + vault data from Fireblocks. | Requires Fireblocks API credentials. |
| `fb_update_prices.js` | Price assets using CoinGecko + overrides. | Writes `execute/last_prices_usd.json`. |
| `fb_analyze_inventory.js` | Inventory analysis with pricing + materiality. | Reads `policy.json` for gas policy. |
| `fb_wallet_materiality.js` | Classify wallets by USD totals. | Uses `analysis/gas_needs_wallets.csv` if present. |
| `fb_wallet_materiality_v2.js` | Simpler wallet materiality analysis. | Uses `execute/prices_usd.json` or `execute/last_prices_usd.json`. |
| `fb_build_move_plan.js` | Build a CSV move plan based on policy and prices. | Reads `execute/gas_policy.json`. |
| `fb_execute_move_plan.js` | Execute moves from `move_plan/move_plan.csv`. | Set `EXECUTE=1` to send live. |
| `fb_execute_plan_all.js` | Execute `plan/plan.jsonl` in batches. | Set `EXECUTE=1` to send live. |
| `fb_rebuild_wallets_and_plan.js` | Aggregate wallets + reconstitute plan files. | Skips vaults via `SKIP_VAULTS`. |
| `fb_receivership_report.js` | Generate receivership report. | Requires `execute/last_prices_usd.json`. |
| `build_plan_from_csv_to_vault.js` | Build a plan from a CSV to a single vault. | Usage: `node tools/build_plan_from_csv_to_vault.js <CSV> <DEST_VAULT_ID>` |

## Environment variables (common)

- `FIREBLOCKS_API_KEY`, `FIREBLOCKS_PRIVATE_KEY`, `FIREBLOCKS_BASE_URL`
- `EXECUTE=1` to enable live Fireblocks execution (default is dry-run logic in most scripts)
- `MIN_USD_PER_TX`, `STABLECOIN_MIN_USD`, `MATERIAL_WALLET_USD` to tune thresholds
- `OFFLINE=1` to skip CoinGecko calls (where supported)

## Safety notes

- Scripts that *execute* transactions default to **dry run**; set `EXECUTE=1` only when you are ready to send real transactions.
- Ensure `execute/asset_to_coingecko.json` is populated when live pricing is required.

## Example: pricing overrides

Create `execute/price_overrides_usd.json`:

```json
{
  "USDC": 1,
  "ETH": 3200
}
```

Then run:

```bash
node tools/fb_update_prices.js
```

## License

Internal/Private use (update as needed).
