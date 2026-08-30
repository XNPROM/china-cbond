# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Project Overview

Chinese convertible bond (可转债) panorama scanner. Produces a themed, interactive HTML report covering the currently tradable public CB universe (usually 300+; the exact count is determined by the as-of `p05479` snapshot), grouped by high-level theme tags. Data sourced from iFinD quant API; themes classified locally via keyword rules + Shenwan industry from data_pool.

## Daily Refresh (canonical pipeline and acceptance)

The project-specific `cbond-monitor` skill is the operating framework for every local refresh. `AGENTS.md` records the repository-level rules; the skill supplies the preflight, data-completeness, failure-gate, and publication acceptance criteria. Do not treat a generated Markdown report as the execution engine.

The normal local refresh is one command:

```bash
cd /Users/apple/cbond_monitor
ASOF=YYYY-MM-DD
caffeinate -i /usr/local/bin/python3.12 scripts/daily_refresh.py \
    --trade-date "$ASOF"
```

The actual stages are: check for a complete local universe snapshot for the target date (reuse it when present; otherwise fetch the as-of universe from iFinD `p05479`) → incrementally reuse same-date valuation rows and fetch only missing/blank bond data, while auditing close prices for the exact code set → refresh stale bond-side fields → refresh underlying profiles when due → incrementally reuse same-date volatility and fetch only missing underlyings → assemble the dated dataset → BS pricing → strategy scoring → theme/business cache and classification → Markdown report → optional weekly backtest → strict snapshot validation → local HTML/index rendering. Only after all required gates pass does `auto_daily.sh` commit and push the report; GitHub Actions packages and deploys the pre-rendered HTML.

The default refresh reuses the exact-date local snapshot when both `cbond_codes.txt` and `cbond_universe.json` are present, valid, and code-consistent. `--refresh-universe` explicitly forces a new iFinD list fetch. For an intentional offline/local rebuild without an exact-date snapshot, `--skip-fetch` reuses the newest complete snapshot on or before the target date and records its date in the log.

`scripts/_network.py` selects the iFinD route before each request: when Clash Verge TUN is enabled it forces direct requests so TUN handles the traffic; otherwise it respects the configured HTTP(S)/ALL_PROXY environment. Use `IFIND_NETWORK_MODE=direct|proxy` for an explicit override, or `IFIND_TUN_CONFIG` to point at a non-default Clash configuration.

### Refresh acceptance checklist

For each run, the final `data/raw/asof=YYYY-MM-DD/cbond_codes.txt` is the expected code set. The run is accepted only when:

- `quote_audit.json` shows the expected count, returned non-blank close count, missing code count of zero, blank code count of zero, and no batch errors;
- `validate_snapshot.py --strict --codes ...` passes, including the dated DuckDB price coverage check;
- the dated Markdown, HTML, `index.html`, and (when enabled) backtest JSON exist;
- no failed required ETL step is present in `etl_runs`; and
- the report is committed and pushed only after the checks above succeed.

The audit artifact is `data/raw/asof=YYYY-MM-DD/quote_audit.json`. It is an operational trace and remains local unless explicitly added to a release artifact.

## First-time Setup / Rebuild DB

```bash
python3.12 scripts/init_db.py
python3.12 scripts/fetch_cb_universe.py --date 2026-04-24
python3.12 scripts/backfill.py \
    --raw data/raw/asof=2026-04-20 \
    --trade-date 2026-04-20
```

## Full Universe Fetch (manual, one-call, ~30s)

```bash
python3.12 scripts/fetch_cb_universe.py --date 2026-04-24
```

When no complete exact-date snapshot exists, the daily refresh calls this data_pool endpoint before valuation, and the fetched count/code set is the expected quote universe. If the exact-date snapshot already exists, it is reused by default. Use `--refresh-universe` to force the data_pool call, or `--skip-fetch` for an intentional local rebuild using the newest available snapshot.

Uses iFinD `data_pool` p05479 endpoint. The post-filter `cbond_codes.txt` is the exact expected list for the subsequent quote audit; do not infer the expected count from DuckDB or an older snapshot.

## Backtest (daily/weekly rebalance + T+1 entry)

```bash
# Via iFinD API (fetches prices + fundamentals, persists to DB)
python3.12 scripts/backtest_weekly.py --start-date 2026-01-23 --end-date 2026-04-23

# Via DB only (fast, requires pre-populated valuation_daily price/premium data)
python3.12 scripts/backtest_weekly.py --start-date 2026-01-23 --end-date 2026-04-23 --from-db
```

Features: multiplicative compounding, configurable slippage/commission, historical universe from valuation_daily, exclusion of ST/*ST underlying stocks, balance >= 2亿元, no hard price cap, and volatility >= Q1. Fundamentals are fetched only on rebalance dates (not every trading day) and are persisted to DuckDB for future `--from-db` runs.

**Important**: PE is retained as an informational field but is not a filter in the weekly backtest. `--from-db` requires historical price, conversion-premium, balance, and volatility data for the rebalance dates; missing data can make a strategy period N/A. The engine currently supports `--rebalance daily` and `--rebalance weekly`; monthly rebalancing is not yet implemented.

## Data Freshness Check

iFinD bond-side fields (conv_prem_pct, pure_bond_value, maturity_call_price) can return NULL when data hasn't been processed yet (e.g., fetched too early after market close). Use `refresh_data.py` to detect and fix:

```bash
# Check only:
python3 scripts/refresh_data.py --trade-date 2026-04-24

# Re-fetch stale fields:
python3 scripts/refresh_data.py --trade-date 2026-04-24 --fix

# Force re-fetch all bond-side fields:
python3 scripts/refresh_data.py --trade-date 2026-04-24 --fix --force
```

After refreshing, rerun the local pipeline from dataset assembly, then run strict snapshot validation before rendering HTML.

## Architecture

### Data Flow

```
iFinD API → raw CSV/JSON (data/raw/asof=YYYY-MM-DD/)
                      ↓
                  DuckDB (data/cbond.duckdb) ← 8 tables
                      ↓
            fetch_valuation.py → quote_audit.json + valuation.csv
                      ↓
            assemble_dataset.py (SQL JOIN) → dated dataset.json
                      ↓
            bs_pricing.py → DB upsert + dataset.json in-place (bs_value, relative_value, greeks)
                      ↓
         strategy_score.py → DB upsert (双低, 双低-偏股/平衡/偏债, 低估)
                      ↓
         generate_themes_direct.py → themes.jsonl + DB upsert
                      ↓
         build_overview_md.py (reads themes + strategy from DB) → .md
                      ↓
         validate_snapshot.py → strict publication gate
                      ↓
         render_html.py → .html/index.html (with equity curve chart)
```

### Active Scripts

| Script | Role |
|---|---|
| `_auth.py` | iFinD access_token lifecycle (cache 6h, refresh_token 1y) |
| `_db.py` | DuckDB connect, `init_schema()` (runs once per session), generic `upsert()` |
| `_network.py` | Detect Clash TUN and select direct/TUN or environment-proxy routing |
| `_ifind.py` | HTTP wrappers: `basic_data`, `history`, `realtime`, `ths_dr` (data_pool) |
| `fetch_cb_universe.py` | Full CB universe + Shenwan industry via data_pool p05479 |
| `fetch_valuation.py` | Daily valuation snapshot plus exact close-quote audit (`quote_audit.json`) |
| `fetch_underlying_profile.py` | Underlying stock company profile + industry |
| `compute_volatility.py` | 20-day annualized vol for underlying stocks |
| `assemble_dataset.py` | SQL JOIN across the dated DuckDB snapshot → dataset.json |
| `bs_pricing.py` | BS pricing + Greek letters (r=2.5% risk-free) |
| `strategy_score.py` | Double-low + sector-neutral + low-RV scoring |
| `generate_themes_direct.py` | Keyword + Shenwan theme classification |
| `build_overview_md.py` | Markdown report from dataset + DB |
| `render_html.py` | Interactive HTML dashboard (Jinja2 + ECharts) |
| `render_markdown_parser.py` | Markdown parser + helpers (extracted from render_html) |
| `backtest_weekly.py` | Daily/weekly-rebalanced backtest engine (monthly not implemented) |
| `backfill.py` | One-shot raw data loader into DuckDB |
| `init_db.py` | Idempotent schema initializer |
| `refresh_data.py` | Data freshness check + iFinD re-fetch for stale fields |
| `_etl_log.py` | ETL run logging context manager (writes to `etl_runs` table) |
| `analyze_business_llm.py` | Structured主营业务 extraction into `underlying_business` with profile-hash incrementality |
| `validate_data.py` | Legacy dataset validator; use `validate_snapshot.py` for the current dated snapshot gate |
| `report_view_model.py` | Dashboard payload builder (normalizes parsed markdown → JSON view model for HTML) |
| `daily_refresh.py` | One-command end-to-end refresh orchestrator (串行 ETL 入口, writes `etl_runs`) |
| `validate_snapshot.py` | Snapshot quality check and exact code-vs-price coverage gate |

Archived scripts in `scripts/archive/`: `discover_universe.py`, `generate_themes_with_claude.py`, `load_themes.py`, `sample_one.py`.

### DuckDB Schema (8 tables + indexes)

| Table | PK | Grain |
|---|---|---|
| `universe` | `code` | Static bond metadata |
| `valuation_daily` | `(trade_date, code)` | Daily price, premiums, rating, balance, 强赎/下修, PE, PB, BS定价, 相对价值, 希腊字母 |
| `vol_daily` | `(trade_date, ucode)` | 20-day annualized vol per underlying stock |
| `underlying_profile` | `ucode` | Company profile text |
| `underlying_business` | `ucode` | Structured主营业务、产品、应用、客户与证据缓存 |
| `strategy_picks` | `(trade_date, code, strategy)` | Strategy scores |
| `themes` | `(trade_date, code)` | Theme tags + business rewrite + 申万行业 |
| `etl_runs` | `run_id` | ETL step execution log (status, timing, row counts) |

Indexes: `idx_val_code_date`, `idx_strat_date_strat`, `idx_vol_ucode_date`, `idx_etl_date`.

The dated raw snapshot uses `asof=YYYY-MM-DD`. The weekly backtest output keeps the historical filename convention `data/raw/asof=YYYYMMDD/backtest_weekly.json`; this difference is intentional and should be preserved in manual commands.

### iFinD Field Reference

`scripts/ifind_cbond_fields.md` — complete field mapping (强赎/下修/PE/PB/行业/转股 + 39 ths_* indicators + full raw field list).

`fetch_cb_universe.py` — p05479 data pool field codes (f001=面值, f009=转股价, f021=正股代码, f041-f043=申万L1-L3, etc.).

### Theme Classification

**`generate_themes_direct.py`** (current): Deterministic keyword rules + Shenwan industry from DB. Contains `THEME_RULES` (~130 rules), `THEME_OVERRIDES` (~100 per-code corrections), `THEME_TO_INDUSTRY` mapping. Prioritizes Shenwan industry data from `fetch_cb_universe.py` over keyword inference.

`theme_vocabulary.md` — ~85 whitelisted tags organized by sector. Rules: max 4 tags per bond, prefer product/technology-level tags.

### HTML Report

`render_html.py` — modern dark-themed interactive dashboard:
- Jinja2 templates (`scripts/templates/base.html.j2`) + inlined CSS/JS
- Dark/light theme toggle (persisted to localStorage)
- KPI summary cards (total, avg price, median conv premium, median RV, undervalued count, sector split)
- Column-level sorting (click header: asc → desc → default)
- ECharts equity curve (tooltip + dataZoom)
- SVG sparklines for delta and relative value trends
- Filter: text search, theme dropdown, quick-filter buttons
- Export CSV / copy codes
- Sector badges, relative value color coding, call/down status badges
- Mobile-responsive (card layout <640px)

Architecture: `render_html.py` → Jinja2 → single self-contained HTML. CSS in `scripts/static/style.css`, JS in `scripts/static/app.js`, both inlined at render time. Bond data in `window.__CBOND_DATA__` JSON (replaces data-* attributes).

## Key Conventions

- **Directory layout**: `data/raw/asof=YYYY-MM-DD/` for raw snapshots, `reports/YYYY-MM-DD/` for output.
- **All fetch scripts write both flat files AND upsert to DuckDB** — flat files for inspection, DB for SQL JOINs.
- **Expected universe**: the post-filter `cbond_codes.txt` from the current `p05479` snapshot is authoritative for that run; compare it exactly with the codes that returned a non-blank close.
- **Quote audit**: `fetch_valuation.py` writes `quote_audit.json`; one missing expected close, blank close, or failed history batch is a hard failure.
- **Underlying profile cache**: `daily_refresh.py` checks `underlying_profile` on every run. Rows are refreshed from iFinD every 30 days; missing/new underlyings refresh immediately; otherwise the latest successful cached value is reused. Failed refreshes never blank an existing value. Use `--skip-profile` only for an intentional offline rebuild.
- **Units**: balance in 亿元, price in 元, premium rates as percent (×100), volatility as annualized percent (×100).
- **Python**: Python 3.9+ with `duckdb`, `jinja2`, `numpy`, `requests`; `pytest` is the test runner. Install: `pip install -r requirements.txt`.
- **Acceptance**: run `python3.12 -m pytest -q` for code changes; for a refresh, require quote audit + strict snapshot validation before rendering or publishing.
- **Date paths**: ordinary raw snapshots use `asof=YYYY-MM-DD`; the legacy weekly backtest artifact uses `asof=YYYYMMDD/backtest_weekly.json`.
- **BS pricing** uses risk-free rate 2.5% (not YTM). Writes BS fields back into dataset.json in-place.
- **Backtest** uses multiplicative compounding with configurable slippage (10bps one-way) and commission (2bps total, split buy/sell).
- **`init_schema()`** runs once per session on first `connect()` call, not per script invocation.

## Known Pitfalls

- iFinD `ths_concept_*` fields all return ERR — no structured concept/sector data available.
- Bonds with `ths_bond_balance_cbond = 0` on as-of date are delisted (forced redemption).
- New listings with <20 trading days will have insufficient volatility samples; `compute_volatility.py` outputs `n_samples` column.
- A missing close for even one code in the expected list is not hidden by an aggregate coverage threshold; inspect `quote_audit.json` and the validation output.
- Anaconda Python has SSL handshake failures with iFinD; use system Python.
- BS pricing skips bonds without `pure_bond_value` from iFinD — the fallback `K*exp(-rT)` ignores coupons.
