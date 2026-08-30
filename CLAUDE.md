# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Chinese convertible bond (可转债) panorama scanner. Produces a themed, interactive HTML report covering the currently tradable public CB universe (usually 300+; the exact count is determined by the dated `p05479` snapshot), grouped by high-level theme tags. Data sourced from iFinD quant API; themes classified locally via keyword rules + Shenwan industry from data_pool.

## Daily Refresh (canonical pipeline and acceptance)

For every local refresh, follow the project-specific `cbond-monitor` skill and the detailed rules in `AGENTS.md`. This file is a Claude entry point; do not maintain a second, conflicting copy of the workflow. The Markdown report is an output artifact, not the execution engine.

```bash
cd /Users/apple/cbond_monitor
ASOF=YYYY-MM-DD
caffeinate -i /usr/local/bin/python3.12 scripts/daily_refresh.py \
    --trade-date "$ASOF"
```

Pipeline order: check for a complete exact-date local universe snapshot and reuse it when present (otherwise fetch the as-of universe from iFinD `p05479`) → fetch and audit close prices for exactly that code set → refresh stale bond-side fields and due underlying profiles → compute volatility → assemble the dated dataset → BS pricing → strategy scoring → theme/business cache and classification → Markdown report → optional backtest → strict snapshot validation → local HTML/index rendering. Only after the gates pass does `auto_daily.sh` commit and push; GitHub Actions packages and deploys the pre-rendered HTML and does not reconstruct the backtest from Markdown.

The default reuses the exact-date local snapshot when its two universe files are valid and consistent. `--refresh-universe` explicitly forces a new universe fetch. `--skip-fetch` is for an intentional offline/local rebuild and reuses the newest complete snapshot on or before the target date. `scripts/_network.py` selects direct/TUN versus environment proxy per iFinD request. The acceptance artifacts are `data/raw/asof=YYYY-MM-DD/quote_audit.json`, the strict `validate_snapshot.py` result, dated `cbond_overview.md`, `cbond_overview.html`, `index.html`, and (when enabled) `backtest_weekly.json`.

## First-time Setup / Rebuild DB

```bash
python3.12 scripts/init_db.py
python3.12 scripts/fetch_cb_universe.py --date 2026-04-24
python3.12 scripts/backfill.py \
    --raw data/raw/asof=2026-04-20 \
    --trade-date 2026-04-20
```

## Full Universe Fetch (one-call, ~30s)

```bash
python3.12 scripts/fetch_cb_universe.py --date 2026-04-24
```

Uses iFinD `data_pool` p05479 endpoint. The post-filter `cbond_codes.txt` is the exact expected list for the subsequent quote audit; do not infer the expected count from DuckDB or an older snapshot.

## Backtest (daily/weekly rebalance + T+1 entry)

```bash
python3.12 scripts/backtest_weekly.py --start-date 2026-01-23 --end-date 2026-04-23
```

Features: multiplicative compounding, configurable slippage/commission, historical universe from valuation_daily, exclusion of ST/*ST underlying stocks, balance >= 2亿元, no hard price cap, and volatility >= Q1. PE remains an informational field but is not a weekly-backtest filter. The engine currently supports daily and weekly rebalancing; monthly rebalancing is not yet implemented.

## One-Command Refresh

```bash
python3.12 scripts/daily_refresh.py --trade-date 2026-04-24
```

Runs the full pipeline in sequence: fetch_cb_universe → refresh_underlying_profile → fetch_valuation/quote_audit → refresh_data → compute_volatility → assemble_dataset → bs_pricing → strategy_score → ensure_themes → generate_themes_direct → build_overview_md → backtest_weekly → validate_snapshot → render_html/index. Each step writes to `etl_runs` where applicable. Supports `--skip-fetch`, `--skip-profile`, `--skip-valuation`, `--skip-vol`, and `--skip-backtest`; skipped API steps are for controlled rebuilds only.

## 主营业务 LLM 缓存 (underlying_business)

`themes.business_rewrite` 由 LLM (claude-sonnet-4-6) 一次性抽取，缓存到 `underlying_business` 表（PK=ucode），日频流水线只读缓存。

原始主营业务保存在 `underlying_profile`。日频流水线每次检查、每 30 天刷新一次；新股、缺失行和空文本立即补充，其他时间复用最近一次成功结果。抓取失败时必须保留旧值。

### 何时跑
- **日频**：不用动。`generate_themes_direct.py` 自动读 `underlying_business`，无 LLM 调用。
- **结构化增量刷新（原文月度刷新后）**：按 `profile_hash` 自动跳过未变行。
  ```bash
  caffeinate -i python3 scripts/analyze_business_llm.py
  ```
- **新上市券**：universe 新增 ucode 时先补 profile，再跑 LLM 增量。
  ```bash
  # 1. 补 profile（替换 NEW_UCODE 为新正股代码）
  python3 -c "
  import sys; sys.path.insert(0,'scripts')
  from _ifind import basic_data
  from _db import connect, upsert as db_upsert
  from datetime import datetime
  UCODES = ['NEW_UCODE.SH']
  FIELDS = [{'indicator':'ths_corp_profile','indiparams':['']},
            {'indicator':'ths_industry','indiparams':['']},
            {'indicator':'ths_stock_short_name_stock','indiparams':['']}]
  r = basic_data(UCODES, FIELDS)
  rows = []
  for t in r.get('tables', []):
      tbl = t.get('table', {})
      rows.append({'ucode':t['thscode'],
                   'uname':(tbl.get('ths_stock_short_name_stock') or [''])[0],
                   'industry':(tbl.get('ths_industry') or [''])[0],
                   'main_business':(tbl.get('ths_corp_profile') or [''])[0],
                   'updated_at':datetime.now().isoformat()})
  db_upsert(connect(), 'underlying_profile', rows, ['ucode'])
  "
  # 2. 跑 LLM 增量（自动只处理新增/变更的）
  caffeinate -i python3 scripts/analyze_business_llm.py
  ```
- **强制全量重刷**：模型升级或 prompt 改动后用。
  ```bash
  caffeinate -i python3 scripts/analyze_business_llm.py --force
  ```

### 失败重试
本轮失败的 ucode 会写入 `data/logs/business_llm_failed_<ts>.jsonl`，下轮：
```bash
python3 scripts/analyze_business_llm.py --retry-failed data/logs/business_llm_failed_<ts>.jsonl --force
```

### 关键约定
- 必须用 `caffeinate -i` 防 Mac 待机，否则长时间挂起会造成大量 `claude exit=1`。
- 走 Claude Code 订阅额度（无 ANTHROPIC_API_KEY）；模型固定 `claude-sonnet-4-6`，~7s/条、~$0.006/条。
- 自动跳过未变更：以 profile 前 16 字符 SHA-256 作为 `profile_hash`，简介无变化则不重跑。

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
| `analyze_business_llm.py` | LLM 抽取主营业务 → `underlying_business`，profile_hash 增量 |
| `validate_data.py` | Legacy dataset validator; use `validate_snapshot.py` for the current dated snapshot gate |
| `report_view_model.py` | Dashboard payload builder (normalizes parsed markdown → JSON view model for HTML) |
| `daily_refresh.py` | One-command end-to-end refresh orchestrator (串行 ETL 入口, writes `etl_runs`) |
| `validate_snapshot.py` | Snapshot quality check plus exact expected-code vs dated price coverage gate |

Archived scripts in `scripts/archive/`: `discover_universe.py`, `generate_themes_with_claude.py`, `load_themes.py`, `sample_one.py`, `build_strategy_page.py`.

### DuckDB Schema (8 tables + indexes)

| Table | PK | Grain |
|---|---|---|
| `universe` | `code` | Static bond metadata |
| `valuation_daily` | `(trade_date, code)` | Daily price, premiums, rating, balance, 强赎/下修, PE, PB, BS定价, 相对价值, 希腊字母 |
| `vol_daily` | `(trade_date, ucode)` | 20-day annualized vol per underlying stock |
| `underlying_profile` | `ucode` | Company profile text (iFinD `ths_corp_profile` 原文) |
| `underlying_business` | `ucode` | LLM 抽取的结构化主营业务（main_business / products / applications / customers / position_evidence + profile_hash） |
| `strategy_picks` | `(trade_date, code, strategy)` | Strategy scores |
| `themes` | `(trade_date, code)` | Theme tags + business rewrite + 申万行业 |
| `etl_runs` | `run_id` | ETL step execution log (status, timing, row counts) |

Indexes: `idx_val_code_date`, `idx_strat_date_strat`, `idx_vol_ucode_date`, `idx_etl_date`.

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

## Testing

Tests use pytest and are all under `tests/`. The exact count can change as coverage grows; the latest baseline should be recorded from `pytest -q` rather than hard-coded in this document.

```bash
# Run all tests
python3.12 -m pytest -q

# Run a single test file
python3.12 -m pytest -q tests/test_bs_pricing.py

# Run a single test case
python3.12 -m pytest -q tests/test_bs_pricing.py -k typical_cbond_pricing
```

Test files: `test_bs_pricing.py` (BS model + Greeks), `test_strategy_score.py` (double-low scoring + sector classification), `test_render_markdown_parser.py`, `test_report_view_model.py`. All tests import from `scripts/` via `sys.path.insert`.

## Key Conventions

- **Directory layout**: `data/raw/asof=YYYY-MM-DD/` for raw snapshots, `reports/YYYY-MM-DD/` for output.
- **All fetch scripts write both flat files AND upsert to DuckDB** — flat files for inspection, DB for SQL JOINs.
- **Units**: balance in 亿元, price in 元, premium rates as percent (×100), volatility as annualized percent (×100).
- **Python**: Python 3.9+ with `duckdb`, `jinja2`, `numpy`, `requests`; `pytest` is the test runner. Install: `pip install -r requirements.txt`.
- **Snapshot paths**: normal raw snapshots use `data/raw/asof=YYYY-MM-DD/`; the weekly backtest artifact retains `data/raw/asof=YYYYMMDD/backtest_weekly.json`.
- **Publication gate**: `quote_audit.json` and `validate_snapshot.py --strict --codes ...` must pass before HTML is rendered or pushed.
- **BS pricing** uses risk-free rate 2.5% (not YTM). Writes BS fields back into dataset.json in-place.
- **Backtest** uses multiplicative compounding with configurable slippage (10bps one-way) and commission (2bps total, split buy/sell).
- **`init_schema()`** runs once per session on first `connect()` call, not per script invocation.

## Known Pitfalls

- iFinD `ths_concept_*` fields all return ERR — no structured concept/sector data available.
- Bonds with `ths_bond_balance_cbond = 0` on as-of date are delisted (forced redemption).
- New listings with <20 trading days will have insufficient volatility samples; `compute_volatility.py` outputs `n_samples` column.
- A missing close for even one code in the fetched expected list is a hard failure; inspect `quote_audit.json` rather than relying on an aggregate coverage percentage.
- Anaconda Python has SSL handshake failures with iFinD; use system Python.
- BS pricing skips bonds without `pure_bond_value` from iFinD — the fallback `K*exp(-rT)` ignores coupons.
