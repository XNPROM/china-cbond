# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Chinese convertible bond (可转债) panorama scanner. Produces a themed, interactive HTML report covering all tradable CBs (~335), grouped by high-level theme tags. Data sourced from iFinD quant API; themes classified locally via keyword rules + Shenwan industry from data_pool.

## Daily Refresh (8-step pipeline, ~4min total)

```bash
ASOF=2026-04-22

# 1. Valuation snapshot + PE + market cap (~2min)
python3 scripts/fetch_valuation.py \
    --codes    data/raw/asof=2026-04-20/cbond_codes.txt \
    --universe data/raw/asof=2026-04-20/cbond_universe.json \
    --date     $ASOF \
    --out      data/raw/asof=$ASOF/valuation.csv

# 2. 20-day annualized volatility (~1.5min)
python3 scripts/compute_volatility.py \
    --universe data/raw/asof=2026-04-20/cbond_universe.json \
    --asof     $ASOF \
    --lookback-days 45 \
    --out data/raw/asof=$ASOF/vol_20d.csv

# 3. Assemble dataset (instant; SQL JOIN from DuckDB)
python3 scripts/assemble_dataset.py \
    --trade-date $ASOF \
    --out data/raw/asof=$ASOF/dataset.json

# 4. BS pricing + Greek letters (instant; pure math, no API calls)
#    Writes BS fields to DB AND back into dataset.json in-place.
python3 scripts/bs_pricing.py \
    --dataset    data/raw/asof=$ASOF/dataset.json \
    --trade-date $ASOF

# 5. Strategy scoring — double-low + sector-neutral + 低估 (instant)
python3 scripts/strategy_score.py \
    --dataset   data/raw/asof=$ASOF/dataset.json \
    --trade-date $ASOF \
    --out       data/raw/asof=$ASOF/strategy_picks.jsonl

# 6. Theme classification — local rules + Shenwan industry (instant)
python3 scripts/generate_themes_direct.py \
    --dataset data/raw/asof=$ASOF/dataset.json \
    --out     data/raw/asof=$ASOF/themes.jsonl \
    --trade-date $ASOF

# 7. Build Markdown (instant; reads themes + strategy from DB)
python3 scripts/build_overview_md.py \
    --dataset    data/raw/asof=$ASOF/dataset.json \
    --trade-date $ASOF \
    --out        reports/$ASOF/cbond_overview.md \
    --title-date $ASOF

# 8. Render HTML (instant; local only, no LLM)
python3 scripts/render_html.py \
    --in         reports/$ASOF/cbond_overview.md \
    --out        reports/$ASOF/cbond_overview.html \
    --title      "可转债概览 · $ASOF" \
    --trade-date $ASOF \
    --backtest   data/raw/asof=$ASOF/backtest_weekly.json
```

## First-time Setup / Rebuild DB

```bash
python3 scripts/init_db.py
python3 scripts/fetch_cb_universe.py --date 20260424
python3 scripts/backfill.py --raw data/raw/asof=2026-04-20 --trade-date 2026-04-20
```

## Full Universe Fetch (one-call, ~30s)

```bash
python3 scripts/fetch_cb_universe.py --date 20260424
```

Uses iFinD `data_pool` p05479 endpoint — returns all listed CBs with Shenwan industry in one API call.

## Backtest (weekly rebalance + T+1 entry)

```bash
python3 scripts/backtest_weekly.py --start-date 2026-01-23 --end-date 2026-04-23
```

Features: multiplicative compounding, configurable slippage/commission, PE>0 + vol>Q1 filters matching live strategy, historical universe from valuation_daily.

## Architecture

### Data Flow

```
iFinD API → raw CSV/JSON (data/raw/asof=YYYY-MM-DD/)
                      ↓
                  DuckDB (data/cbond.duckdb) ← 6 tables
                      ↓
            assemble_dataset.py (SQL JOIN) → dataset.json
                      ↓
            bs_pricing.py → DB upsert + dataset.json in-place (bs_value, relative_value, greeks)
                      ↓
         strategy_score.py → DB upsert (双低, 双低-偏股/平衡/偏债, 低估)
                      ↓
         generate_themes_direct.py → themes.jsonl + DB upsert
                      ↓
         build_overview_md.py (reads themes + strategy from DB) → .md
                      ↓
         render_html.py → .html (with equity curve chart)
```

### Active Scripts

| Script | Role |
|---|---|
| `_auth.py` | iFinD access_token lifecycle (cache 6h, refresh_token 1y) |
| `_db.py` | DuckDB connect, `init_schema()` (runs once per session), generic `upsert()` |
| `_ifind.py` | HTTP wrappers: `basic_data`, `realtime`, `history`, `ths_dr` (data_pool) |
| `fetch_cb_universe.py` | Full CB universe + Shenwan industry via data_pool p05479 |
| `fetch_valuation.py` | Daily valuation snapshot (26 indicators) |
| `fetch_underlying_profile.py` | Underlying stock company profile + industry |
| `compute_volatility.py` | 20-day annualized vol for underlying stocks |
| `assemble_dataset.py` | SQL JOIN across 4 tables → dataset.json |
| `bs_pricing.py` | BS pricing + Greek letters (r=2.5% risk-free) |
| `strategy_score.py` | Double-low + sector-neutral + low-RV scoring |
| `generate_themes_direct.py` | Keyword + Shenwan theme classification |
| `build_overview_md.py` | Markdown report from dataset + DB |
| `render_html.py` | Interactive HTML with filter/sort/equity curve |
| `backtest_weekly.py` | Weekly-rebalanced backtest engine |
| `backfill.py` | One-shot raw data loader into DuckDB |
| `init_db.py` | Idempotent schema initializer |

Archived scripts in `scripts/archive/`: `discover_universe.py`, `generate_themes_with_claude.py`, `load_themes.py`, `sample_one.py`.

### DuckDB Schema (6 tables + indexes)

| Table | PK | Grain |
|---|---|---|
| `universe` | `code` | Static bond metadata |
| `valuation_daily` | `(trade_date, code)` | Daily price, premiums, rating, balance, 强赎/下修, PE, PB, BS定价, 相对价值, 希腊字母 |
| `vol_daily` | `(trade_date, ucode)` | 20-day annualized vol per underlying stock |
| `underlying_profile` | `ucode` | Company profile text |
| `strategy_picks` | `(trade_date, code, strategy)` | Strategy scores |
| `themes` | `(trade_date, code)` | Theme tags + business rewrite + 申万行业 |

Indexes: `idx_val_code_date`, `idx_strat_date_strat`, `idx_vol_ucode_date`.

### iFinD Field Reference

`scripts/ifind_cbond_fields.md` — complete field mapping (强赎/下修/PE/PB/行业/转股 + 39 ths_* indicators + full raw field list).

`fetch_cb_universe.py` — p05479 data pool field codes (f001=面值, f009=转股价, f021=正股代码, f041-f043=申万L1-L3, etc.).

### Theme Classification

**`generate_themes_direct.py`** (current): Deterministic keyword rules + Shenwan industry from DB. Contains `THEME_RULES` (~130 rules), `THEME_OVERRIDES` (~100 per-code corrections), `THEME_TO_INDUSTRY` mapping. Prioritizes Shenwan industry data from `fetch_cb_universe.py` over keyword inference.

`theme_vocabulary.md` — ~85 whitelisted tags organized by sector. Rules: max 4 tags per bond, prefer product/technology-level tags.

### HTML Report

`render_html.py` — interactive HTML with:
- Sort/filter (price, premium, volatility, daily change)
- Export CSV / copy codes
- Sector badge (偏股/平衡/偏债) per bond
- Relative value color coding (green <1.0, red >1.2)
- BS Delta + sparkline charts
- 强赎/下修 status columns with color badges
- Backtest equity curve SVG chart
- Mobile-responsive

## Key Conventions

- **Directory layout**: `data/raw/asof=YYYY-MM-DD/` for raw snapshots, `reports/YYYY-MM-DD/` for output.
- **All fetch scripts write both flat files AND upsert to DuckDB** — flat files for inspection, DB for SQL JOINs.
- **Units**: balance in 亿元, price in 元, premium rates as percent (×100), volatility as annualized percent (×100).
- **Python**: stdlib + `duckdb` only. No pandas/numpy. Python 3.9+.
- **BS pricing** uses risk-free rate 2.5% (not YTM). Writes BS fields back into dataset.json in-place.
- **Backtest** uses multiplicative compounding with configurable slippage (10bps one-way) and commission (2bps total, split buy/sell).
- **`init_schema()`** runs once per session on first `connect()` call, not per script invocation.

## Known Pitfalls

- iFinD `ths_concept_*` fields all return ERR — no structured concept/sector data available.
- Bonds with `ths_bond_balance_cbond = 0` on as-of date are delisted (forced redemption).
- New listings with <20 trading days will have insufficient volatility samples; `compute_volatility.py` outputs `n_samples` column.
- Anaconda Python has SSL handshake failures with iFinD; use system Python.
- BS pricing skips bonds without `pure_bond_value` from iFinD — the fallback `K*exp(-rT)` ignores coupons.
