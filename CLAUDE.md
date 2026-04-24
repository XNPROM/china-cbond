# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Chinese convertible bond (可转债) panorama scanner. Produces a themed, interactive HTML report covering all tradable CBs (~335), grouped by high-level theme tags. Data sourced from iFinD quant API; themes classified locally via keyword rules (with optional Claude Sonnet refinement).

## Daily Refresh (typical workflow)

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
#    Writes bs_value/relative_value/greeks to valuation_daily in DB.
python3 scripts/bs_pricing.py \
    --dataset    data/raw/asof=$ASOF/dataset.json \
    --trade-date $ASOF

# 5. Re-assemble dataset to include BS pricing results from DB
python3 scripts/assemble_dataset.py \
    --trade-date $ASOF \
    --out data/raw/asof=$ASOF/dataset.json

# 6. Strategy scoring — double-low + sector-neutral + 低估 (instant)
python3 scripts/strategy_score.py \
    --dataset   data/raw/asof=$ASOF/dataset.json \
    --trade-date $ASOF \
    --out       data/raw/asof=$ASOF/strategy_picks.jsonl

# 7. Theme classification — local rules (instant)
python3 scripts/generate_themes_direct.py \
    --dataset data/raw/asof=$ASOF/dataset.json \
    --out     data/raw/asof=$ASOF/themes.jsonl \
    --trade-date $ASOF

# 8. Build Markdown (instant; reads themes + strategy from DB)
python3 scripts/build_overview_md.py \
    --dataset    data/raw/asof=$ASOF/dataset.json \
    --trade-date $ASOF \
    --out        reports/$ASOF/cbond_overview.md \
    --title-date $ASOF

# 9. Render HTML (instant; local only, no LLM)
python3 scripts/render_html.py \
    --in         reports/$ASOF/cbond_overview.md \
    --out        reports/$ASOF/cbond_overview.html \
    --title      "可转债概览 · $ASOF" \
    --trade-date $ASOF
```

## First-time Setup / Rebuild DB

```bash
python3 scripts/init_db.py
python3 scripts/backfill.py --raw data/raw/asof=2026-04-20 --trade-date 2026-04-20
```

## Full Universe Rediscovery (slow, ~2-3min; skip for daily refresh)

```bash
python3 scripts/discover_universe.py \
    --seed /path/to/ifind_cbond_seed_codes.txt \
    --asof $ASOF \
    --probe \
    --out-json data/raw/asof=$ASOF/cbond_universe.json \
    --out-csv  data/raw/asof=$ASOF/cbond_universe.csv \
    --out-codes data/raw/asof=$ASOF/cbond_codes.txt
```

## Architecture

### Data Flow

```
iFinD API → raw CSV/JSON (data/raw/asof=YYYY-MM-DD/)
                      ↓
                  DuckDB (data/cbond.duckdb) ← 7 tables
                      ↓
            assemble_dataset.py (SQL JOIN) → dataset.json
                      ↓
            bs_pricing.py → DB upsert (bs_value, relative_value, greeks)
                      ↓
            assemble_dataset.py (2nd pass) → dataset.json (with BS fields)
                      ↓
         strategy_score.py → DB upsert (双低, 双低-偏股/平衡/偏债, 低估)
                      ↓
         generate_themes_direct.py → themes.jsonl + DB upsert
                      ↓
         build_overview_md.py (reads themes + strategy from DB) → .md
                      ↓
         render_html.py → .html
```

### DuckDB Schema (7 tables)

| Table | PK | Grain |
|---|---|---|
| `universe` | `code` | Static bond metadata (one row per bond) |
| `valuation_daily` | `(trade_date, code)` | Daily price, premium rates, rating, balance, 强赎/下修, 转股价, PB, PE_TTM, 市值(亿), 同花顺行业, 纯债YTM, 纯债价值, 到期赎回价, BS定价, 相对价值, 希腊字母 |
| `vol_daily` | `(trade_date, ucode)` | 20-day annualized vol per underlying stock |
| `underlying_profile` | `ucode` | Company profile text (no history) |
| `strategy_picks` | `(trade_date, code, strategy)` | Strategy scores (双低, 双低-偏股/平衡/偏债, 低估) |
| `themes` | `(trade_date, code)` | Theme tags + business rewrite + 申万行业 |
| `etl_runs` | `run_id` | ETL run logging |

### Shared Infrastructure (`scripts/_*.py`)

- **`_auth.py`**: iFinD access_token lifecycle (cache 6h, refresh_token 1y). Token file: `~/.codex_logs/ifind_refresh_token.txt`
- **`_ifind.py`**: Thin HTTP wrappers for 3 iFinD endpoints: `basic_data_service`, `real_time_quotation`, `cmd_history_quotation`. Includes `batched()` helper.
- **`_db.py`**: DuckDB connect (`data/cbond.duckdb`), `init_schema()` runs `schema.sql`, generic `upsert()` with ON CONFLICT DO UPDATE.
- **`ifind_cbond_fields.md`**: Complete iFinD field name reference for CB data (强赎/下修/PE/PB/etc).

### Theme Classification

Two paths exist:
1. **`generate_themes_direct.py`** (current default): Deterministic keyword rules from profile text. Contains `THEME_RULES` (keyword→theme mapping), `THEME_OVERRIDES` (hand-corrected per-code), and `THEME_TO_INDUSTRY` (theme→申万行业). Also generates `business_rewrite` via regex extraction from profile.
2. **`generate_themes_with_claude.py`**: Batch-calls Claude Sonnet for higher quality. Outputs `themes.jsonl` → loaded via `load_themes.py`.

### Theme Vocabulary

`theme_vocabulary.md` defines ~85 whitelisted tags organized by sector (TMT, 新能源, 汽车, 军工, 医药, etc.). Rules: max 4 tags per bond, must use whitelisted tags, prefer product/technology-level tags over downstream application tags.

### HTML Rendering

`render_html.py` is a large (~1400-line) self-contained script that converts Markdown to a polished interactive HTML report with:
- Sidebar navigation with IntersectionObserver
- Sort/filter controls (by price, premium, volatility, daily change)
- Compact/reading view toggle
- Export visible bonds as CSV / copy codes
- Mobile-responsive 2×2 metric grid
- Sector badge (偏股/平衡/偏债) per bond based on conv_prem
- Relative value color coding: green (<1.0), red (>1.2)
- BS Delta column showing equity sensitivity

## Key Conventions

- **Directory layout**: `data/raw/asof=YYYY-MM-DD/` for raw snapshots, `reports/YYYY-MM-DD/` for output. `data/cbond.duckdb` is the query layer.
- **All fetch scripts write both flat files (CSV/JSON) AND upsert to DuckDB** — flat files for human inspection, DB for SQL JOINs.
- **Units**: balance in 亿元, price in 元, premium rates as percent (×100), volatility as annualized percent (×100).
- **Python**: stdlib + `duckdb` only. No pandas/numpy. Python 3.9+.
- **iFinD access_token** TTL ~8h; `_auth.py` caches 6h. Don't manually edit the cache file.
- **`universe` table has no history** — upsert overwrites static fields. Time-varying fields live in `valuation_daily`.

## Known Pitfalls

- iFinD `ths_concept_*` fields all return ERR — no structured concept/sector data available. Theme classification uses profile text instead.
- No single API to list all tradable CBs — relies on seed codes + range probing (`discover_universe.py --probe`). Newly issued bonds may be missed.
- Bonds with `ths_bond_balance_cbond = 0` on as-of date are delisted (forced redemption).
- New listings with <20 trading days will have insufficient volatility samples; `compute_volatility.py` outputs `n_samples` column.
- Anaconda Python has SSL handshake failures with iFinD; use system `/usr/bin/python3`.
- **assemble_dataset.py must run twice**: once before bs_pricing (to create dataset.json with raw data), once after (to include BS pricing results written to DB). Skipping the 2nd run means dataset.json will have stale/missing BS values.
