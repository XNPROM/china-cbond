"""Weekly-rebalance backtest for convertible bond strategies.

Usage:
  python scripts/backtest_weekly.py --end-date 2026-04-23 --days 5
  python scripts/backtest_weekly.py --start-date 2025-04-23 --end-date 2026-04-23
  python scripts/backtest_weekly.py --start-date 2025-04-23 --end-date 2026-04-23 --from-db
"""
import argparse
import json
import math
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta

import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from _db import connect, upsert as db_upsert

try:
    from _ifind import basic_data, history, batched
except ImportError:
    basic_data = history = batched = None


SLIPPAGE_BPS = 10       # one-way slippage in basis points (0.1%)
COMMISSION_BPS = 2       # total commission in basis points, split half buy/half sell


def _yyyymmdd(dt):
    return dt.strftime("%Y%m%d")


def _ymd_to_dash(ymd):
    """'20260423' -> '2026-04-23'"""
    return f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:]}"


def _safe_float(arr, idx):
    if idx >= len(arr):
        return None
    v = arr[idx]
    if v is None or v == "-" or v == "":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _interp_log(start, end, frac):
    """Geometric interpolation in log-space for multiplicative equity."""
    if start <= 0 or end <= 0:
        return 0.0
    return start * math.exp(frac * math.log(end / start))


def fetch_trading_dates(codes, start_ymd, end_ymd):
    """Get list of trading dates from iFinD history."""
    r = history(codes[0], "close", _ymd_to_dash(start_ymd), _ymd_to_dash(end_ymd))
    dates = []
    for t in r.get("tables", []):
        for d in t.get("time", []):
            if d and d != "-":
                dates.append(d.replace("-", ""))
    return sorted(set(dates))


def fetch_history_prices(codes, start_ymd, end_ymd):
    """Fetch daily close prices for all CBs via iFinD history (batch, fast)."""
    prices = defaultdict(dict)  # {code: {date_ymd: price}}
    for batch_codes in batched(codes, 30):
        try:
            r = history(
                batch_codes, "close",
                _ymd_to_dash(start_ymd), _ymd_to_dash(end_ymd),
            )
            for t in r.get("tables", []):
                code = t.get("thscode", "")
                tbl = t.get("table", {})
                dates = t.get("time", [])
                closes = tbl.get("close", [])
                for i, d in enumerate(dates):
                    if d and d != "-":
                        ymd = d.replace("-", "")
                        v = _safe_float(closes, i)
                        if v and v > 0:
                            prices[code][ymd] = v
            time.sleep(0.12)
        except Exception as e:
            print(f"[warn] history batch: {e}")
    return prices


def fetch_day_fundamentals(codes, date_ymd):
    """Fetch conv_prem, balance, and 20d vol for a specific date via iFinD."""
    date_param = _ymd_to_dash(date_ymd)
    fields = [
        {"indicator": "ths_conversion_premium_rate_cbond", "indiparams": [date_param]},
        {"indicator": "ths_bond_balance_cbond", "indiparams": [date_param]},
        {"indicator": "ths_vol_20d_cbond", "indiparams": [date_param]},
    ]
    result = {}
    for batch_codes in batched(codes, 40):
        try:
            r = basic_data(batch_codes, fields)
            for t in r.get("tables", []):
                code = t.get("thscode", "")
                tbl = t.get("table", {})
                conv_prem = _safe_float(tbl.get("ths_conversion_premium_rate_cbond", []), 0)
                balance = _safe_float(tbl.get("ths_bond_balance_cbond", []), 0)
                vol_20d = _safe_float(tbl.get("ths_vol_20d_cbond", []), 0)
                if conv_prem is not None:
                    result[code] = {
                        "conv_prem": conv_prem,
                        "balance": balance,
                        "pe_ttm": None,
                        "vol_20d": vol_20d,
                    }
            time.sleep(0.12)
        except Exception as e:
            print(f"[warn] basic_data batch for {date_ymd}: {e}")
    return result


def fetch_underlying_pe_bulk(code_to_ucode, start_ymd, end_ymd):
    """Fetch PE_TTM for all underlying stocks via history(), return {ucode: {ymd: pe}}."""
    ucodes = list(set(code_to_ucode.values()))
    print(f"[fetch] pulling underlying stock PE for {len(ucodes)} stocks...")
    pe_map = defaultdict(dict)  # {ucode: {ymd: pe}}
    for batch_ucodes in batched(ucodes, 30):
        try:
            r = history(
                batch_ucodes, "pe_ttm",
                _ymd_to_dash(start_ymd), _ymd_to_dash(end_ymd),
            )
            for t in r.get("tables", []):
                ucode = t.get("thscode", "")
                tbl = t.get("table", {})
                dates = t.get("time", [])
                pe_vals = tbl.get("pe_ttm", [])
                for i, d in enumerate(dates):
                    if d and d != "-":
                        ymd = d.replace("-", "")
                        v = _safe_float(pe_vals, i)
                        if v is not None and v > 0:
                            pe_map[ucode][ymd] = v
            time.sleep(0.12)
        except Exception as e:
            print(f"[warn] history pe_ttm batch: {e}")
    total_pts = sum(len(v) for v in pe_map.values())
    print(f"[PE] {total_pts} PE data points for {len(pe_map)} underlying stocks")
    return pe_map


def merge_pe_into_fundamentals(fundamentals, code_to_ucode, pe_map):
    """Merge underlying stock PE into fundamentals dict (in-place)."""
    n_merged = 0
    for ymd, fund_map in fundamentals.items():
        for code, f in fund_map.items():
            ucode = code_to_ucode.get(code)
            if ucode and ucode in pe_map:
                pe = pe_map[ucode].get(ymd)
                if pe is not None:
                    f["pe_ttm"] = pe
                    n_merged += 1
    print(f"[PE] merged {n_merged} PE values into fundamentals")
    return n_merged


def compute_vol_from_prices(prices, trading_dates, rebalance_ymds, window=20):
    """Compute annualized 20-day realized volatility from close prices.

    Returns {ymd: {code: vol_pct}} for each rebalance date.
    """
    # Build sorted date index for lookup
    date_idx = {d: i for i, d in enumerate(trading_dates)}
    vol_map = {}
    for ymd in rebalance_ymds:
        idx = date_idx.get(ymd)
        if idx is None or idx < window:
            vol_map[ymd] = {}
            continue
        # Window of dates for computing vol
        window_dates = trading_dates[idx - window:idx + 1]
        code_vols = {}
        for code, px_dict in prices.items():
            # Get prices in window
            px_series = []
            for d in window_dates:
                p = px_dict.get(d)
                if p and p > 0:
                    px_series.append(p)
            if len(px_series) < window // 2:
                continue
            # Log returns
            log_rets = []
            for i in range(1, len(px_series)):
                log_rets.append(math.log(px_series[i] / px_series[i - 1]))
            if len(log_rets) >= 5:
                arr = np.array(log_rets)
                vol_ann = float(np.std(arr, ddof=1) * math.sqrt(252) * 100)
                code_vols[code] = vol_ann
        vol_map[ymd] = code_vols
    return vol_map


def merge_vol_into_fundamentals(fundamentals, vol_map):
    """Merge computed vol into fundamentals dict (in-place) where vol_20d is missing."""
    n_merged = 0
    for ymd, fund_map in fundamentals.items():
        vols = vol_map.get(ymd, {})
        for code, f in fund_map.items():
            if (f.get("vol_20d") is None or f["vol_20d"] == 0) and code in vols:
                f["vol_20d"] = vols[code]
                n_merged += 1
    print(f"[vol] merged {n_merged} computed vol values into fundamentals")
    return n_merged


def fetch_trading_dates_from_db(start_ymd, end_ymd):
    """Get list of trading dates from DuckDB valuation_daily."""
    con = connect()
    rows = con.execute(
        "SELECT DISTINCT trade_date FROM valuation_daily "
        "WHERE trade_date >= ? AND trade_date <= ? ORDER BY trade_date",
        [_ymd_to_dash(start_ymd), _ymd_to_dash(end_ymd)]
    ).fetchall()
    con.close()
    return [r[0].replace("-", "") for r in rows]


def fetch_prices_from_db(trading_dates):
    """Fetch daily close prices from DuckDB valuation_daily."""
    con = connect()
    dates_fmt = [_ymd_to_dash(d) for d in trading_dates]
    placeholders = ",".join(["?"] * len(dates_fmt))
    rows = con.execute(
        f"SELECT code, trade_date, price FROM valuation_daily "
        f"WHERE trade_date IN ({placeholders}) AND price IS NOT NULL AND price > 0",
        dates_fmt
    ).fetchall()
    con.close()
    prices = defaultdict(dict)
    for code, td, px in rows:
        ymd = td.replace("-", "")
        prices[code][ymd] = px
    return prices


def fetch_fundamentals_from_db(rebalance_dates):
    """Fetch conv_prem, balance, PE, vol from DuckDB for rebalance dates only."""
    con = connect()
    dates_fmt = [_ymd_to_dash(d) for d in rebalance_dates]
    placeholders = ",".join(["?"] * len(dates_fmt))
    rows = con.execute(
        f"SELECT v.code, v.trade_date, v.conv_prem_pct, v.outstanding_yi, v.pe_ttm, "
        f"  vd.vol_20d_pct "
        f"FROM valuation_daily v "
        f"LEFT JOIN universe u ON v.code = u.code "
        f"LEFT JOIN vol_daily vd ON u.ucode = vd.ucode AND v.trade_date = vd.trade_date "
        f"WHERE v.trade_date IN ({placeholders})",
        dates_fmt
    ).fetchall()
    con.close()
    fundamentals = defaultdict(dict)
    for code, td, conv_prem, balance, pe_ttm, vol_20d in rows:
        ymd = td.replace("-", "")
        fundamentals.setdefault(ymd, {})[code] = {
            "conv_prem": conv_prem,
            "balance": balance,
            "pe_ttm": pe_ttm,
            "vol_20d": vol_20d,
        }
    return fundamentals


def persist_prices_to_db(prices, codes_set):
    """Write fetched close prices into valuation_daily (price column only)."""
    con = connect()
    n_written = 0
    batch = []
    for code, date_prices in prices.items():
        if code not in codes_set:
            continue
        for ymd, px in date_prices.items():
            batch.append({
                "trade_date": _ymd_to_dash(ymd),
                "code": code,
                "price": px,
            })
    if batch:
        # Use ON CONFLICT to only set price if row doesn't exist or price is NULL
        cols = "trade_date, code, price"
        for row in batch:
            try:
                con.execute(
                    "INSERT INTO valuation_daily (trade_date, code, price) VALUES (?, ?, ?) "
                    "ON CONFLICT (trade_date, code) DO UPDATE SET price = "
                    "CASE WHEN valuation_daily.price IS NULL THEN excluded.price ELSE valuation_daily.price END",
                    [row["trade_date"], row["code"], row["price"]]
                )
                n_written += 1
            except Exception:
                pass
    con.close()
    return n_written


def persist_fundamentals_to_db(fundamentals):
    """Write fetched fundamentals into valuation_daily."""
    con = connect()
    n_written = 0
    for ymd, fund_map in fundamentals.items():
        trade_date = _ymd_to_dash(ymd)
        for code, f in fund_map.items():
            try:
                con.execute(
                    "INSERT INTO valuation_daily (trade_date, code, conv_prem_pct, outstanding_yi, pe_ttm) "
                    "VALUES (?, ?, ?, ?, ?) "
                    "ON CONFLICT (trade_date, code) DO UPDATE SET "
                    "conv_prem_pct = CASE WHEN valuation_daily.conv_prem_pct IS NULL THEN excluded.conv_prem_pct ELSE valuation_daily.conv_prem_pct END, "
                    "outstanding_yi = CASE WHEN valuation_daily.outstanding_yi IS NULL THEN excluded.outstanding_yi ELSE valuation_daily.outstanding_yi END, "
                    "pe_ttm = CASE WHEN valuation_daily.pe_ttm IS NULL THEN excluded.pe_ttm ELSE valuation_daily.pe_ttm END",
                    [trade_date, code, f.get("conv_prem"), f.get("balance"), f.get("pe_ttm")]
                )
                n_written += 1
            except Exception:
                pass
    con.close()
    return n_written


def classify_sector(conv_prem):
    if conv_prem is None:
        return "偏债"
    if conv_prem < 20:
        return "偏股"
    if conv_prem < 50:
        return "平衡"
    return "偏债"


def _percentile(sorted_vals, pct):
    if not sorted_vals:
        return 0
    k = (len(sorted_vals) - 1) * pct / 100
    lo = int(k)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = k - lo
    return sorted_vals[lo] + frac * (sorted_vals[hi] - sorted_vals[lo])


def _apply_pe_vol_filter(eligible):
    """Apply PE>0 and vol>Q1 filters matching strategy_score.py."""
    eligible = [b for b in eligible if b.get("pe_ttm") is not None and b["pe_ttm"] > 0]
    eligible = [b for b in eligible if b.get("vol_20d") is not None]
    if not eligible:
        return eligible
    vol_vals = sorted(b["vol_20d"] for b in eligible)
    vol_q1 = _percentile(vol_vals, 25)
    eligible = [b for b in eligible if b["vol_20d"] >= vol_q1]
    return eligible


def score_double_low(bonds):
    """Classic double-low: rank = 1.5*rank(conv_prem) + rank(price)."""
    eligible = [b for b in bonds if b.get("conv_prem") is not None and b.get("price") and b.get("balance") and b["balance"] > 0]
    eligible = _apply_pe_vol_filter(eligible)
    if not eligible:
        return []

    by_cp = sorted(eligible, key=lambda x: x["conv_prem"])
    by_px = sorted(eligible, key=lambda x: x["price"])
    cp_rank = {b["code"]: i + 1 for i, b in enumerate(by_cp)}
    px_rank = {b["code"]: i + 1 for i, b in enumerate(by_px)}
    for b in eligible:
        b["dl_score"] = 1.5 * cp_rank.get(b["code"], 999) + px_rank.get(b["code"], 999)
    eligible.sort(key=lambda x: x["dl_score"])
    return eligible[:30]


def score_sector_neutral(bonds):
    """Sector-neutral: double-low within each sector, top 10 per sector."""
    by_sector = defaultdict(list)
    for b in bonds:
        if b.get("conv_prem") is not None and b.get("price") and b.get("balance") and b["balance"] > 0:
            sec = classify_sector(b["conv_prem"])
            by_sector[sec].append(b)
    picks = []
    for sec in ["偏股", "平衡", "偏债"]:
        bonds_s = by_sector.get(sec, [])
        if not bonds_s:
            continue
        bonds_s = _apply_pe_vol_filter(bonds_s)
        if not bonds_s:
            continue

        by_cp = sorted(bonds_s, key=lambda x: x["conv_prem"])
        by_px = sorted(bonds_s, key=lambda x: x["price"])
        cp_rank = {b["code"]: i + 1 for i, b in enumerate(by_cp)}
        px_rank = {b["code"]: i + 1 for i, b in enumerate(by_px)}
        for b in bonds_s:
            b["dl_score"] = 1.5 * cp_rank.get(b["code"], 999) + px_rank.get(b["code"], 999)
        bonds_s.sort(key=lambda x: x["dl_score"])
        for b in bonds_s[:10]:
            b["sector"] = sec
        picks.extend(bonds_s[:10])
    return picks


def compute_avg_return(picks, sell_prices, top_n=10, buy_prices=None,
                       slippage_bps=SLIPPAGE_BPS, commission_bps=COMMISSION_BPS):
    """Compute equal-weight average return for top N picks with transaction costs."""
    returns = []
    slip = slippage_bps / 10000
    comm_half = commission_bps / 20000
    for b in picks[:top_n]:
        if buy_prices:
            px_entry = buy_prices.get(b["code"])
        else:
            px_entry = b.get("price")
        px_exit = sell_prices.get(b["code"])
        if px_entry and px_exit and px_entry > 0:
            actual_entry = px_entry * (1 + slip + comm_half)
            actual_exit = px_exit * (1 - slip - comm_half)
            returns.append((actual_exit - actual_entry) / actual_entry)
    if not returns:
        return None, 0
    return sum(returns) / len(returns), len(returns)


def dedup_equity(curve):
    """Remove duplicate dates. Keep last occurrence."""
    seen = {}
    for i, pt in enumerate(curve):
        seen[pt["date"]] = i
    last_indices = sorted(seen.values())
    return [curve[i] for i in last_indices]


def main():
    ap = argparse.ArgumentParser(description="Weekly-rebalance backtest for CB strategies")
    ap.add_argument("--start-date", default="", help="YYYY-MM-DD")
    ap.add_argument("--end-date", default="", help="YYYY-MM-DD")
    ap.add_argument("--days", type=int, default=5, help="trading days to backtest (if no start-date)")
    ap.add_argument("--top", type=int, default=10, help="top N picks for return calc")
    ap.add_argument("--skip-fetch", action="store_true", help="use cached data if available")
    ap.add_argument("--from-db", action="store_true",
                    help="read ALL data from DuckDB (fast, requires pre-populated valuation_daily)")
    ap.add_argument("--rebalance", default="weekly", choices=["daily", "weekly"])
    ap.add_argument("--holding-days", type=int, default=5, help="holding period in trading days")
    ap.add_argument("--slippage-bps", type=int, default=SLIPPAGE_BPS)
    ap.add_argument("--commission-bps", type=int, default=COMMISSION_BPS)
    args = ap.parse_args()

    if args.end_date:
        end_dt = datetime.strptime(args.end_date, "%Y-%m-%d")
    else:
        end_dt = datetime.now()
    if args.start_date:
        start_dt = datetime.strptime(args.start_date, "%Y-%m-%d")
    else:
        start_dt = end_dt - timedelta(days=args.days * 2)

    start_ymd = _yyyymmdd(start_dt)
    end_ymd = _yyyymmdd(end_dt)
    print(f"[backtest] {start_ymd} -> {end_ymd}")
    print(f"[costs] slippage={args.slippage_bps}bps one-way, commission={args.commission_bps}bps total")

    if not args.from_db and basic_data is None:
        print("[warn] iFinD not available, falling back to --from-db mode")
        args.from_db = True

    # ----- Get CB codes -----
    con = connect()
    start_dash = _ymd_to_dash(start_ymd)
    end_dash = _ymd_to_dash(end_ymd)
    try:
        rows = con.execute(
            "SELECT DISTINCT code FROM valuation_daily WHERE trade_date >= ? AND trade_date <= ?",
            [start_dash, end_dash]
        ).fetchall()
        if len(rows) < 50:
            raise RuntimeError("Too few historical codes")
        codes = [r[0] for r in rows]
        print(f"[universe] {len(codes)} codes from valuation_daily (historical)")
    except Exception:
        rows = con.execute("SELECT code FROM universe").fetchall()
        codes = [r[0] for r in rows]
        print(f"[universe] {len(codes)} codes from universe table (current snapshot)")
    con.close()
    codes_set = set(codes)

    # Load code -> underlying stock code mapping (for PE fetch)
    con = connect()
    code_ucode_rows = con.execute("SELECT code, ucode FROM universe WHERE ucode IS NOT NULL").fetchall()
    con.close()
    code_to_ucode = {r[0]: r[1] for r in code_ucode_rows}
    print(f"[mapping] {len(code_to_ucode)} CB -> underlying stock mappings")

    # ----- Fetch prices (to derive accurate trading dates) -----
    if args.from_db:
        print("[fetch] reading trading dates from DB...")
        trading_dates = fetch_trading_dates_from_db(start_ymd, end_ymd)
        if len(trading_dates) < 2:
            print(f"[error] only {len(trading_dates)} trading dates found, need >= 2")
            print("[hint] DB only has data for dates that were fetched via fetch_valuation.py")
            return
        print("[fetch] reading prices from DB...")
        prices = fetch_prices_from_db(trading_dates)
    else:
        print(f"[fetch] pulling historical prices for {len(codes)} bonds...")
        prices = fetch_history_prices(codes, start_ymd, end_ymd)
        # Derive trading dates from the union of all price dates
        all_dates = set()
        for code_dates in prices.values():
            all_dates.update(code_dates.keys())
        trading_dates = sorted(d for d in all_dates if start_ymd <= d <= end_ymd)

    if len(trading_dates) < 2:
        print(f"[error] only {len(trading_dates)} trading dates found, need >= 2")
        return

    print(f"[dates] {len(trading_dates)} trading days: {trading_dates[0]} -> {trading_dates[-1]}")

    # ----- Compute rebalance schedule -----
    if args.rebalance == "weekly":
        holding = args.holding_days
        rebalance_indices = list(range(0, len(trading_dates) - holding, holding))
        if not rebalance_indices or rebalance_indices[-1] + holding < len(trading_dates) - 1:
            rebalance_indices.append(rebalance_indices[-1] + holding if rebalance_indices else 0)
    else:
        rebalance_indices = list(range(len(trading_dates) - 1))
        holding = 1

    rebalance_ymds = set(trading_dates[i] for i in rebalance_indices)
    print(f"[rebalance] {args.rebalance} mode, {len(rebalance_indices)} rebalance points, holding {holding} days")

    total_px = sum(len(v) for v in prices.values())
    print(f"[prices] {total_px} price points for {len(prices)} bonds")

    # ----- Fetch fundamentals (rebalance dates only) -----
    rebalance_date_list = sorted(rebalance_ymds)

    if args.from_db:
        print(f"[fetch] reading fundamentals from DB for {len(rebalance_date_list)} rebalance dates...")
        fundamentals = fetch_fundamentals_from_db(rebalance_date_list)
        for td in rebalance_date_list:
            fund = fundamentals.get(td, {})
            n_cp = sum(1 for v in fund.values() if v.get("conv_prem") is not None)
            n_pe = sum(1 for v in fund.values() if v.get("pe_ttm") is not None)
            n_vol = sum(1 for v in fund.values() if v.get("vol_20d") is not None)
            print(f"  {td}: {len(fund)} bonds, {n_cp} conv_prem, {n_pe} PE, {n_vol} vol")
        # Warn if data is sparse
        if rebalance_date_list:
            sample = fundamentals.get(rebalance_date_list[0], {})
            n_complete = sum(1 for v in sample.values()
                            if v.get("conv_prem") is not None
                            and v.get("pe_ttm") is not None
                            and v.get("vol_20d") is not None)
            if n_complete < 50:
                print(f"[warn] Only {n_complete} bonds have complete fundamentals on {rebalance_date_list[0]}")
                if basic_data is not None:
                    print("[info] Fetching underlying stock PE from iFinD to supplement DB data...")
                    pe_map = fetch_underlying_pe_bulk(code_to_ucode, start_ymd, end_ymd)
                    merge_pe_into_fundamentals(fundamentals, code_to_ucode, pe_map)
                # Compute vol from prices if missing
                print("[vol] computing 20-day realized volatility from prices...")
                vol_map = compute_vol_from_prices(prices, trading_dates, rebalance_ymds)
                merge_vol_into_fundamentals(fundamentals, vol_map)
    else:
        print(f"[fetch] pulling fundamentals for {len(rebalance_date_list)} rebalance dates (not all {len(trading_dates)})...")
        fundamentals = {}
        for i, td in enumerate(rebalance_date_list):
            fund = fetch_day_fundamentals(codes, td)
            fundamentals[td] = fund
            n_pe = sum(1 for v in fund.values() if v.get("pe_ttm") is not None)
            n_vol = sum(1 for v in fund.values() if v.get("vol_20d") is not None)
            print(f"  [{i+1}/{len(rebalance_date_list)}] {td}: {len(fund)} conv_prem, {n_pe} PE, {n_vol} vol")

        # Persist fetched data to DB
        print("[persist] saving fetched data to DB...")
        n_px = persist_prices_to_db(prices, codes_set)
        n_fund = persist_fundamentals_to_db(fundamentals)
        print(f"[persist] wrote {n_px} price rows + {n_fund} fundamental rows to DB")

        # Fetch PE from underlying stocks
        pe_map = fetch_underlying_pe_bulk(code_to_ucode, start_ymd, end_ymd)
        merge_pe_into_fundamentals(fundamentals, code_to_ucode, pe_map)

        # Compute vol from price history
        print("[vol] computing 20-day realized volatility from prices...")
        vol_map = compute_vol_from_prices(prices, trading_dates, rebalance_ymds)
        merge_vol_into_fundamentals(fundamentals, vol_map)

        # Persist PE into DB
        pe_persist_count = 0
        pe_con = connect()
        for ymd, fund_map in fundamentals.items():
            trade_date = _ymd_to_dash(ymd)
            for code, f in fund_map.items():
                pe = f.get("pe_ttm")
                if pe is not None:
                    try:
                        pe_con.execute(
                            "UPDATE valuation_daily SET pe_ttm = ? WHERE trade_date = ? AND code = ? AND (pe_ttm IS NULL OR pe_ttm = 0)",
                            [pe, trade_date, code]
                        )
                        pe_persist_count += 1
                    except Exception:
                        pass
        pe_con.close()
        print(f"[persist] updated {pe_persist_count} PE values in DB")

    # ----- Run backtest -----
    equity_dl = 1.0
    equity_sn = 1.0
    equity_mkt = 1.0
    results = []
    prev_rb_end = -1

    for rb_i in rebalance_indices:
        td_select = trading_dates[rb_i]
        td_buy = trading_dates[min(rb_i + 1, len(trading_dates) - 1)]
        td_sell = trading_dates[min(rb_i + 1 + holding, len(trading_dates) - 1)]

        fund = fundamentals.get(td_select, {})

        # Build bond list: bonds that have BOTH price and fundamental data
        day_bonds = []
        for code in prices:
            px = prices[code].get(td_select)
            if not px:
                continue
            f = fund.get(code, {})
            day_bonds.append({
                "code": code,
                "price": px,
                "conv_prem": f.get("conv_prem"),
                "balance": f.get("balance"),
                "pe_ttm": f.get("pe_ttm"),
                "vol_20d": f.get("vol_20d"),
            })

        # Strategy picks
        dl_picks = score_double_low(day_bonds)
        sn_picks = score_sector_neutral(day_bonds)

        # T+1 buy prices, T+1+holding sell prices
        buy_prices = {code: prices[code].get(td_buy) for code in prices if td_buy in prices[code]}
        sell_prices = {code: prices[code].get(td_sell) for code in prices if td_sell in prices[code]}

        # Returns
        dl_ret, dl_n = compute_avg_return(
            dl_picks[:args.top], sell_prices, args.top, buy_prices,
            args.slippage_bps, args.commission_bps
        )
        sn_ret, sn_n = compute_avg_return(
            sn_picks[:args.top], sell_prices, args.top, buy_prices,
            args.slippage_bps, args.commission_bps
        )

        # Market return
        mkt_rets = []
        slip = args.slippage_bps / 10000
        comm_half = args.commission_bps / 20000
        for b in day_bonds:
            px_buy = buy_prices.get(b["code"])
            px_sell = sell_prices.get(b["code"])
            if px_buy and px_sell and px_buy > 0:
                actual_entry = px_buy * (1 + slip + comm_half)
                actual_exit = px_sell * (1 - slip - comm_half)
                mkt_rets.append((actual_exit - actual_entry) / actual_entry)
        mkt_ret = sum(mkt_rets) / len(mkt_rets) if mkt_rets else 0

        # Multiplicative compounding
        if dl_ret is not None:
            equity_dl = max(equity_dl * (1 + dl_ret), 0.0)
        if sn_ret is not None:
            equity_sn = max(equity_sn * (1 + sn_ret), 0.0)
        equity_mkt = max(equity_mkt * (1 + mkt_ret), 0.0)

        # Emit equity curve points
        period_start = max(prev_rb_end + 1, rb_i)
        period_end = min(rb_i + 1 + holding, len(trading_dates))

        eq_start_dl = equity_dl / (1 + dl_ret) if dl_ret is not None and (1 + dl_ret) > 0 else equity_dl
        eq_start_sn = equity_sn / (1 + sn_ret) if sn_ret is not None and (1 + sn_ret) > 0 else equity_sn
        eq_start_mkt = equity_mkt / (1 + mkt_ret) if (1 + mkt_ret) > 0 else equity_mkt

        for d_i in range(period_start, period_end):
            frac = (d_i - rb_i) / max(holding, 1) if d_i > rb_i else 0
            frac = min(frac, 1.0)
            results.append({
                "date": trading_dates[d_i],
                "cum_dl": round(_interp_log(eq_start_dl, equity_dl, frac) - 1, 6),
                "cum_sn": round(_interp_log(eq_start_sn, equity_sn, frac) - 1, 6),
                "cum_mkt": round(_interp_log(eq_start_mkt, equity_mkt, frac) - 1, 6),
            })

        prev_rb_end = period_end - 1

        dl_s = f"{dl_ret*100:+.3f}%" if dl_ret is not None else "N/A"
        sn_s = f"{sn_ret*100:+.3f}%" if sn_ret is not None else "N/A"
        cum_dl_pct = (equity_dl - 1) * 100
        cum_sn_pct = (equity_sn - 1) * 100
        cum_mkt_pct = (equity_mkt - 1) * 100
        print(f"  {td_select}->{td_buy}->{td_sell}: DL={dl_s}(n={dl_n}) SN={sn_s}(n={sn_n}) MKT={mkt_ret*100:+.3f}% | cum: DL={cum_dl_pct:+.3f}% SN={cum_sn_pct:+.3f}% MKT={cum_mkt_pct:+.3f}%")

    # ----- Summary -----
    n_rebalances = len(rebalance_indices)
    n_trading_days = len(trading_dates)
    cum_dl_pct = (equity_dl - 1) * 100
    cum_sn_pct = (equity_sn - 1) * 100
    cum_mkt_pct = (equity_mkt - 1) * 100

    if n_trading_days > 0:
        ann_dl = ((max(equity_dl, 0.001)) ** (252 / n_trading_days) - 1) * 100
        ann_sn = ((max(equity_sn, 0.001)) ** (252 / n_trading_days) - 1) * 100
        ann_mkt = ((max(equity_mkt, 0.001)) ** (252 / n_trading_days) - 1) * 100
    else:
        ann_dl = ann_sn = ann_mkt = 0

    results = dedup_equity(results)

    print(f"\n{'='*70}")
    print(f"回测区间: {trading_dates[0]} -> {trading_dates[-1]} ({n_trading_days} 个交易日)")
    print(f"调仓频率: {args.rebalance} (共{n_rebalances}次调仓, T日选券->T+1买入->持有{holding}日)")
    print(f"交易成本: 滑点{args.slippage_bps}bps(单边), 手续费{args.commission_bps}bps(往返)")
    print(f"策略过滤: PE>0 + vol>Q1 (与实盘strategy_score一致)")
    print(f"{'='*70}")
    print(f"{'策略':<20} {'累计收益':>10} {'年化(复利)':>12}")
    print(f"{'-'*44}")
    print(f"{'双低Top'+str(args.top):<20} {cum_dl_pct:>+9.3f}% {ann_dl:>+11.1f}%")
    print(f"{'分域双低Top'+str(args.top):<20} {cum_sn_pct:>+9.3f}% {ann_sn:>+11.1f}%")
    print(f"{'全市场等权':<20} {cum_mkt_pct:>+9.3f}% {ann_mkt:>+11.1f}%")

    # ----- Save -----
    out_dir = f"data/raw/asof={end_ymd}"
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{out_dir}/backtest_weekly.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            "start_date": trading_dates[0],
            "end_date": trading_dates[-1],
            "trading_days": n_trading_days,
            "rebalance": args.rebalance,
            "holding_days": holding,
            "n_rebalances": n_rebalances,
            "slippage_bps": args.slippage_bps,
            "commission_bps": args.commission_bps,
            "cum_return_dl_pct": round(cum_dl_pct, 3),
            "cum_return_sn_pct": round(cum_sn_pct, 3),
            "cum_return_mkt_pct": round(cum_mkt_pct, 3),
            "annualized_dl_pct": round(ann_dl, 1),
            "annualized_sn_pct": round(ann_sn, 1),
            "annualized_mkt_pct": round(ann_mkt, 1),
            "equity_curve": results,
        }, f, ensure_ascii=False, indent=2)
    print(f"\n[done] -> {out_path}")


if __name__ == "__main__":
    main()
