"""Simple 1-week backtest for convertible bond strategies.

Fetches historical daily data:
  - OHLCV from cmd_history_quotation (batch, fast)
  - Conv premium + balance from basic_data_service (per-day, slower)
Runs double-low and sector-neutral strategies on each day,
tracks cumulative returns over the period.

Improvements over naive version:
  - Multiplicative compounding (not additive)
  - Configurable slippage + commission
  - Historical universe (bonds with price data on each date, no survivorship bias)
  - Deduplicated equity curve (keeps last occurrence on tie)

Usage:
  python scripts/backtest_weekly.py --end-date 2026-04-23 --days 5
  python scripts/backtest_weekly.py --start-date 2026-01-23 --end-date 2026-04-23
"""
import argparse
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(__file__))
from _db import connect
from _ifind import basic_data, history, batched


SLIPPAGE_BPS = 10       # one-way slippage in basis points (0.1%)
COMMISSION_BPS = 2      # round-trip commission in basis points (0.02%)


def _yyyymmdd(dt):
    return dt.strftime("%Y%m%d")


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


def fetch_trading_dates(codes, start_ymd, end_ymd):
    """Get list of trading dates from iFinD history."""
    r = history(codes[0], "close", f"{start_ymd[:4]}-{start_ymd[4:6]}-{start_ymd[6:]}",
                f"{end_ymd[:4]}-{end_ymd[4:6]}-{end_ymd[6:]}")
    dates = []
    for t in r.get("tables", []):
        for d in t.get("time", []):
            if d and d != "-":
                dates.append(d.replace("-", ""))
    return sorted(set(dates))


def fetch_history_prices(codes, start_ymd, end_ymd):
    """Fetch daily close prices for all CBs."""
    prices = defaultdict(dict)  # {code: {date_ymd: price}}
    for batch_codes in batched(codes, 30):
        try:
            r = history(
                batch_codes,
                "close",
                f"{start_ymd[:4]}-{start_ymd[4:6]}-{start_ymd[6:]}",
                f"{end_ymd[:4]}-{end_ymd[4:6]}-{end_ymd[6:]}",
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
    """Fetch conv_prem and balance for a specific date via basic_data."""
    date_param = f"{date_ymd[:4]}-{date_ymd[4:6]}-{date_ymd[6:]}"
    fields = [
        {"indicator": "ths_conversion_premium_rate_cbond", "indiparams": [date_param]},
        {"indicator": "ths_bond_balance_cbond", "indiparams": [date_param]},
    ]
    result = {}
    for batch_codes in batched(codes, 50):
        try:
            r = basic_data(batch_codes, fields)
            for t in r.get("tables", []):
                code = t.get("thscode", "")
                tbl = t.get("table", {})
                conv_prem = _safe_float(tbl.get("ths_conversion_premium_rate_cbond", []), 0)
                balance = _safe_float(tbl.get("ths_bond_balance_cbond", []), 0)
                if conv_prem is not None:
                    result[code] = {"conv_prem": conv_prem, "balance": balance}
            time.sleep(0.12)
        except Exception as e:
            print(f"[warn] basic_data batch for {date_ymd}: {e}")
    return result


def classify_sector(conv_prem):
    if conv_prem is None:
        return "偏债"
    if conv_prem < 20:
        return "偏股"
    if conv_prem < 50:
        return "平衡"
    return "偏债"


def score_double_low(bonds):
    """Classic double-low: rank = 1.5*rank(conv_prem) + rank(price).
    Filters: balance > 0, conv_prem not None, price present.
    Matches strategy_score.py: also requires PE > 0 and vol > Q1 when available.
    """
    eligible = [b for b in bonds if b.get("conv_prem") is not None and b.get("price") and b.get("balance") and b["balance"] > 0]
    if not eligible:
        return []

    # Optional PE filter (when available in fundamentals)
    has_pe = any(b.get("pe_ttm") is not None for b in eligible)
    if has_pe:
        eligible = [b for b in eligible if b.get("pe_ttm") is None or b["pe_ttm"] > 0]

    # Optional vol filter: vol > Q1 (when available)
    has_vol = any(b.get("vol_20d") is not None for b in eligible)
    if has_vol and len(eligible) > 4:
        vol_vals = sorted(b["vol_20d"] for b in eligible if b.get("vol_20d") is not None)
        if vol_vals:
            k = (len(vol_vals) - 1) * 0.25
            lo = int(k)
            hi = min(lo + 1, len(vol_vals) - 1)
            vol_q1 = vol_vals[lo] + (k - lo) * (vol_vals[hi] - vol_vals[lo])
            eligible = [b for b in eligible if b.get("vol_20d") is None or b["vol_20d"] >= vol_q1]

    if not eligible:
        return []

    by_cp = sorted(eligible, key=lambda x: x["conv_prem"])
    by_px = sorted(eligible, key=lambda x: x["price"])
    cp_rank = {b["code"]: i for i, b in enumerate(by_cp)}
    px_rank = {b["code"]: i for i, b in enumerate(by_px)}
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

        # Same PE/vol filters as score_double_low
        has_pe = any(b.get("pe_ttm") is not None for b in bonds_s)
        if has_pe:
            bonds_s = [b for b in bonds_s if b.get("pe_ttm") is None or b["pe_ttm"] > 0]

        has_vol = any(b.get("vol_20d") is not None for b in bonds_s)
        if has_vol and len(bonds_s) > 4:
            vol_vals = sorted(b["vol_20d"] for b in bonds_s if b.get("vol_20d") is not None)
            if vol_vals:
                k = (len(vol_vals) - 1) * 0.25
                lo = int(k)
                hi = min(lo + 1, len(vol_vals) - 1)
                vol_q1 = vol_vals[lo] + (k - lo) * (vol_vals[hi] - vol_vals[lo])
                bonds_s = [b for b in bonds_s if b.get("vol_20d") is None or b["vol_20d"] >= vol_q1]

        if not bonds_s:
            continue

        by_cp = sorted(bonds_s, key=lambda x: x["conv_prem"])
        by_px = sorted(bonds_s, key=lambda x: x["price"])
        cp_rank = {b["code"]: i for i, b in enumerate(by_cp)}
        px_rank = {b["code"]: i for i, b in enumerate(by_px)}
        for b in bonds_s:
            b["dl_score"] = 1.5 * cp_rank.get(b["code"], 999) + px_rank.get(b["code"], 999)
        bonds_s.sort(key=lambda x: x["dl_score"])
        for b in bonds_s[:10]:
            b["sector"] = sec
        picks.extend(bonds_s[:10])
    return picks


def compute_avg_return(picks, sell_prices, top_n=10, buy_prices=None,
                       slippage_bps=SLIPPAGE_BPS, commission_bps=COMMISSION_BPS):
    """Compute equal-weight average return for top N picks with transaction costs.

    If buy_prices provided, use those as entry (T+1 buy);
    otherwise use pick's price as entry (legacy daily mode).
    Slippage: buy at buy_price * (1 + slippage), sell at sell_price * (1 - slippage).
    Commission: deducted from round-trip.
    """
    returns = []
    for b in picks[:top_n]:
        if buy_prices:
            px_entry = buy_prices.get(b["code"])
        else:
            px_entry = b.get("price")
        px_exit = sell_prices.get(b["code"])
        if px_entry and px_exit and px_entry > 0:
            slip = slippage_bps / 10000
            comm = commission_bps / 10000
            actual_entry = px_entry * (1 + slip)
            actual_exit = px_exit * (1 - slip - comm)
            returns.append((actual_exit - actual_entry) / actual_entry)
    if not returns:
        return None, 0
    return sum(returns) / len(returns), len(returns)


def dedup_equity(curve):
    """Remove duplicate dates (rebalance points appear twice).
    Keep the LAST occurrence — it has the updated cumulative value.
    """
    seen = {}
    for i, pt in enumerate(curve):
        seen[pt["date"]] = i
    # Rebuild keeping only last occurrence of each date
    last_indices = sorted(seen.values())
    return [curve[i] for i in last_indices]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-date", default="", help="YYYY-MM-DD")
    ap.add_argument("--end-date", default="", help="YYYY-MM-DD")
    ap.add_argument("--days", type=int, default=5, help="trading days to backtest")
    ap.add_argument("--top", type=int, default=10, help="top N picks for return calc")
    ap.add_argument("--skip-fetch", action="store_true", help="use cached data if available")
    ap.add_argument("--rebalance", default="weekly", choices=["daily", "weekly"], help="rebalance frequency")
    ap.add_argument("--holding-days", type=int, default=5, help="holding period in trading days for weekly mode")
    ap.add_argument("--slippage-bps", type=int, default=SLIPPAGE_BPS, help="one-way slippage in bps")
    ap.add_argument("--commission-bps", type=int, default=COMMISSION_BPS, help="round-trip commission in bps")
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
    print(f"[backtest] {start_ymd} → {end_ymd}")
    print(f"[costs] slippage={args.slippage_bps}bps one-way, commission={args.commission_bps}bps round-trip")

    # 1. Get CB codes from DB (use all codes ever seen, not just current universe)
    con = connect()
    # Try to get all historical codes from valuation_daily for survivorship-free universe
    try:
        rows = con.execute(
            "SELECT DISTINCT code FROM valuation_daily WHERE trade_date >= ? AND trade_date <= ?",
            [start_ymd[:4] + "-" + start_ymd[4:6] + "-" + start_ymd[6:],
             end_ymd[:4] + "-" + end_ymd[4:6] + "-" + end_ymd[6:]]
        ).fetchall()
        if len(rows) < 50:
            raise RuntimeError("Too few historical codes, falling back to universe table")
        codes = [r[0] for r in rows]
        print(f"[universe] {len(codes)} codes from valuation_daily (historical, no survivorship bias)")
    except Exception:
        rows = con.execute("SELECT code FROM universe").fetchall()
        codes = [r[0] for r in rows]
        print(f"[universe] {len(codes)} codes from DB (current only, may have survivorship bias)")
    con.close()

    # 2. Fetch trading dates
    print(f"[fetch] getting trading dates...")
    trading_dates = fetch_trading_dates(codes[:1], start_ymd, end_ymd)
    print(f"[dates] {len(trading_dates)} trading days: {trading_dates}")

    if len(trading_dates) < 2:
        print("[error] need at least 2 trading days")
        return

    # 3. Fetch historical prices
    print(f"[fetch] pulling historical prices for {len(codes)} bonds...")
    prices = fetch_history_prices(codes, start_ymd, end_ymd)
    total_px = sum(len(v) for v in prices.values())
    print(f"[prices] {total_px} price points for {len(prices)} bonds")

    # 4. Fetch fundamentals per day (conv_prem, balance)
    print(f"[fetch] pulling daily fundamentals...")
    fundamentals = {}  # {date_ymd: {code: {conv_prem, balance}}}
    for td in trading_dates:
        fund = fetch_day_fundamentals(codes, td)
        fundamentals[td] = fund
        print(f"  {td}: {len(fund)} bonds with conv_prem")

    # 5. Run backtest
    # Build rebalance dates: weekly = every 5th trading day
    if args.rebalance == "weekly":
        holding = args.holding_days
        rebalance_indices = list(range(0, len(trading_dates) - holding, holding))
        if not rebalance_indices or rebalance_indices[-1] + holding < len(trading_dates) - 1:
            rebalance_indices.append(rebalance_indices[-1] + holding if rebalance_indices else 0)
        print(f"[rebalance] weekly mode, {len(rebalance_indices)} rebalance points, holding {holding} days")
    else:
        rebalance_indices = list(range(len(trading_dates) - 1))
        holding = 1

    # Use multiplicative equity curve (not additive)
    equity_dl = 1.0
    equity_sn = 1.0
    equity_mkt = 1.0
    results = []

    # Track per-day equity for all dates (non-overlapping)
    prev_rb_end = -1  # last date index already emitted

    for rb_i in rebalance_indices:
        # T day: select picks using T day's fundamentals
        td_select = trading_dates[rb_i]
        # T+1 day: buy at open (use T+1 close as proxy)
        td_buy = trading_dates[min(rb_i + 1, len(trading_dates) - 1)]
        # T+1+holding: sell
        td_sell = trading_dates[min(rb_i + 1 + holding, len(trading_dates) - 1)]

        fund = fundamentals.get(td_select, {})

        # Build bond list for selection day — use bonds that have price data on that date
        # This avoids survivorship bias: delisted/matured bonds are included if they traded
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
            })

        # Strategy picks
        dl_picks = score_double_low(day_bonds)
        sn_picks = score_sector_neutral(day_bonds)

        # T+1 buy prices, T+1+holding sell prices
        buy_prices = {code: prices[code].get(td_buy) for code in prices if td_buy in prices[code]}
        sell_prices = {code: prices[code].get(td_sell) for code in prices if td_sell in prices[code]}

        # Returns: buy at T+1, sell at T+1+holding (with transaction costs)
        dl_ret, dl_n = compute_avg_return(
            dl_picks[:args.top], sell_prices, args.top, buy_prices,
            args.slippage_bps, args.commission_bps
        )
        sn_ret, sn_n = compute_avg_return(
            sn_picks[:args.top], sell_prices, args.top, buy_prices,
            args.slippage_bps, args.commission_bps
        )

        # Market return (with transaction costs)
        mkt_rets = []
        for b in day_bonds:
            px_buy = buy_prices.get(b["code"])
            px_sell = sell_prices.get(b["code"])
            if px_buy and px_sell and px_buy > 0:
                slip = args.slippage_bps / 10000
                comm = args.commission_bps / 10000
                actual_entry = px_buy * (1 + slip)
                actual_exit = px_sell * (1 - slip - comm)
                mkt_rets.append((actual_exit - actual_entry) / actual_entry)
        mkt_ret = sum(mkt_rets) / len(mkt_rets) if mkt_rets else 0

        # Multiplicative compounding
        if dl_ret is not None:
            equity_dl *= (1 + dl_ret)
        if sn_ret is not None:
            equity_sn *= (1 + sn_ret)
        equity_mkt *= (1 + mkt_ret)

        # Emit equity curve points for this holding period
        # Only emit dates not yet covered (non-overlapping)
        period_start = max(prev_rb_end + 1, rb_i)
        period_end = min(rb_i + 1 + holding, len(trading_dates))

        # Snapshot equity at start of period
        eq_start_dl = equity_dl / (1 + dl_ret) if dl_ret is not None and (1 + dl_ret) != 0 else equity_dl
        eq_start_sn = equity_sn / (1 + sn_ret) if sn_ret is not None and (1 + sn_ret) != 0 else equity_sn
        eq_start_mkt = equity_mkt / (1 + mkt_ret) if (1 + mkt_ret) != 0 else equity_mkt

        for d_i in range(period_start, period_end):
            frac = (d_i - rb_i) / max(holding, 1) if d_i > rb_i else 0
            frac = min(frac, 1.0)
            # Log-space interpolation for multiplicative equity
            import math
            def interp_log(start, end, f):
                if start <= 0 or end <= 0:
                    return end
                return start * math.exp(f * math.log(end / start))

            results.append({
                "date": trading_dates[d_i],
                "cum_dl": round(interp_log(eq_start_dl, equity_dl, frac) - 1, 6),
                "cum_sn": round(interp_log(eq_start_sn, equity_sn, frac) - 1, 6),
                "cum_mkt": round(interp_log(eq_start_mkt, equity_mkt, frac) - 1, 6),
            })

        prev_rb_end = period_end - 1

        dl_s = f"{dl_ret*100:+.3f}%" if dl_ret is not None else "N/A"
        sn_s = f"{sn_ret*100:+.3f}%" if sn_ret is not None else "N/A"
        cum_dl_pct = (equity_dl - 1) * 100
        cum_sn_pct = (equity_sn - 1) * 100
        cum_mkt_pct = (equity_mkt - 1) * 100
        print(f"  {td_select}选券→{td_buy}买入→{td_sell}卖出: 双低={dl_s}(n={dl_n}) 分域={sn_s}(n={sn_n}) 市场={mkt_ret*100:+.3f}% | 累计: 双低={cum_dl_pct:+.3f}% 分域={cum_sn_pct:+.3f}% 市场={cum_mkt_pct:+.3f}%")

    # 6. Summary
    n_rebalances = len(rebalance_indices)
    n_trading_days = len(trading_dates)
    cum_dl_pct = (equity_dl - 1) * 100
    cum_sn_pct = (equity_sn - 1) * 100
    cum_mkt_pct = (equity_mkt - 1) * 100

    # Annualized: (equity)^(252/n_trading_days) - 1
    if n_trading_days > 0:
        ann_dl = ((equity_dl) ** (252 / n_trading_days) - 1) * 100
        ann_sn = ((equity_sn) ** (252 / n_trading_days) - 1) * 100
        ann_mkt = ((equity_mkt) ** (252 / n_trading_days) - 1) * 100
    else:
        ann_dl = ann_sn = ann_mkt = 0

    # Deduplicate equity curve
    results = dedup_equity(results)

    print(f"\n{'='*70}")
    print(f"回测区间: {trading_dates[0]} → {trading_dates[-1]} ({n_trading_days} 个交易日)")
    print(f"调仓频率: {args.rebalance} (共{n_rebalances}次调仓, T日选券→T+1买入→持有{holding}日)")
    print(f"交易成本: 滑点{args.slippage_bps}bps(单边), 手续费{args.commission_bps}bps(往返)")
    print(f"{'='*70}")
    print(f"{'策略':<20} {'累计收益':>10} {'年化(复利)':>12}")
    print(f"{'-'*44}")
    print(f"{'双低Top'+str(args.top):<20} {cum_dl_pct:>+9.3f}% {ann_dl:>+11.1f}%")
    print(f"{'分域双低Top'+str(args.top):<20} {cum_sn_pct:>+9.3f}% {ann_sn:>+11.1f}%")
    print(f"{'全市场等权':<20} {cum_mkt_pct:>+9.3f}% {ann_mkt:>+11.1f}%")

    # 7. Save
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
    print(f"\n[done] → {out_path}")


if __name__ == "__main__":
    main()
