"""Simple 1-week backtest for convertible bond strategies.

Fetches historical daily data:
  - OHLCV from cmd_history_quotation (batch, fast)
  - Conv premium + balance from basic_data_service (per-day, slower)
Runs double-low and sector-neutral strategies on each day,
tracks cumulative returns over the period.

Usage:
  python scripts/backtest_weekly.py --end-date 2026-04-23 --days 5
  python scripts/backtest_weekly.py --start-date 2026-04-17 --end-date 2026-04-23
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
    # Use first code to get trading calendar
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
    """Classic double-low: rank = 1.5*rank(conv_prem) + rank(price)."""
    eligible = [b for b in bonds if b.get("conv_prem") is not None and b.get("price") and b.get("balance") and b["balance"] > 0]
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


def compute_avg_return(picks, next_prices, top_n=10):
    """Compute equal-weight average return for top N picks."""
    returns = []
    for b in picks[:top_n]:
        px_today = b.get("price")
        px_next = next_prices.get(b["code"])
        if px_today and px_next and px_today > 0:
            returns.append((px_next - px_today) / px_today)
    if not returns:
        return None, 0
    return sum(returns) / len(returns), len(returns)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-date", default="", help="YYYY-MM-DD")
    ap.add_argument("--end-date", default="", help="YYYY-MM-DD")
    ap.add_argument("--days", type=int, default=5, help="trading days to backtest")
    ap.add_argument("--top", type=int, default=10, help="top N picks for return calc")
    ap.add_argument("--skip-fetch", action="store_true", help="use cached data if available")
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

    # 1. Get CB codes from DB
    con = connect()
    rows = con.execute("SELECT code FROM universe").fetchall()
    con.close()
    codes = [r[0] for r in rows]
    print(f"[universe] {len(codes)} codes from DB")

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
    results = []
    cum_dl = 0.0
    cum_sn = 0.0
    cum_mkt = 0.0

    for i in range(len(trading_dates) - 1):
        td = trading_dates[i]
        td_next = trading_dates[i + 1]
        fund = fundamentals.get(td, {})
        next_prices = {code: prices[code].get(td_next) for code in prices if td_next in prices[code]}

        # Build bond list for this day
        day_bonds = []
        for code in codes:
            px = prices.get(code, {}).get(td)
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

        # Returns
        dl_ret, dl_n = compute_avg_return(dl_picks, next_prices, args.top)
        sn_ret, sn_n = compute_avg_return(sn_picks, next_prices, args.top)

        # Market return (all bonds with price on both days)
        mkt_rets = []
        for b in day_bonds:
            px_next = next_prices.get(b["code"])
            if b["price"] and px_next and b["price"] > 0:
                mkt_rets.append((px_next - b["price"]) / b["price"])
        mkt_ret = sum(mkt_rets) / len(mkt_rets) if mkt_rets else 0

        if dl_ret is not None:
            cum_dl += dl_ret
        if sn_ret is not None:
            cum_sn += sn_ret
        cum_mkt += mkt_ret

        results.append({
            "date": td,
            "date_next": td_next,
            "n_bonds": len(day_bonds),
            "n_with_prem": len(fund),
            "dl_return": round(dl_ret * 100, 3) if dl_ret is not None else None,
            "dl_n": dl_n,
            "sn_return": round(sn_ret * 100, 3) if sn_ret is not None else None,
            "sn_n": sn_n,
            "mkt_return": round(mkt_ret * 100, 3),
            "cum_dl": round(cum_dl * 100, 3),
            "cum_sn": round(cum_sn * 100, 3),
            "cum_mkt": round(cum_mkt * 100, 3),
        })
        dl_s = f"{dl_ret*100:+.3f}%" if dl_ret is not None else "N/A"
        sn_s = f"{sn_ret*100:+.3f}%" if sn_ret is not None else "N/A"
        print(f"  {td}→{td_next}: 双低={dl_s}(n={dl_n}) 分域={sn_s}(n={sn_n}) 市场={mkt_ret*100:+.3f}% | 累计: 双低={cum_dl*100:+.3f}% 分域={cum_sn*100:+.3f}% 市场={cum_mkt*100:+.3f}%")

    # 6. Summary
    n_days = len(results)
    print(f"\n{'='*70}")
    print(f"回测区间: {trading_dates[0]} → {trading_dates[-1]} ({n_days} 个交易日)")
    print(f"{'='*70}")
    print(f"{'策略':<20} {'累计收益':>10} {'日均收益':>10}")
    print(f"{'-'*42}")
    print(f"{'双低Top'+str(args.top):<20} {cum_dl*100:>+9.3f}% {cum_dl*100/n_days:>+9.3f}%")
    print(f"{'分域双低Top'+str(args.top):<20} {cum_sn*100:>+9.3f}% {cum_sn*100/n_days:>+9.3f}%")
    print(f"{'全市场等权':<20} {cum_mkt*100:>+9.3f}% {cum_mkt*100/n_days:>+9.3f}%")

    # 7. Save
    out_dir = f"data/raw/asof={end_ymd}"
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{out_dir}/backtest_weekly.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            "start_date": trading_dates[0],
            "end_date": trading_dates[-1],
            "trading_days": n_days,
            "cum_return_dl_pct": round(cum_dl * 100, 3),
            "cum_return_sn_pct": round(cum_sn * 100, 3),
            "cum_return_mkt_pct": round(cum_mkt * 100, 3),
            "daily_results": results,
        }, f, ensure_ascii=False, indent=2)
    print(f"\n[done] → {out_path}")


if __name__ == "__main__":
    main()
