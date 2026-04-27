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


# ── Default cost parameters ──────────────────────────────────────────
SLIPPAGE_BPS = 10       # one-way slippage (0.10%)
COMMISSION_BPS = 2      # round-trip commission (0.02%), split half buy / half sell

# ── Universe filters ─────────────────────────────────────────────────
MIN_BALANCE_YI = 2.0    # minimum outstanding balance in yi (2 yi = 200M CNY)
MAX_PRICE = 150.0       # exclude bonds priced above 150
MIN_HOLDINGS = 5        # strategy returns N/A if fewer picks available


# ── Helpers ──────────────────────────────────────────────────────────

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


# ── Data layer ───────────────────────────────────────────────────────

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
    prices = defaultdict(dict)
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


def fetch_day_fundamentals(codes, date_ymd, ucode_map=None, vol_db=None):
    """Fetch conv_prem + balance from iFinD; PE via underlying stock; vol from local DB.

    Aligns with the live pipeline: PE is keyed by underlying stock code (ths_pe_ttm),
    vol_20d is computed by compute_volatility.py and stored in vol_daily DB.
    """
    date_param = _ymd_to_dash(date_ymd)
    cb_fields = [
        {"indicator": "ths_conversion_premium_rate_cbond", "indiparams": [date_param]},
        {"indicator": "ths_bond_balance_cbond", "indiparams": [date_param]},
    ]
    result = {}
    for batch_codes in batched(codes, 40):
        try:
            r = basic_data(batch_codes, cb_fields)
            for t in r.get("tables", []):
                code = t.get("thscode", "")
                tbl = t.get("table", {})
                conv_prem = _safe_float(tbl.get("ths_conversion_premium_rate_cbond", []), 0)
                balance = _safe_float(tbl.get("ths_bond_balance_cbond", []), 0)
                if conv_prem is not None:
                    result[code] = {
                        "conv_prem": conv_prem,
                        "balance": balance,
                        "pe_ttm": None,
                        "vol_20d": None,
                    }
            time.sleep(0.12)
        except Exception as e:
            print(f"[warn] basic_data batch for {date_ymd}: {e}")

    # PE: fetch via underlying stock code
    if ucode_map:
        ucodes = list({ucode_map.get(c, "") for c in result if ucode_map.get(c, "")})
        pe_by_ucode = {}
        for batch in batched(ucodes, 50):
            try:
                r = basic_data(batch, [{"indicator": "ths_pe_ttm", "indiparams": [date_param]}])
                for t in r.get("tables", []):
                    pe_by_ucode[t.get("thscode", "")] = _safe_float(
                        t.get("table", {}).get("ths_pe_ttm", []), 0)
                time.sleep(0.10)
            except Exception as e:
                print(f"[warn] pe batch for {date_ymd}: {e}")
        for code, row in result.items():
            row["pe_ttm"] = pe_by_ucode.get(ucode_map.get(code, ""))

    # vol_20d: from local DB (computed by compute_volatility.py)
    if vol_db is not None and ucode_map:
        for code, row in result.items():
            ucode = ucode_map.get(code, "")
            row["vol_20d"] = vol_db.get((ucode, date_param))

    return result


def _load_ucode_map():
    """Build bond→ucode map from any available dataset.json (most stable)."""
    import glob as _glob
    paths = sorted(_glob.glob("data/raw/asof=????-??-??/dataset.json"))
    for path in reversed(paths):
        try:
            ds = json.load(open(path, encoding="utf-8"))
            m = {item["code"]: item.get("ucode", "") for item in ds.get("items", [])}
            if m:
                return m
        except Exception:
            pass
    return {}


def _load_vol_db():
    """Load vol_daily as {(ucode, date_iso): vol_20d_pct}."""
    con = connect()
    rows = con.execute("SELECT ucode, trade_date, vol_20d_pct FROM vol_daily").fetchall()
    con.close()
    return {(r[0], r[1]): r[2] for r in rows}


def fetch_underlying_pe_bulk(code_to_ucode, start_ymd, end_ymd):
    """Fetch PE_TTM for all underlying stocks via history(), return {ucode: {ymd: pe}}."""
    ucodes = list(set(code_to_ucode.values()))
    print(f"[fetch] pulling underlying stock PE for {len(ucodes)} stocks...")
    pe_map = defaultdict(dict)
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
    """Compute annualized 20-day realized volatility from close prices."""
    date_idx = {d: i for i, d in enumerate(trading_dates)}
    vol_map = {}
    for ymd in rebalance_ymds:
        idx = date_idx.get(ymd)
        if idx is None or idx < window:
            vol_map[ymd] = {}
            continue
        window_dates = trading_dates[idx - window:idx + 1]
        code_vols = {}
        for code, px_dict in prices.items():
            px_series = []
            for d in window_dates:
                p = px_dict.get(d)
                if p and p > 0:
                    px_series.append(p)
            if len(px_series) < window // 2:
                continue
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
    con = connect()
    rows = con.execute(
        "SELECT DISTINCT trade_date FROM valuation_daily "
        "WHERE trade_date >= ? AND trade_date <= ? ORDER BY trade_date",
        [_ymd_to_dash(start_ymd), _ymd_to_dash(end_ymd)]
    ).fetchall()
    con.close()
    return [r[0].replace("-", "") for r in rows]


def fetch_prices_from_db(trading_dates):
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
    con = connect()
    dates_fmt = [_ymd_to_dash(d) for d in rebalance_dates]
    placeholders = ",".join(["?"] * len(dates_fmt))
    rows = con.execute(
        f"SELECT v.code, v.trade_date, v.conv_prem_pct, v.outstanding_yi, v.pe_ttm, "
        f"  vd.vol_20d_pct, v.bs_delta, v.relative_value, v.surplus_years, v.maturity_call_price, "
        f"  v.pure_bond_value "
        f"FROM valuation_daily v "
        f"LEFT JOIN universe u ON v.code = u.code "
        f"LEFT JOIN vol_daily vd ON u.ucode = vd.ucode AND v.trade_date = vd.trade_date "
        f"WHERE v.trade_date IN ({placeholders})",
        dates_fmt
    ).fetchall()
    con.close()
    fundamentals = defaultdict(dict)
    for code, td, conv_prem, balance, pe_ttm, vol_20d, bs_delta, rel_val, surplus_yr, mcp, pbv in rows:
        ymd = td.replace("-", "")
        fundamentals.setdefault(ymd, {})[code] = {
            "conv_prem": conv_prem,
            "balance": balance,
            "pe_ttm": pe_ttm,
            "vol_20d": vol_20d,
            "bs_delta": bs_delta,
            "relative_value": rel_val,
            "surplus_years": surplus_yr,
            "maturity_call_price": mcp,
            "pure_bond_value": pbv,
        }
    return fundamentals


def persist_prices_to_db(prices, codes_set):
    con = connect()
    n_written = 0
    for code, date_prices in prices.items():
        if code not in codes_set:
            continue
        for ymd, px in date_prices.items():
            try:
                con.execute(
                    "INSERT INTO valuation_daily (trade_date, code, price) VALUES (?, ?, ?) "
                    "ON CONFLICT (trade_date, code) DO UPDATE SET price = "
                    "CASE WHEN valuation_daily.price IS NULL THEN excluded.price ELSE valuation_daily.price END",
                    [_ymd_to_dash(ymd), code, px]
                )
                n_written += 1
            except Exception:
                pass
    con.close()
    return n_written


def persist_fundamentals_to_db(fundamentals):
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


# ── BS formulas ──────────────────────────────────────────────────────

def classify_sector(delta):
    """Classify by BS Delta: 偏股≥0.6, 平衡0.3–0.6, 偏债<0.3. Matches strategy_score.py."""
    if delta is None:
        return "偏债"
    if delta >= 0.6:
        return "偏股"
    if delta >= 0.3:
        return "平衡"
    return "偏债"


def _compute_bs_delta(price, conv_prem, vol_20d, surplus_years=None, maturity_call=None):
    """Compute BS delta when bs_delta unavailable in DB."""
    if price is None or conv_prem is None:
        return None
    if price <= 0 or conv_prem <= -90:
        return None
    S = price / (1.0 + conv_prem / 100.0)
    K = maturity_call if maturity_call and maturity_call > 0 else 110.0
    if vol_20d and vol_20d > 0:
        sigma = vol_20d / 100.0 if vol_20d > 1.5 else vol_20d
    else:
        sigma = 0.25
    T = surplus_years if surplus_years and surplus_years > 0.01 else 3.0
    r = 0.025
    sqrtT = math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * sqrtT)
    return 0.5 * (1 + math.erf(d1 / math.sqrt(2)))


def _compute_relative_value(price, conv_prem, vol_20d, surplus_years=None,
                             maturity_call=None, pure_bond_value=None):
    """Compute relative_value = price / bs_value when not available in DB."""
    if price is None or conv_prem is None or price <= 0:
        return None
    S = price / (1.0 + conv_prem / 100.0)
    K = maturity_call if maturity_call and maturity_call > 0 else 110.0
    if vol_20d and vol_20d > 0:
        sigma = vol_20d / 100.0 if vol_20d > 1.5 else vol_20d
    else:
        sigma = 0.25
    T = surplus_years if surplus_years and surplus_years > 0.01 else 3.0
    r = 0.025
    sqrtT = math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * sqrtT)
    d2 = d1 - sigma * sqrtT
    nd1 = 0.5 * (1 + math.erf(d1 / math.sqrt(2)))
    nd2 = 0.5 * (1 + math.erf(d2 / math.sqrt(2)))
    call_value = S * nd1 - K * math.exp(-r * T) * nd2
    pbv = pure_bond_value if pure_bond_value and pure_bond_value > 0 else K * math.exp(-r * T)
    bs_value = pbv + call_value
    if bs_value <= 0:
        return None
    return price / bs_value


def _percentile(sorted_vals, pct):
    if not sorted_vals:
        return 0
    k = (len(sorted_vals) - 1) * pct / 100
    lo = int(k)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = k - lo
    return sorted_vals[lo] + frac * (sorted_vals[hi] - sorted_vals[lo])


# ── Universe filter ──────────────────────────────────────────────────

def filter_universe(bonds, min_balance=MIN_BALANCE_YI, max_price=MAX_PRICE):
    """Pre-filter: liquidity + price cap + basic data completeness."""
    out = []
    for b in bonds:
        if b.get("conv_prem") is None:
            continue
        px = b.get("price")
        if not px or px <= 0 or px > max_price:
            continue
        bal = b.get("balance")
        if not bal or bal < min_balance:
            continue
        out.append(b)
    return out


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


# ── Strategy functions (pure: no side effects) ───────────────────────

def _rank_double_low(bonds):
    """Compute double-low ranking. Returns sorted list of (code, dl_score)."""
    eligible = _apply_pe_vol_filter(bonds)
    if not eligible:
        return []
    by_cp = sorted(eligible, key=lambda x: x["conv_prem"])
    by_px = sorted(eligible, key=lambda x: x["price"])
    cp_rank = {b["code"]: i + 1 for i, b in enumerate(by_cp)}
    px_rank = {b["code"]: i + 1 for i, b in enumerate(by_px)}
    scored = []
    for b in eligible:
        dl = 1.5 * cp_rank[b["code"]] + px_rank[b["code"]]
        scored.append((b["code"], dl))
    scored.sort(key=lambda x: x[1])
    return scored


def select_double_low(bonds, top_n=10):
    """Classic double-low: return list of codes (top N)."""
    ranked = _rank_double_low(bonds)
    codes = [code for code, _ in ranked[:top_n]]
    return codes if len(codes) >= MIN_HOLDINGS else []


def select_sector_neutral(bonds, per_sector=10):
    """Sector-neutral double-low: top N per delta sector.

    Returns dict {"equity": [codes], "balanced": [codes], "debt": [codes]}.
    """
    by_sector = defaultdict(list)
    for b in bonds:
        sec = classify_sector(b.get("delta"))
        by_sector[sec].append(b)

    result = {}
    key_map = {"偏股": "equity", "平衡": "balanced", "偏债": "debt"}
    for sec_cn, sec_en in key_map.items():
        sector_bonds = by_sector.get(sec_cn, [])
        ranked = _rank_double_low(sector_bonds)
        codes = [code for code, _ in ranked[:per_sector]]
        result[sec_en] = codes if len(codes) >= MIN_HOLDINGS else []
    return result


def select_low_rv(bonds, top_n=10):
    """Low relative-value strategy: return list of codes."""
    eligible = _apply_pe_vol_filter(bonds)
    eligible = [b for b in eligible if b.get("relative_value") is not None]
    if not eligible:
        return []
    eligible.sort(key=lambda x: x["relative_value"])
    codes = [b["code"] for b in eligible[:top_n]]
    return codes if len(codes) >= MIN_HOLDINGS else []


# ── Portfolio (per-position tracking) ────────────────────────────────

class Portfolio:
    """Equal-weight portfolio with correct per-position cost tracking.

    Cost model:
    - Buy cost  = slippage + half_commission (charged on buy price)
    - Sell cost = slippage + half_commission (charged on sell price)
    - Only turnover positions pay costs; held-over positions pay nothing.

    Each period:
    1. Receive new picks (list of codes).
    2. Determine which codes can actually be bought (have buy_px).
    3. Equal-weight across actual holdings.
    4. Compute per-position return:
       - Gross return = (sell_px - buy_px) / buy_px
       - New position:  net = gross - buy_cost - sell_cost
       - Continuing + exiting: net = gross - sell_cost  (if exiting)
       - Continuing + staying: net = gross  (no cost)
    5. Portfolio return = mean of per-position net returns.
    """

    def __init__(self, slippage_bps=SLIPPAGE_BPS, commission_bps=COMMISSION_BPS):
        self.one_way_cost = slippage_bps / 10000 + commission_bps / 20000
        self.holdings = set()  # codes held entering this period
        self.equity = 1.0      # cumulative equity

    def rebalance(self, picks, buy_px, sell_px):
        """Execute one rebalance cycle.

        Returns:
            period_return (float or None if no valid holdings),
            n_held (int),
            turnover_rate (float: fraction of portfolio that changed)
        """
        actual = [c for c in picks if buy_px.get(c) and buy_px[c] > 0]
        if len(actual) < MIN_HOLDINGS:
            return None, 0, 0.0

        new_set = set(actual)
        prev_set = self.holdings

        pos_returns = []
        for code in actual:
            pb = buy_px[code]
            ps = sell_px.get(code)
            gross = (ps - pb) / pb if (ps and ps > 0) else 0.0
            cost = 0.0
            if code not in prev_set:
                cost += self.one_way_cost
            pos_returns.append(gross - cost)

        period_ret = sum(pos_returns) / len(pos_returns)

        exiting = prev_set - new_set
        if prev_set and exiting:
            exit_drag = (len(exiting) / len(prev_set)) * self.one_way_cost
            period_ret -= exit_drag

        if prev_set:
            changed = len(exiting | (new_set - prev_set))
            turnover = changed / max(len(prev_set | new_set), 1)
        else:
            turnover = 1.0

        self.holdings = new_set
        self.equity *= (1 + period_ret)
        self.equity = max(self.equity, 0.0)

        return period_ret, len(actual), turnover


# ── Risk metrics ─────────────────────────────────────────────────────

def compute_risk_metrics(equity_series, n_trading_days):
    """Compute Sharpe, MaxDD, and annualized return from a list of equity values."""
    if len(equity_series) < 2:
        return {"cum_return": 0, "ann_return": 0, "sharpe": 0, "max_drawdown": 0}

    cum_ret = equity_series[-1] / equity_series[0] - 1

    if n_trading_days > 0 and equity_series[-1] > 0:
        ann_ret = (equity_series[-1] ** (252 / n_trading_days) - 1)
    else:
        ann_ret = 0

    period_rets = []
    for i in range(1, len(equity_series)):
        if equity_series[i - 1] > 0:
            period_rets.append(equity_series[i] / equity_series[i - 1] - 1)

    if len(period_rets) >= 2:
        avg_ret = np.mean(period_rets)
        std_ret = np.std(period_rets, ddof=1)
        if std_ret > 0:
            periods_per_year = 252 / max(n_trading_days / len(period_rets), 1)
            sharpe = (avg_ret / std_ret) * math.sqrt(periods_per_year)
        else:
            sharpe = 0
    else:
        sharpe = 0

    peak = equity_series[0]
    max_dd = 0
    for eq in equity_series:
        if eq > peak:
            peak = eq
        dd = (peak - eq) / peak if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd

    return {
        "cum_return": cum_ret,
        "ann_return": ann_ret,
        "sharpe": round(sharpe, 3),
        "max_drawdown": round(max_dd, 4),
    }


# ── Main backtest engine ─────────────────────────────────────────────

def load_universe_codes(start_ymd, end_ymd):
    """Load CB codes and underlying stock mapping from DB."""
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

    code_ucode_rows = con.execute("SELECT code, ucode FROM universe WHERE ucode IS NOT NULL").fetchall()
    con.close()
    code_to_ucode = {r[0]: r[1] for r in code_ucode_rows}
    print(f"[mapping] {len(code_to_ucode)} CB -> underlying stock mappings")
    return codes, code_to_ucode


def load_prices_and_dates(args, codes, start_ymd, end_ymd):
    """Load prices and derive trading dates."""
    if args.from_db:
        print("[fetch] reading trading dates from DB...")
        trading_dates = fetch_trading_dates_from_db(start_ymd, end_ymd)
        if len(trading_dates) < 2:
            print(f"[error] only {len(trading_dates)} trading dates found, need >= 2")
            return None, None
        print("[fetch] reading prices from DB...")
        prices = fetch_prices_from_db(trading_dates)
    else:
        print(f"[fetch] pulling historical prices for {len(codes)} bonds...")
        prices = fetch_history_prices(codes, start_ymd, end_ymd)
        all_dates = set()
        for code_dates in prices.values():
            all_dates.update(code_dates.keys())
        trading_dates = sorted(d for d in all_dates if start_ymd <= d <= end_ymd)

    if len(trading_dates) < 2:
        print(f"[error] only {len(trading_dates)} trading dates found, need >= 2")
        return None, None

    print(f"[dates] {len(trading_dates)} trading days: {trading_dates[0]} -> {trading_dates[-1]}")
    total_px = sum(len(v) for v in prices.values())
    print(f"[prices] {total_px} price points for {len(prices)} bonds")
    return trading_dates, prices


def compute_rebalance_schedule(args, trading_dates, start_ymd, end_ymd):
    """Compute rebalance indices into trading_dates."""
    holding = args.holding_days

    if args.rebalance == "weekly":
        if args.from_db:
            con = connect()
            fund_rows = con.execute(
                "SELECT DISTINCT trade_date FROM valuation_daily "
                "WHERE conv_prem_pct IS NOT NULL AND trade_date >= ? AND trade_date <= ? "
                "ORDER BY trade_date",
                [_ymd_to_dash(start_ymd), _ymd_to_dash(end_ymd)]
            ).fetchall()
            con.close()
            fund_dates_set = set(r[0].replace("-", "") for r in fund_rows)
            date_idx = {d: i for i, d in enumerate(trading_dates)}

            rebalance_indices = []
            last_rb = -holding - 1
            for d in sorted(fund_dates_set):
                idx = date_idx.get(d)
                if idx is not None and idx - last_rb >= holding:
                    rebalance_indices.append(idx)
                    last_rb = idx
            print(f"[rebalance] snapped to {len(rebalance_indices)} DB dates with conv_prem")
        else:
            rebalance_indices = list(range(0, len(trading_dates) - holding, holding))
            if not rebalance_indices or rebalance_indices[-1] + holding < len(trading_dates) - 1:
                rebalance_indices.append(rebalance_indices[-1] + holding if rebalance_indices else 0)
    else:
        rebalance_indices = list(range(len(trading_dates) - 1))
        holding = 1

    print(f"[rebalance] {args.rebalance} mode, {len(rebalance_indices)} rebalance points, holding {holding} days")
    return rebalance_indices, holding


def load_fundamentals(args, codes, codes_set, code_to_ucode, trading_dates,
                      rebalance_ymds, prices, start_ymd, end_ymd):
    """Load and enrich fundamentals data."""
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
                print("[vol] computing 20-day realized volatility from prices...")
                vol_map = compute_vol_from_prices(prices, trading_dates, rebalance_ymds)
                merge_vol_into_fundamentals(fundamentals, vol_map)
    else:
        print(f"[fetch] pulling fundamentals for {len(rebalance_date_list)} rebalance dates...")
        # Pre-load ucode map and vol DB for aligned PE/vol sourcing
        ucode_map = _load_ucode_map() or {v: k for k, v in code_to_ucode.items() if v}
        vol_db = _load_vol_db()
        fundamentals = {}
        for i, td in enumerate(rebalance_date_list):
            fund = fetch_day_fundamentals(codes, td, ucode_map, vol_db)
            fundamentals[td] = fund
            n_pe = sum(1 for v in fund.values() if v.get("pe_ttm") is not None)
            n_vol = sum(1 for v in fund.values() if v.get("vol_20d") is not None)
            print(f"  [{i+1}/{len(rebalance_date_list)}] {td}: {len(fund)} conv_prem, {n_pe} PE, {n_vol} vol")

        print("[persist] saving fetched data to DB...")
        n_px = persist_prices_to_db(prices, codes_set)
        n_fund = persist_fundamentals_to_db(fundamentals)
        print(f"[persist] wrote {n_px} price rows + {n_fund} fundamental rows to DB")

        # Supplement with bulk PE if still sparse
        if basic_data is not None:
            pe_map = fetch_underlying_pe_bulk(code_to_ucode, start_ymd, end_ymd)
            merge_pe_into_fundamentals(fundamentals, code_to_ucode, pe_map)

        print("[vol] computing 20-day realized volatility from prices...")
        vol_map = compute_vol_from_prices(prices, trading_dates, rebalance_ymds)
        merge_vol_into_fundamentals(fundamentals, vol_map)

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

    return fundamentals


def load_benchmark(start_ymd, end_ymd, trading_dates):
    """Load CSI convertible bond index (000832.CSI) or fall back to equal-weight."""
    bench_prices = {}
    if history is not None:
        print("[bench] fetching CSI convertible index (000832.CSI)...")
        try:
            r = history("000832.CSI", "close",
                        _ymd_to_dash(start_ymd), _ymd_to_dash(end_ymd))
            for t in r.get("tables", []):
                dates_raw = t.get("time", [])
                closes = t.get("table", {}).get("close", [])
                for i, d in enumerate(dates_raw):
                    if d and d != "-":
                        ymd = d.replace("-", "")
                        v = _safe_float(closes, i)
                        if v and v > 0:
                            bench_prices[ymd] = v
            print(f"  {len(bench_prices)} data points")
        except Exception as e:
            print(f"  [warn] benchmark fetch failed: {e}")

    use_eq_weight = not bench_prices
    if use_eq_weight:
        print("[bench] CSI index unavailable, using equal-weight market return as proxy")

    bench_base = None
    if bench_prices:
        for d in trading_dates:
            if d in bench_prices:
                bench_base = bench_prices[d]
                break

    return bench_prices, bench_base, use_eq_weight


def build_day_bonds(prices, fund, td_select):
    """Build bond snapshot list for a single rebalance date."""
    day_bonds = []
    for code in prices:
        px = prices[code].get(td_select)
        if not px:
            continue
        f = fund.get(code, {})
        raw_delta = f.get("bs_delta")
        if raw_delta is None:
            raw_delta = _compute_bs_delta(
                px, f.get("conv_prem"), f.get("vol_20d"),
                f.get("surplus_years"), f.get("maturity_call_price"))
        rv = f.get("relative_value")
        if rv is None:
            rv = _compute_relative_value(
                px, f.get("conv_prem"), f.get("vol_20d"),
                f.get("surplus_years"), f.get("maturity_call_price"),
                f.get("pure_bond_value"))
        day_bonds.append({
            "code": code,
            "price": px,
            "conv_prem": f.get("conv_prem"),
            "balance": f.get("balance"),
            "pe_ttm": f.get("pe_ttm"),
            "vol_20d": f.get("vol_20d"),
            "delta": raw_delta,
            "relative_value": rv,
        })
    return day_bonds


def run_backtest_loop(args, trading_dates, rebalance_indices, holding,
                      prices, fundamentals, bench_prices, bench_base, use_eq_weight_bench):
    """Core backtest loop. Returns (results, portfolios, equity_history, bench_equity)."""
    STRATEGIES = ["dl", "equity", "balanced", "debt", "rv"]
    LABELS = {
        "dl": "双低Top",
        "equity": "偏股双低",
        "balanced": "平衡双低",
        "debt": "偏债双低",
        "rv": "低估Top",
        "bench": "中证转债" if not use_eq_weight_bench else "全市场等权",
    }

    portfolios = {k: Portfolio(args.slippage_bps, args.commission_bps) for k in STRATEGIES}
    bench_equity = 1.0
    results = []
    equity_history = {k: [1.0] for k in STRATEGIES}
    equity_history["bench"] = [1.0]
    turnover_history = {k: [] for k in STRATEGIES}

    for rb_idx, rb_i in enumerate(rebalance_indices):
        td_select = trading_dates[rb_i]
        td_buy = trading_dates[min(rb_i + 1, len(trading_dates) - 1)]
        if rb_idx + 1 < len(rebalance_indices):
            next_rb = rebalance_indices[rb_idx + 1]
            td_sell = trading_dates[min(next_rb + 1, len(trading_dates) - 1)]
        else:
            td_sell = trading_dates[-1]

        fund = fundamentals.get(td_select, {})
        day_bonds = build_day_bonds(prices, fund, td_select)
        filtered = filter_universe(day_bonds)

        dl_codes = select_double_low(filtered, top_n=args.top)
        sn_picks = select_sector_neutral(filtered, per_sector=args.top)
        rv_codes = select_low_rv(filtered, top_n=args.top)

        pick_map = {
            "dl": dl_codes,
            "equity": sn_picks.get("equity", []),
            "balanced": sn_picks.get("balanced", []),
            "debt": sn_picks.get("debt", []),
            "rv": rv_codes,
        }

        buy_px = {code: prices[code].get(td_buy) for code in prices if td_buy in prices[code]}
        sell_px = {code: prices[code].get(td_sell) for code in prices if td_sell in prices[code]}

        ret = {}
        n_held = {}
        any_valid = False
        for k in STRATEGIES:
            r_val, n_val, turnover = portfolios[k].rebalance(pick_map[k], buy_px, sell_px)
            ret[k] = r_val
            n_held[k] = n_val
            if r_val is not None:
                any_valid = True
                equity_history[k].append(portfolios[k].equity)
                turnover_history[k].append(turnover)

        if not any_valid:
            continue

        if use_eq_weight_bench:
            n_mkt, sum_mkt = 0, 0.0
            for code in prices:
                pb = buy_px.get(code)
                ps = sell_px.get(code)
                if pb and pb > 0 and ps and ps > 0:
                    sum_mkt += (ps - pb) / pb
                    n_mkt += 1
            if n_mkt > 0:
                bench_equity *= (1.0 + sum_mkt / n_mkt)
        else:
            bp = bench_prices.get(td_sell)
            if bp and bench_base:
                bench_equity = bp / bench_base
        equity_history["bench"].append(bench_equity)

        pt = {"date": td_sell}
        for k in STRATEGIES:
            pt[f"cum_{k}"] = round(portfolios[k].equity - 1, 6)
        pt["cum_bench"] = round(bench_equity - 1, 6)
        results.append(pt)

        parts = []
        for k in STRATEGIES:
            r_val = ret[k]
            if r_val is not None:
                parts.append(f"{LABELS[k]}={r_val*100:+.2f}%(n={n_held[k]})")
            else:
                parts.append(f"{LABELS[k]}=N/A")
        print(f"  {td_select}: {' | '.join(parts)}")

    return results, portfolios, equity_history, turnover_history, bench_equity, STRATEGIES, LABELS


def dedup_equity(curve):
    """Remove duplicate dates. Keep last occurrence."""
    seen = {}
    for i, pt in enumerate(curve):
        seen[pt["date"]] = i
    last_indices = sorted(seen.values())
    return [curve[i] for i in last_indices]


def print_summary(args, results, equity_history, turnover_history, trading_dates,
                  holding, strategies, labels, use_eq_weight_bench):
    """Print backtest summary with risk metrics."""
    results = dedup_equity(results)
    if not results:
        print("[error] No valid backtest periods (all N/A). Check data completeness.")
        return None

    actual_start = results[0]["date"]
    actual_end = results[-1]["date"]
    n_actual_days = len([d for d in trading_dates if actual_start <= d <= actual_end])

    ALL_CURVES = strategies + ["bench"]

    print(f"\n{'='*78}")
    print(f"回测区间: {actual_start} -> {actual_end} ({n_actual_days} 个交易日)")
    print(f"调仓频率: {args.rebalance} (共{len(results)}次有效调仓, T日选券->T+1买入->下次调仓卖出)")
    print(f"交易成本: 滑点{args.slippage_bps}bps(单边)+佣金{args.commission_bps}bps(往返), 仅换手部分收费")
    print(f"Universe: 余额>={MIN_BALANCE_YI}亿 + 价格<={MAX_PRICE}元 + PE>0 + vol>Q1")
    print(f"最少持仓: {MIN_HOLDINGS}只 (不足则该策略当期N/A)")
    print(f"分域标准: 偏股(Delta>=0.6) 平衡(0.3<=Delta<0.6) 偏债(Delta<0.3)")
    print(f"停牌处理: 无卖出价按0%收益计入")
    print(f"{'='*78}")
    print(f"{'策略':<16} {'累计收益':>10} {'年化':>10} {'Sharpe':>8} {'最大回撤':>10} {'平均换手':>10}")
    print(f"{'-'*66}")

    summary_data = {}
    for k in ALL_CURVES:
        eq_series = equity_history.get(k, [1.0])
        metrics = compute_risk_metrics(eq_series, n_actual_days)

        avg_turnover = 0
        if k in turnover_history and turnover_history[k]:
            avg_turnover = sum(turnover_history[k]) / len(turnover_history[k])

        label = labels.get(k, k) + (str(args.top) if k in ("dl", "rv") else "")
        cum_str = f"{metrics['cum_return']*100:+.2f}%"
        ann_str = f"{metrics['ann_return']*100:+.1f}%"
        sharpe_str = f"{metrics['sharpe']:.2f}"
        dd_str = f"{metrics['max_drawdown']*100:.1f}%"
        to_str = f"{avg_turnover*100:.0f}%" if k != "bench" else "-"

        print(f"{label:<16} {cum_str:>10} {ann_str:>10} {sharpe_str:>8} {dd_str:>10} {to_str:>10}")

        summary_data[k] = {
            "cum_return_pct": round(metrics["cum_return"] * 100, 3),
            "annualized_pct": round(metrics["ann_return"] * 100, 1),
            "sharpe": metrics["sharpe"],
            "max_drawdown_pct": round(metrics["max_drawdown"] * 100, 2),
            "avg_turnover_pct": round(avg_turnover * 100, 1) if k != "bench" else None,
        }

    return {
        "actual_start": actual_start,
        "actual_end": actual_end,
        "n_actual_days": n_actual_days,
        "results": results,
        "summary_data": summary_data,
    }


def save_output(args, summary_info, end_ymd, strategies, use_eq_weight_bench, holding):
    """Save backtest results to JSON."""
    ALL_CURVES = strategies + ["bench"]
    out_dir = f"data/raw/asof={end_ymd}"
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{out_dir}/backtest_weekly.json"

    output = {
        "start_date": summary_info["actual_start"],
        "end_date": summary_info["actual_end"],
        "trading_days": summary_info["n_actual_days"],
        "rebalance": args.rebalance,
        "holding_days": holding,
        "n_rebalances": len(summary_info["results"]),
        "slippage_bps": args.slippage_bps,
        "commission_bps": args.commission_bps,
        "sector_method": "delta",
        "min_balance_yi": MIN_BALANCE_YI,
        "max_price": MAX_PRICE,
        "min_holdings": MIN_HOLDINGS,
        "benchmark": "000832.CSI" if not use_eq_weight_bench else "equal_weight",
    }
    for k in ALL_CURVES:
        sd = summary_info["summary_data"].get(k, {})
        output[f"cum_return_{k}_pct"] = sd.get("cum_return_pct", 0)
        output[f"annualized_{k}_pct"] = sd.get("annualized_pct", 0)
        output[f"sharpe_{k}"] = sd.get("sharpe", 0)
        output[f"max_drawdown_{k}_pct"] = sd.get("max_drawdown_pct", 0)
    output["equity_curve"] = summary_info["results"]

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n[done] -> {out_path}")
    return out_path


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
    print(f"[filter] balance>={MIN_BALANCE_YI}yi, price<={MAX_PRICE}, min_holdings={MIN_HOLDINGS}")

    if not args.from_db and basic_data is None:
        print("[warn] iFinD not available, falling back to --from-db mode")
        args.from_db = True

    codes, code_to_ucode = load_universe_codes(start_ymd, end_ymd)
    codes_set = set(codes)

    trading_dates, prices = load_prices_and_dates(args, codes, start_ymd, end_ymd)
    if trading_dates is None:
        return

    rebalance_indices, holding = compute_rebalance_schedule(args, trading_dates, start_ymd, end_ymd)
    rebalance_ymds = set(trading_dates[i] for i in rebalance_indices)

    fundamentals = load_fundamentals(
        args, codes, codes_set, code_to_ucode, trading_dates,
        rebalance_ymds, prices, start_ymd, end_ymd)

    bench_prices, bench_base, use_eq_weight_bench = load_benchmark(start_ymd, end_ymd, trading_dates)

    (results, portfolios, equity_history, turnover_history,
     bench_equity, strategies, labels) = run_backtest_loop(
        args, trading_dates, rebalance_indices, holding,
        prices, fundamentals, bench_prices, bench_base, use_eq_weight_bench)

    summary_info = print_summary(
        args, results, equity_history, turnover_history,
        trading_dates, holding, strategies, labels, use_eq_weight_bench)
    if summary_info is None:
        return

    save_output(args, summary_info, end_ymd, strategies, use_eq_weight_bench, holding)


if __name__ == "__main__":
    main()
