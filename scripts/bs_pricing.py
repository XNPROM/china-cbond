"""BS pricing model for convertible bonds.

Uses iFinD-provided pure bond value and maturity call price for accuracy.
Computes: BS call option + pure bond value = theoretical value.
Also computes Greek letters (delta, gamma, theta, vega).

Key improvement over naive BS:
- Uses actual pure bond value from iFinD (not K*exp(-rT))
- Uses actual maturity call price per bond (not fixed 110)
- Conversion value S derived from price and premium

Inputs (from dataset.json):
  - latest: bond price
  - conv_prem: conversion premium rate (%)
  - vol_20d: 20-day annualized volatility (decimal)
  - pure_bond_ytm: pure bond yield to maturity (%)
  - surplus_years: remaining term (years)
  - pure_bond_value: pure bond value from iFinD
  - maturity_call_price: maturity redemption price from iFinD

Usage:
  python3 scripts/bs_pricing.py \
      --dataset data/raw/asof=2026-04-23/dataset.json \
      --trade-date 2026-04-23
"""
import argparse, json, math, os, sys

sys.path.insert(0, os.path.dirname(__file__))
from _db import connect, init_schema, upsert as db_upsert


def _norm_cdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def _norm_pdf(x):
    return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)


def bs_call(S, K, sigma, r, T):
    if T <= 0.01 or sigma <= 0 or S <= 0 or K <= 0:
        return max(S - K, 0), 0.0, 0.0, 0.0, 0.0

    sqrtT = math.sqrt(T)
    d = sigma * sqrtT
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / d
    d2 = d1 - d

    Nd1 = _norm_cdf(d1)
    Nd2 = _norm_cdf(d2)
    nd1 = _norm_pdf(d1)
    KrT = K * math.exp(-r * T)

    call = S * Nd1 - KrT * Nd2
    delta = Nd1
    gamma = nd1 / (S * d)
    vega = S * sqrtT * nd1 / 100  # per 1% vol change
    theta = (-sigma * S * nd1 / (2 * sqrtT) - r * KrT * Nd2) / 365  # per day

    return call, delta, gamma, theta, vega


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--trade-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--default-r", type=float, default=0.025)
    args = ap.parse_args()

    if not os.path.exists(args.dataset):
        print(f"[error] Dataset file not found: {args.dataset}")
        sys.exit(1)

    try:
        dataset = json.load(open(args.dataset, encoding="utf-8"))
    except json.JSONDecodeError as e:
        print(f"[error] Invalid JSON in dataset: {e}")
        sys.exit(1)

    items = dataset.get("items")
    if not items:
        print("[error] Dataset has no items")
        sys.exit(1)

    results = []
    priced = 0
    skipped_missing = 0
    skipped_invalid = 0
    skipped_err = 0

    for it in items:
        price = it.get("latest")
        conv_prem = it.get("conv_prem")
        vol_20d = it.get("vol_20d")
        ytm = it.get("pure_bond_ytm")
        surplus_years = it.get("surplus_years")
        pure_bond_val = it.get("pure_bond_value")
        maturity_call = it.get("maturity_call_price")

        if not all(v is not None for v in [price, conv_prem, vol_20d]):
            results.append(None)
            skipped_missing += 1
            continue
        if price <= 0 or conv_prem <= -90 or vol_20d <= 0:
            results.append(None)
            skipped_invalid += 1
            continue

        # Conversion value S
        # conv_prem = (price / conv_value - 1) * 100
        # So conv_value = price / (1 + conv_prem / 100)
        S = price / (1 + conv_prem / 100)

        # Strike: use actual maturity call price if available, else 110
        K = maturity_call if maturity_call and maturity_call > 0 else 110.0

        # Volatility
        sigma = vol_20d

        # Discount rate: use risk-free rate (CGB 5Y or default 2.5%).
        # Do NOT use YTM — it includes credit spread, violating BS risk-free assumption.
        r = args.default_r

        # Time to maturity
        T = surplus_years if surplus_years and surplus_years > 0.01 else 2.0

        try:
            option_val, delta, gamma, theta, vega = bs_call(S, K, sigma, r, T)
        except (ValueError, ZeroDivisionError, OverflowError):
            results.append(None)
            skipped_err += 1
            continue

        # Pure bond value: use iFinD value if available.
        # If unavailable, skip this bond — K*exp(-rT) ignores coupons
        # and credit spread, producing unreliable bs_value.
        if pure_bond_val and pure_bond_val > 0:
            pbv = pure_bond_val
        else:
            results.append(None)
            skipped_missing += 1
            continue

        total_val = option_val + pbv
        rel_val = price / total_val if total_val > 0 else None

        results.append({
            "trade_date": args.trade_date,
            "code": it["code"],
            "bs_value": round(total_val, 2),
            "relative_value": round(rel_val, 4) if rel_val else None,
            "bs_delta": round(delta, 4),
            "bs_gamma": round(gamma, 4),
            "bs_theta": round(theta, 4),
            "bs_vega": round(vega, 4),
        })
        priced += 1

    db_rows = [r for r in results if r is not None]
    if db_rows:
        try:
            con = connect()
            init_schema(con)
            n = db_upsert(con, "valuation_daily", db_rows, ["trade_date", "code"])
            con.close()
            print(f"[db] valuation_daily BS fields upserted for {n} rows")
        except Exception as e:
            print(f"[error] Failed to upsert to DB: {e}")

    # Also write BS fields back into dataset.json (avoids needing a 2nd assemble run)
    bs_map = {r["code"]: r for r in db_rows if r}
    for it in items:
        bs = bs_map.get(it["code"])
        if bs:
            it["bs_value"] = bs["bs_value"]
            it["relative_value"] = bs["relative_value"]
            it["bs_delta"] = bs["bs_delta"]
            it["bs_gamma"] = bs["bs_gamma"]
            it["bs_theta"] = bs["bs_theta"]
            it["bs_vega"] = bs["bs_vega"]
    try:
        with open(args.dataset, "w", encoding="utf-8") as f:
            json.dump(dataset, f, ensure_ascii=False, indent=2)
        print(f"[json] dataset.json updated with BS fields in-place")
    except IOError as e:
        print(f"[error] Failed to write dataset.json: {e}")

    # Stats
    rv_vals = [r["relative_value"] for r in db_rows if r and r.get("relative_value") is not None]
    delta_vals = [r["bs_delta"] for r in db_rows if r and r.get("bs_delta") is not None]
    def _median(vals):
        s = sorted(vals)
        n = len(s)
        if n % 2 == 1:
            return s[n // 2]
        return (s[n // 2 - 1] + s[n // 2]) / 2

    if rv_vals:
        print(f"[stats] relative_value: median={_median(rv_vals):.2f}, "
              f"<1.0:{sum(1 for v in rv_vals if v<1.0)}, "
              f"1.0-1.2:{sum(1 for v in rv_vals if 1.0<=v<1.2)}, "
              f">1.2:{sum(1 for v in rv_vals if v>=1.2)}")
    if delta_vals:
        print(f"[stats] delta: median={_median(delta_vals):.3f}, "
              f"<0.1:{sum(1 for v in delta_vals if v<0.1)}, "
              f"0.1-0.5:{sum(1 for v in delta_vals if 0.1<=v<0.5)}, "
              f">0.5:{sum(1 for v in delta_vals if v>=0.5)}")

    print(f"[done] BS priced {priced}/{len(items)} bonds "
          f"(skip_missing={skipped_missing}, skip_invalid={skipped_invalid}, skip_err={skipped_err})")


if __name__ == "__main__":
    main()
