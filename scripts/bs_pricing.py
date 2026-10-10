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
  - vol_20d: 20-day annualized volatility (percentage, e.g. 34.52 means 34.52%)
  - conversion_end_date: actual option expiry; term computed from report date
  - surplus_years: fallback remaining term only when no expiry date is provided
  - pure_bond_value: pure bond value from iFinD
  - maturity_call_price: maturity redemption price from iFinD

Usage:
  python3 scripts/bs_pricing.py \
      --dataset data/raw/asof=2026-04-23/dataset.json \
      --trade-date 2026-04-23
"""
import argparse, json, math, os, sys

sys.path.insert(0, os.path.dirname(__file__))
from _db import connect, upsert as db_upsert
from _snapshot_policy import finite_number


def _norm_cdf(x):
    return 0.5 * math.erfc(-x / math.sqrt(2))


def _norm_pdf(x):
    return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)


MODEL_VERSION = 'bs-hv-3'


def bs_call(S, K, sigma, r, T):
    if not all(finite_number(x) for x in (S, K, sigma, r, T)):
        raise ValueError('non-finite option input')
    if S < 0 or K <= 0 or sigma < 0 or T < 0:
        raise ValueError('invalid option input')
    if S == 0:
        return 0.0, 0.0, 0.0, 0.0, 0.0
    if T == 0:
        delta = 1.0 if S > K else (0.0 if S < K else None)
        return max(S-K, 0.0), delta, None, None, 0.0
    KrT = K * math.exp(-r*T)
    if sigma == 0:
        itm = S > KrT
        delta = 1.0 if itm else (0.0 if S < KrT else None)
        return max(S-KrT, 0.0), delta, (0.0 if delta is not None else None), (-r*KrT/365 if itm else (0.0 if delta is not None else None)), (0.0 if delta is not None else S*math.sqrt(T)*_norm_pdf(0)/100)
    sqrtT = math.sqrt(T)
    d = sigma * sqrtT
    d1 = (math.log(S/K) + (r + 0.5*sigma*sigma)*T) / d
    d2 = d1-d
    Nd1, Nd2, nd1 = _norm_cdf(d1), _norm_cdf(d2), _norm_pdf(d1)
    call = max(0.0, S*Nd1 - KrT*Nd2)
    return call, Nd1, nd1/(S*d), (-sigma*S*nd1/(2*sqrtT)-r*KrT*Nd2)/365, S*sqrtT*nd1/100


def bond_metrics(item, r=0.025, asof=None):
    """One percentage-unit model for reports and backtests; no invented inputs."""
    from datetime import date
    from _lifecycle import iso_date, lifecycle, pricing_exclusion
    if asof and item.get('code'):
        if pricing_exclusion(item, asof):
            return None
        # Resolve dated official terms here too: callers need not pre-annotate.
        item = {**item, **lifecycle(item, asof)}
    price, premium, vol = item.get('latest'), item.get('conv_prem'), item.get('vol_20d')
    K = item.get('maturity_call_price')
    T = item.get('surplus_years')
    end = item.get('conversion_end_date')
    if end and asof:
        T = (date.fromisoformat(iso_date(end))-date.fromisoformat(iso_date(asof))).days/365
    inputs = (price, premium, vol, K, T, r)
    if not all(finite_number(v) for v in inputs):
        return None
    price, premium, vol, K, T, r = map(float, inputs)
    if price <= 0 or premium <= -100 or vol < 0 or K <= 0 or T < 0:
        return None
    try:
        call, delta, gamma, theta, vega = bs_call(price/(1+premium/100), K, vol/100, r, T)
    except (ValueError, ZeroDivisionError, OverflowError):
        return None
    floor = item.get('pure_bond_value')
    total = call+float(floor) if finite_number(floor) and float(floor) > 0 else None
    return {'bs_value': total, 'relative_value': price/total if total and total > 0 else None,
            'bs_delta': delta, 'bs_gamma': gamma, 'bs_theta': theta, 'bs_vega': vega,
            'model_version': MODEL_VERSION, 'volatility_source': 'stock_historical_20d',
            'model_term_years': T, 'model_rate': r}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--trade-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--default-r", type=float, default=0.025)
    args = ap.parse_args()

    dataset = json.load(open(args.dataset, encoding="utf-8"))
    items = dataset["items"]

    results = []
    priced = 0
    for it in items:
        from _lifecycle import annotate
        annotate(it, args.trade_date)
        metrics = bond_metrics(it, args.default_r, args.trade_date)
        if metrics is None or metrics['bs_value'] is None:
            for key in ('model_version', 'volatility_source', 'model_term_years', 'model_rate'):
                it[key] = None
            results.append(None)
            continue
        results.append({'trade_date': args.trade_date, 'code': it['code'],
                        **{k: round(metrics[k], 2 if k == 'bs_value' else 4) if metrics[k] is not None else None
                           for k in ('bs_value', 'relative_value', 'bs_delta', 'bs_gamma', 'bs_theta', 'bs_vega')}})
        it.update({k: metrics[k] for k in ('model_version', 'volatility_source', 'model_term_years', 'model_rate')})
        priced += 1

    bs_fields = ("bs_value", "relative_value", "bs_delta", "bs_gamma", "bs_theta", "bs_vega")
    db_rows = [result or {"trade_date": args.trade_date, "code": item["code"],
                         **dict.fromkeys(bs_fields)}
               for item, result in zip(items, results)]
    if db_rows:
        con = connect()
        n = db_upsert(con, "valuation_daily", db_rows, ["trade_date", "code"])
        con.close()
        print(f"[db] valuation_daily BS fields upserted for {n} rows")

    # Also write BS fields back into dataset.json (avoids needing a 2nd assemble run)
    bs_map = {r["code"]: r for r in db_rows}
    for it in items:
        bs = bs_map.get(it["code"])
        if bs:
            it["bs_value"] = bs["bs_value"]
            it["relative_value"] = bs["relative_value"]
            it["bs_delta"] = bs["bs_delta"]
            it["bs_gamma"] = bs["bs_gamma"]
            it["bs_theta"] = bs["bs_theta"]
            it["bs_vega"] = bs["bs_vega"]
    with open(args.dataset, "w", encoding="utf-8") as f:
        json.dump(dataset, f, ensure_ascii=False, indent=2)
    print(f"[json] dataset.json updated with BS fields in-place")

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

    print(f"[done] BS priced {priced}/{len(items)} bonds (trade_date={args.trade_date})")


if __name__ == "__main__":
    main()
