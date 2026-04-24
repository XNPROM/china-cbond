"""Pull the full field set for a single convertible bond — debugging helper.

Usage:
  python3 sample_one.py --code 113632.SH --asof 2026-04-20
"""
import argparse, json, math, os, sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(__file__))
from _ifind import basic_data, realtime, history


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--code", required=True, help="e.g. 113632.SH")
    ap.add_argument("--asof", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    # bond fields
    r = basic_data([args.code], [
        {"indicator": "ths_bond_short_name_bond", "indiparams": [""]},
        {"indicator": "ths_stock_code_cbond", "indiparams": [""]},
        {"indicator": "ths_stock_short_name_cbond", "indiparams": [""]},
        {"indicator": "ths_conversion_premium_rate_cbond", "indiparams": [args.asof]},
        {"indicator": "ths_pure_bond_premium_rate_cbond", "indiparams": [args.asof]},
        {"indicator": "ths_bond_balance_cbond", "indiparams": [args.asof]},
        {"indicator": "ths_issue_credit_rating_cbond", "indiparams": [""]},
        {"indicator": "ths_maturity_date_bond", "indiparams": [""]},
    ])
    bond = r["tables"][0]["table"]

    # price
    px = realtime([args.code], "latest")
    latest = px["tables"][0]["table"]["latest"][0]

    # underlying profile
    ucode = bond["ths_stock_code_cbond"][0]
    prof = basic_data([ucode], [{"indicator": "ths_corp_profile", "indiparams": [""]}])
    profile = prof["tables"][0]["table"].get("ths_corp_profile", [""])[0]

    # volatility
    asof = datetime.strptime(args.asof, "%Y-%m-%d").date()
    start = (asof - timedelta(days=45)).isoformat()
    h = history([ucode], "close", start, args.asof, {"Interval": "D", "Fill": "Omit"})
    closes = [c for c in (h["tables"][0]["table"].get("close") or []) if c]
    rets = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]
    last20 = rets[-20:]
    m = sum(last20) / len(last20)
    var = sum((x - m) ** 2 for x in last20) / (len(last20) - 1)
    vol = (var ** 0.5) * (252 ** 0.5)

    print(json.dumps({
        "code": args.code,
        "name": bond["ths_bond_short_name_bond"][0],
        "ucode": ucode,
        "uname": bond["ths_stock_short_name_cbond"][0],
        "latest": latest,
        "conv_prem_pct": bond.get("ths_conversion_premium_rate_cbond", [None])[0],
        "pure_prem_pct": bond.get("ths_pure_bond_premium_rate_cbond", [None])[0],
        "balance_yi": bond.get("ths_bond_balance_cbond", [None])[0],
        "rating": bond.get("ths_issue_credit_rating_cbond", [""])[0],
        "maturity": bond.get("ths_maturity_date_bond", [""])[0],
        "vol_20d_pct": round(vol * 100, 2),
        "vol_sample_n": len(last20),
        "profile": profile,
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
