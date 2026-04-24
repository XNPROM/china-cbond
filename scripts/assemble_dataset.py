"""Assemble cbond_dataset.json from DuckDB via SQL JOIN.

Usage:
  python3 scripts/assemble_dataset.py --trade-date 2026-04-22 --out data/raw/asof=2026-04-22/dataset.json
"""
import argparse, json, os, sys

sys.path.insert(0, os.path.dirname(__file__))
from _db import connect


QUERY = """
SELECT
  u.code, u.name, u.ucode, u.uname,
  v.price          AS latest,
  v.change_pct     AS day_chg,
  v.outstanding_yi AS balance,
  v.maturity_date  AS maturity,
  v.rating,
  v.conv_prem_pct  AS conv_prem,
  v.pure_prem_pct  AS pure_prem,
  v.conv_price,
  v.no_call_start,
  v.no_call_end,
  v.call_trigger_days,
  v.call_trigger_ratio,
  v.has_down_revision,
  v.down_trigger_ratio,
  v.ths_industry,
  v.pb,
  v.redemp_stop_date,
  v.implied_vol,
  v.pe_ttm,
  v.total_mv_yi,
  v.pure_bond_ytm,
  v.ifind_doublelow,
  v.option_value,
  v.surplus_days,
  v.surplus_years,
  v.accum_conv_ratio,
  v.dilution_ratio,
  v.bs_value,
  v.relative_value,
  v.bs_delta,
  v.bs_gamma,
  v.bs_theta,
  v.bs_vega,
  v.pure_bond_value,
  v.maturity_call_price,
  vd.vol_20d_pct   AS vol_20d,
  vd.n_samples     AS vol_n,
  p.main_business  AS profile,
  p.industry
FROM universe u
JOIN valuation_daily v  ON u.code  = v.code  AND v.trade_date = ?
LEFT JOIN vol_daily vd  ON u.ucode = vd.ucode AND vd.trade_date = ?
LEFT JOIN underlying_profile p ON u.ucode = p.ucode
ORDER BY u.code
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trade-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--out", required=True, help="output JSON path")
    args = ap.parse_args()

    con = connect()
    rows = con.execute(QUERY, [args.trade_date, args.trade_date]).fetchall()
    cols = [d[0] for d in con.description]
    con.close()

    items = [dict(zip(cols, row)) for row in rows]
    # Filter out force-redeemed bonds (redemp_stop_date <= trade_date means already stopped trading)
    before = len(items)
    items = [it for it in items if not (it.get("redemp_stop_date") and it["redemp_stop_date"] <= args.trade_date.replace("-", ""))]
    filtered = before - len(items)
    if filtered:
        print(f"[filter] excluded {filtered} force-redeemed bonds")
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    json.dump(
        {"trade_date": args.trade_date, "count": len(items), "items": items},
        open(args.out, "w", encoding="utf-8"), ensure_ascii=False, indent=2
    )
    print(f"[done] {len(items)} records (trade_date={args.trade_date}) → {args.out}")


if __name__ == "__main__":
    main()
