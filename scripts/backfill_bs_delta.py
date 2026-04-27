"""Backfill BS delta for historical dates that have conv_prem but no bs_delta.

Fetches surplus_years, pure_bond_value, maturity_call_price from iFinD,
then computes BS delta using the same formula as bs_pricing.py.

Usage:
    python scripts/backfill_bs_delta.py [--dry-run]
"""
import argparse
import math
import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
from _db import connect, init_schema
from _ifind import basic_data, batched


def _norm_cdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def bs_delta(price, conv_prem, vol_pct, surplus_years, maturity_call_price,
             pure_bond_value, r=0.025):
    """Compute BS delta. Returns None if inputs are insufficient."""
    if any(v is None for v in [price, conv_prem, vol_pct]):
        return None, None
    if price <= 0 or conv_prem <= -90 or vol_pct <= 0:
        return None, None

    S = price / (1.0 + conv_prem / 100.0)
    K = maturity_call_price if maturity_call_price and maturity_call_price > 0 else 110.0
    sigma = vol_pct / 100.0 if vol_pct > 1.5 else vol_pct
    T = surplus_years if surplus_years and surplus_years > 0.01 else 3.0

    if sigma <= 0 or T <= 0.01 or S <= 0 or K <= 0:
        return None, None

    sqrtT = math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * sqrtT)
    d2 = d1 - sigma * sqrtT

    delta = _norm_cdf(d1)

    # Also compute relative_value if pure_bond_value available
    option_val = S * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)
    if pure_bond_value and pure_bond_value > 0:
        total_val = option_val + pure_bond_value
        rel_val = price / total_val if total_val > 0 else None
    else:
        rel_val = None

    return round(delta, 4), round(rel_val, 4) if rel_val else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--batch-size", type=int, default=40)
    args = ap.parse_args()

    con = connect()

    # Find dates that have conv_prem but no bs_delta
    dates = con.execute("""
        SELECT trade_date, COUNT(*) as n
        FROM valuation_daily
        WHERE conv_prem_pct IS NOT NULL
        GROUP BY trade_date
        HAVING SUM(CASE WHEN bs_delta IS NOT NULL THEN 1 ELSE 0 END) = 0
        ORDER BY trade_date
    """).fetchall()
    print(f"[backfill] {len(dates)} dates need BS delta")

    if not dates:
        print("[done] nothing to backfill")
        return

    # Get all codes per date
    all_codes = set()
    for td, _ in dates:
        rows = con.execute(
            "SELECT DISTINCT code FROM valuation_daily WHERE trade_date = ? AND conv_prem_pct IS NOT NULL",
            [td]
        ).fetchall()
        all_codes.update(r[0] for r in rows)
    codes = sorted(all_codes)
    print(f"[codes] {len(codes)} unique codes across all dates")

    # Fetch from iFinD: surplus_years, pure_bond_value, maturity_call_price per date
    total_fetched = 0
    total_computed = 0

    for i, (td, n_bonds) in enumerate(dates):
        print(f"\n[{i+1}/{len(dates)}] {td} ({n_bonds} bonds)")

        # Get existing data from DB for this date
        db_rows = con.execute("""
            SELECT v.code, v.price, v.conv_prem_pct, v.surplus_years,
                   v.pure_bond_value, v.maturity_call_price
            FROM valuation_daily v
            WHERE v.trade_date = ? AND v.conv_prem_pct IS NOT NULL
        """, [td]).fetchall()

        # Check if we already have surplus_years for this date
        n_have_sy = sum(1 for r in db_rows if r[3] is not None)
        need_fetch = n_have_sy < len(db_rows) * 0.5

        if need_fetch:
            # Fetch from iFinD
            date_codes = [r[0] for r in db_rows]
            fetched = {}
            fields = [
                {"indicator": "ths_remain_duration_y_cbond", "indiparams": [td]},
                {"indicator": "ths_pure_bond_value_cbond", "indiparams": [td]},
                {"indicator": "ths_maturity_redemp_price_cbond", "indiparams": [""]},
            ]
            for batch in batched(date_codes, args.batch_size):
                try:
                    r = basic_data(batch, fields)
                    for t in r.get("tables", []):
                        tbl = t.get("table", {})
                        code = t["thscode"]
                        sy = (tbl.get("ths_remain_duration_y_cbond") or [None])[0]
                        pbv = (tbl.get("ths_pure_bond_value_cbond") or [None])[0]
                        mcp = (tbl.get("ths_maturity_redemp_price_cbond") or [None])[0]
                        fetched[code] = (sy, pbv, mcp)
                    time.sleep(0.15)
                except Exception as e:
                    print(f"  [warn] fetch error: {e}")
                    time.sleep(1)
            total_fetched += len(fetched)
            print(f"  fetched {len(fetched)} records from iFinD")

            if not args.dry_run:
                # Write fetched fields to DB
                for code, (sy, pbv, mcp) in fetched.items():
                    con.execute("""
                        UPDATE valuation_daily
                        SET surplus_years = COALESCE(surplus_years, ?),
                            pure_bond_value = COALESCE(pure_bond_value, ?),
                            maturity_call_price = COALESCE(maturity_call_price, ?)
                        WHERE trade_date = ? AND code = ?
                    """, [sy, pbv, mcp, td, code])
        else:
            print(f"  surplus_years already available ({n_have_sy}/{len(db_rows)})")

        # Now get vol from vol_daily (joined via universe.ucode)
        vol_rows = con.execute("""
            SELECT v.code, vd.vol_20d_pct
            FROM valuation_daily v
            JOIN universe u ON v.code = u.code
            JOIN vol_daily vd ON u.ucode = vd.ucode AND v.trade_date = vd.trade_date
            WHERE v.trade_date = ?
        """, [td]).fetchall()
        vol_map = {r[0]: r[1] for r in vol_rows}

        # Re-read updated data
        db_rows = con.execute("""
            SELECT code, price, conv_prem_pct, surplus_years,
                   pure_bond_value, maturity_call_price
            FROM valuation_daily
            WHERE trade_date = ? AND conv_prem_pct IS NOT NULL
        """, [td]).fetchall()

        # Compute BS delta
        n_ok = 0
        for code, price, conv_prem, sy, pbv, mcp in db_rows:
            vol = vol_map.get(code)
            if vol is None:
                continue
            delta, rel_val = bs_delta(price, conv_prem, vol, sy, mcp, pbv)
            if delta is not None:
                if not args.dry_run:
                    updates = ["bs_delta = ?"]
                    params = [delta]
                    if rel_val is not None:
                        updates.append("relative_value = ?")
                        params.append(rel_val)
                    params.extend([td, code])
                    con.execute(
                        f"UPDATE valuation_daily SET {', '.join(updates)} "
                        f"WHERE trade_date = ? AND code = ? AND bs_delta IS NULL",
                        params
                    )
                n_ok += 1
        total_computed += n_ok
        print(f"  computed {n_ok}/{len(db_rows)} deltas (vol available: {len(vol_map)})")

    con.close()
    print(f"\n[done] fetched {total_fetched} records, computed {total_computed} deltas")


if __name__ == "__main__":
    main()
