"""Assemble cbond_dataset.json from DuckDB via SQL JOIN.

Usage:
  python3 scripts/assemble_dataset.py --trade-date 2026-04-22 --out data/raw/asof=2026-04-22/dataset.json
"""
import argparse, json, os, re, sys

sys.path.insert(0, os.path.dirname(__file__))
from _db import connect
from _lifecycle import POLICY_VERSION, filter_snapshot, snapshot_metadata, next_session


_PRIVATE_CB_RE = re.compile(r"(定转|定\d+)")


QUERY = """
SELECT
  v.code, u.name, u.ucode, u.uname,
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
  v.pe_ttm,
  v.total_mv_yi,
  COALESCE(v.pure_bond_ytm, (
    SELECT v2.pure_bond_ytm FROM valuation_daily v2
    WHERE v2.code = v.code AND v2.trade_date < ? AND v2.pure_bond_ytm IS NOT NULL
    ORDER BY v2.trade_date DESC LIMIT 1
  )) AS pure_bond_ytm,
  v.ifind_doublelow,
  v.option_value,
  v.implied_vol,
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
  p.industry,
  u.list_date
FROM valuation_daily v
LEFT JOIN universe u ON u.code = v.code AND v.trade_date = ?
LEFT JOIN vol_daily vd  ON u.ucode = vd.ucode AND vd.trade_date = ?
LEFT JOIN underlying_profile p ON u.ucode = p.ucode
WHERE v.trade_date = ?
ORDER BY u.code
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trade-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--out", required=True, help="output JSON path")
    args = ap.parse_args()

    con = connect()
    rows = con.execute(QUERY, [args.trade_date, args.trade_date, args.trade_date, args.trade_date]).fetchall()
    cols = [d[0] for d in con.description]
    vol_rows = {u:(v,n) for u,v,n in con.execute('SELECT ucode,vol_20d_pct,n_samples FROM vol_daily WHERE trade_date=?',[args.trade_date]).fetchall()}
    profiles = {u:(text,industry) for u,text,industry in con.execute('SELECT ucode,main_business,industry FROM underlying_profile').fetchall()}
    con.close()

    items = [dict(zip(cols, row)) for row in rows]
    metadata = snapshot_metadata(args.trade_date)
    audit_path = os.path.join(os.path.dirname(args.out), 'quote_audit.json')
    details = json.load(open(audit_path)).get('quote_details', {}) if os.path.exists(audit_path) else {}
    for it in items:
        bond = metadata.get(it['code'])
        if bond is None:
            continue
        it.update({k:bond.get(k) for k in ('name','ucode','uname')})
        it['list_date'] = str(bond.get('listed') or bond.get('list_date') or '').replace('-','').replace('/','')
        it['vol_20d'], it['vol_n'] = vol_rows.get(it['ucode'], (None,None))
        it['profile'], it['industry'] = profiles.get(it['ucode'], (None,None))
        it['quote_date'] = details.get(it['code'], {}).get('quote_date')
        it['quote_volume'] = details.get(it['code'], {}).get('volume')
    items = [it for it in items if it['code'] in metadata]
    # Filter out privately placed CBs that are not part of the public tradable pool.
    before = len(items)
    items = [it for it in items if not _PRIVATE_CB_RE.search(it.get("name") or "")]
    private = before - len(items)
    if private:
        print(f"[filter] excluded {private} private-placement bonds")

    # Filter out unlisted bonds (list_date IS NULL means not yet listed)
    before = len(items)
    items = [it for it in items if it.get("list_date")]
    unlisted = before - len(items)
    if unlisted:
        print(f"[filter] excluded {unlisted} unlisted bonds (list_date IS NULL)")
    # Exclude bonds whose listing date is after the snapshot date.
    trade_ymd = args.trade_date.replace("-", "")
    before = len(items)
    items = [it for it in items if (it.get("list_date") or "") <= trade_ymd]
    future_listed = before - len(items)
    if future_listed:
        print(f"[filter] excluded {future_listed} not-yet-listed bonds (list_date > trade_date)")

    metadata = snapshot_metadata(args.trade_date)
    for it in items:
        bond = metadata.get(it['code'], {})
        it['redemp_stop_date'] = bond.get('redemp_stop_date')
        for key in ('conversion_end_date', 'last_trade_date', 'stop_trading_date', 'delisting_date',
                    'redemption_date', 'redemption_payment_date', 'redemption_price', 'lifecycle_source', 'lifecycle_known_on'):
            if bond.get(key):
                it[key] = bond[key]
        if not it.get('conversion_end_date'):
            it['conversion_end_date'] = bond.get('maturity')
    items, lifecycle_exclusions = filter_snapshot(items, args.trade_date)

    # Sanity guard: pure_bond_value should never exceed 1.3× maturity redemption price.
    # iFinD occasionally returns inflated pbv for irregular bonds (e.g. 星球转债 2026-05-27
    # returned pbv=204 with redemp=114). When that happens, null both pbv and the
    # derived pure_prem so downstream consumers don't render misleading values.
    sanity_fixed = 0
    for it in items:
        pbv = it.get("pure_bond_value")
        cap = it.get("maturity_call_price") or 110.0
        if pbv is not None and pbv > cap * 1.3:
            print(f"[sanity] {it.get('code')} {it.get('name')}: pbv={pbv:.2f} > 1.3×redemp({cap}); nulling pbv/pure_prem")
            it["pure_bond_value"] = None
            it["pure_prem"] = None
            sanity_fixed += 1
    if sanity_fixed:
        print(f"[sanity] nulled {sanity_fixed} bonds with bogus pure_bond_value")

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    json.dump(
        {"trade_date": args.trade_date, "execution_date": next_session(args.trade_date), "tradability_policy": POLICY_VERSION, "count": len(items), "items": items, "tradability_exclusions": lifecycle_exclusions},
        open(args.out, "w", encoding="utf-8"), ensure_ascii=False, indent=2
    )
    print(f"[done] {len(items)} records (trade_date={args.trade_date}) → {args.out}")


if __name__ == "__main__":
    main()
