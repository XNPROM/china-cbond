"""Check data freshness and re-fetch stale fields from iFinD.

Usage:
  # Check only (dry-run):
  python3 scripts/refresh_data.py --trade-date 2026-04-24

  # Re-fetch missing fields:
  python3 scripts/refresh_data.py --trade-date 2026-04-24 --fix

  # Force re-fetch all bond-side fields:
  python3 scripts/refresh_data.py --trade-date 2026-04-24 --fix --force
"""
import argparse, json, os, sys, time

sys.path.insert(0, os.path.dirname(__file__))
from _db import connect, upsert as db_upsert
from _ifind import basic_data, batched

# Bond-side fields that need [date] in indiparams — these go NULL when iFinD data is delayed.
DATE_FIELDS = [
    ("conv_prem_pct",            "ths_conversion_premium_rate_cbond"),
    ("pure_prem_pct",            "ths_pure_bond_premium_rate_cbond"),
    ("pure_bond_value",          "ths_pure_bond_value_cbond"),
    ("maturity_call_price",      "ths_maturity_redemp_price_cbond"),
    ("pure_bond_ytm",            "ths_pure_bond_ytm_cbond"),
    ("ifind_doublelow",          "ths_convertible_debt_doublelow_cbond"),
    ("option_value",             "ths_option_value_cbond"),
    ("implied_vol",              "ths_implied_volatility_cbond"),
    ("surplus_days",             "ths_surplus_term_d_cbond"),
    ("surplus_years",            "ths_remain_duration_y_cbond"),
    ("accum_conv_ratio",         "ths_accum_conversion_ratio_cbond"),
    ("dilution_ratio",           "ths_conversion_dlt_ratio_cbond"),
    ("conv_price",               "ths_conversion_price_cbond"),
    ("pb",                       "ths_stock_pb_cbond"),
    ("call_trigger_days",        "ths_conditionalredemption_triggercumulativedays_cbond"),
]

# Fields that use [""] (static) — rarely NULL, but included in force mode.
STATIC_FIELDS = [
    ("rating",                   "ths_issue_credit_rating_cbond"),
    ("maturity_date",            "ths_maturity_date_bond"),
    ("no_call_start",            "ths_not_compulsory_redemp_startdate_cbond"),
    ("no_call_end",              "ths_not_compulsory_redemp_enddate_cbond_bond"),
    ("call_trigger_ratio",       "ths_redemp_trigger_ratio_cbond"),
    ("has_down_revision",        "ths_is_special_down_correct_clause_cbond"),
    ("down_trigger_ratio",       "ths_trigger_ratio_cbond"),
    ("ths_industry",             "ths_the_ths_industry_cbond"),
    ("redemp_stop_date",         "ths_redemp_stop_trading_date_bond"),
]

# Key fields whose NULL-ness determines "stale" status.
CRITICAL_COLS = ["conv_prem_pct", "pure_bond_value", "maturity_call_price"]


def check_freshness(trade_date):
    """Return (total_rows, {col: null_count}) for critical fields."""
    con = connect()
    total = con.execute(
        "SELECT count(*) FROM valuation_daily WHERE trade_date = ?", [trade_date]
    ).fetchone()[0]
    if total == 0:
        con.close()
        return 0, {}

    nulls = {}
    for col in CRITICAL_COLS:
        n = con.execute(
            f"SELECT count(*) FROM valuation_daily WHERE trade_date = ? AND {col} IS NULL",
            [trade_date],
        ).fetchone()[0]
        nulls[col] = n
    con.close()
    return total, nulls


def refresh(trade_date, force=False, batch_size=40):
    """Re-fetch bond-side fields from iFinD and upsert to DB.

    If force=False, only fetches fields that are mostly NULL (>50% rows).
    Returns number of rows updated.
    """
    con = connect()
    codes = [r[0] for r in con.execute(
        "SELECT code FROM valuation_daily WHERE trade_date = ? ORDER BY code",
        [trade_date],
    ).fetchall()]
    con.close()

    if not codes:
        print(f"[refresh] No rows for {trade_date}")
        return 0

    # Decide which fields to fetch
    total, nulls = check_freshness(trade_date)
    fields_to_fetch = []

    for db_col, ifind_key in DATE_FIELDS:
        if force:
            fields_to_fetch.append((db_col, ifind_key, [trade_date]))
        elif nulls.get(db_col, 0) > total * 0.5:
            fields_to_fetch.append((db_col, ifind_key, [trade_date]))

    if not force:
        for db_col, ifind_key in STATIC_FIELDS:
            n_null = 0
            try:
                con = connect()
                n_null = con.execute(
                    f"SELECT count(*) FROM valuation_daily WHERE trade_date = ? AND {db_col} IS NULL",
                    [trade_date],
                ).fetchone()[0]
                con.close()
            except Exception:
                con and con.close()
            if n_null > total * 0.5:
                fields_to_fetch.append((db_col, ifind_key, [""]))

    if not fields_to_fetch:
        print(f"[refresh] Data looks fresh for {trade_date}")
        return 0

    print(f"[refresh] Re-fetching {len(fields_to_fetch)} fields for {len(codes)} bonds")

    indipara = [
        {"indicator": ifind_key, "indiparams": params}
        for _, ifind_key, params in fields_to_fetch
    ]

    updates = {}  # code -> {db_col: value}
    for b in batched(codes, batch_size):
        try:
            r = basic_data(b, indipara)
            for t in r.get("tables", []):
                tbl = t.get("table", {})
                code = t["thscode"]
                for db_col, ifind_key, _ in fields_to_fetch:
                    val = (tbl.get(ifind_key) or [None])[0]
                    if val is not None:
                        updates.setdefault(code, {})[db_col] = val
        except Exception as e:
            print(f"[warn] batch err: {e}")
        time.sleep(0.15)

    if not updates:
        print("[refresh] iFinD returned no data — may still be delayed")
        return 0

    # Upsert to DB
    def _f(v):
        try: return float(v)
        except: return None
    def _i(v):
        try: return int(v)
        except: return None
    def _s(v):
        return str(v) if v is not None else None

    INT_COLS = {"surplus_days", "call_trigger_days"}
    STR_COLS = {"rating", "maturity_date", "no_call_start", "no_call_end",
                "has_down_revision", "ths_industry", "redemp_stop_date"}

    db_rows = []
    for code, vals in updates.items():
        row = {"trade_date": trade_date, "code": code}
        for db_col, raw_val in vals.items():
            if db_col in INT_COLS:
                row[db_col] = _i(raw_val)
            elif db_col in STR_COLS:
                row[db_col] = _s(raw_val)
            else:
                row[db_col] = _f(raw_val)
        db_rows.append(row)

    con = connect()
    n = db_upsert(con, "valuation_daily", db_rows, ["trade_date", "code"])
    con.close()
    print(f"[refresh] Updated {n} rows for {trade_date}")
    return n


def main():
    ap = argparse.ArgumentParser(description="Check & refresh iFinD data freshness")
    ap.add_argument("--trade-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--fix", action="store_true", help="Re-fetch stale fields from iFinD")
    ap.add_argument("--force", action="store_true", help="Re-fetch all fields (ignore freshness check)")
    args = ap.parse_args()

    total, nulls = check_freshness(args.trade_date)
    if total == 0:
        print(f"[check] No data for {args.trade_date}")
        return

    stale = any(v > 0 for v in nulls.values())
    print(f"[check] {args.trade_date}: {total} rows")
    for col, n in nulls.items():
        status = "OK" if n == 0 else f"STALE ({n}/{total} null)"
        print(f"  {col}: {status}")

    if stale and args.fix:
        refresh(args.trade_date, force=args.force)
        # Re-check
        _, nulls_after = check_freshness(args.trade_date)
        remaining = sum(nulls_after.values())
        if remaining == 0:
            print(f"[check] All critical fields now fresh for {args.trade_date}")
        else:
            print(f"[check] Still {remaining} nulls — iFinD data may not be available yet")
    elif stale and not args.fix:
        print(f"[hint] Run with --fix to re-fetch stale fields")


if __name__ == "__main__":
    main()
