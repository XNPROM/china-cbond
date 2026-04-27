"""Check data freshness and re-fetch stale fields from iFinD.

Usage:
  # Check only (dry-run):
  python3.12 scripts/refresh_data.py --trade-date 2026-04-24

  # Re-fetch missing fields:
  python3.12 scripts/refresh_data.py --trade-date 2026-04-24 --fix

  # Force re-fetch all bond-side fields:
  python3.12 scripts/refresh_data.py --trade-date 2026-04-24 --fix --force
"""
import argparse, os, sys, time

sys.path.insert(0, os.path.dirname(__file__))
from _db import connect, upsert as db_upsert
from _ifind import basic_data, batched

# Bond-side fields that need [date] in indiparams.
DATE_FIELDS = [
    ("conv_prem_pct",            "ths_conversion_premium_rate_cbond"),
    ("pure_prem_pct",            "ths_pure_bond_premium_rate_cbond"),
    ("pure_bond_value",          "ths_pure_bond_value_cbond"),
    ("pure_bond_ytm",            "ths_pure_bond_ytm_cbond"),
    ("ifind_doublelow",          "ths_convertible_debt_doublelow_cbond"),
    ("option_value",             "ths_option_value_cbond"),
    ("implied_vol",              "ths_implied_volatility_cbond", [None, "1", "1"]),
    ("surplus_days",             "ths_surplus_term_d_cbond"),
    ("surplus_years",            "ths_remain_duration_y_cbond"),
    ("accum_conv_ratio",         "ths_accum_conversion_ratio_cbond"),
    ("dilution_ratio",           "ths_conversion_dlt_ratio_cbond"),
    ("conv_price",               "ths_conversion_price_cbond"),
    ("pb",                       "ths_stock_pb_cbond"),
    ("call_trigger_days",        "ths_conditionalredemption_triggercumulativedays_cbond"),
]

# Fields that use [""] (static).
STATIC_FIELDS = [
    ("rating",                   "ths_issue_credit_rating_cbond"),
    ("maturity_date",            "ths_maturity_date_bond"),
    ("maturity_call_price",      "ths_maturity_redemp_price_cbond"),
    ("no_call_start",            "ths_not_compulsory_redemp_startdate_cbond"),
    ("no_call_end",              "ths_not_compulsory_redemp_enddate_cbond_bond"),
    ("call_trigger_ratio",       "ths_redemp_trigger_ratio_cbond"),
    ("has_down_revision",        "ths_is_special_down_correct_clause_cbond"),
    ("down_trigger_ratio",       "ths_trigger_ratio_cbond"),
    ("ths_industry",             "ths_the_ths_industry_cbond"),
    ("redemp_stop_date",         "ths_redemp_stop_trading_date_bond"),
]

ALL_FIELDS = DATE_FIELDS + STATIC_FIELDS
CRITICAL_COLS = ["price", "conv_prem_pct", "pure_bond_value", "maturity_call_price"]


def _params(template, trade_date):
    if template is None:
        return [trade_date]
    return [trade_date if p is None else p for p in template]


def check_freshness(trade_date, cols=None):
    """Return (total_rows, {col: null_count}) for requested valuation fields."""
    cols = cols or [field[0] for field in ALL_FIELDS]
    con = connect()
    total = con.execute(
        "SELECT count(*) FROM valuation_daily WHERE trade_date = ?", [trade_date]
    ).fetchone()[0]
    if total == 0:
        con.close()
        return 0, {}

    nulls = {}
    for col in cols:
        n = con.execute(
            f"SELECT count(*) FROM valuation_daily WHERE trade_date = ? AND {col} IS NULL",
            [trade_date],
        ).fetchone()[0]
        nulls[col] = n
    con.close()
    return total, nulls


def _fields_to_fetch(trade_date, force=False, null_threshold=0.5):
    total, nulls = check_freshness(trade_date)
    if total == 0:
        return total, nulls, []

    fields_to_fetch = []
    for field in DATE_FIELDS:
        db_col, ifind_key = field[:2]
        template = field[2] if len(field) > 2 else None
        if force or nulls.get(db_col, 0) > total * null_threshold:
            fields_to_fetch.append((db_col, ifind_key, _params(template, trade_date)))

    for db_col, ifind_key in STATIC_FIELDS:
        if force or nulls.get(db_col, 0) > total * null_threshold:
            fields_to_fetch.append((db_col, ifind_key, [""]))

    return total, nulls, fields_to_fetch


def refresh(trade_date, force=False, batch_size=40, null_threshold=0.5, retries=2):
    """Re-fetch bond-side fields from iFinD and upsert to DB.

    If force=False, only fetches fields whose null rate exceeds null_threshold.
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

    total, _, fields_to_fetch = _fields_to_fetch(
        trade_date, force=force, null_threshold=null_threshold
    )
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
        for attempt in range(retries + 1):
            try:
                r = basic_data(b, indipara)
                for t in r.get("tables", []):
                    tbl = t.get("table", {})
                    code = t["thscode"]
                    for db_col, ifind_key, _ in fields_to_fetch:
                        val = (tbl.get(ifind_key) or [None])[0]
                        if val is not None:
                            updates.setdefault(code, {})[db_col] = val
                break
            except Exception as e:
                if attempt >= retries:
                    print(f"[warn] batch err after {attempt + 1} attempts: {e}")
                    break
                wait = 0.5 * (2 ** attempt)
                print(f"[warn] batch err: {e}; retrying in {wait:.1f}s")
                time.sleep(wait)
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
    ap.add_argument("--null-threshold", type=float, default=0.5,
                    help="Fetch a field when null_count / total exceeds this ratio")
    ap.add_argument("--batch-size", type=int, default=40)
    ap.add_argument("--retries", type=int, default=2)
    args = ap.parse_args()

    total, nulls = check_freshness(args.trade_date)
    if total == 0:
        print(f"[check] No data for {args.trade_date}")
        return

    _, _, fields_to_fetch = _fields_to_fetch(
        args.trade_date, force=args.force, null_threshold=args.null_threshold
    )
    print(f"[check] {args.trade_date}: {total} rows")
    for col, n in sorted(nulls.items()):
        rate = n / total if total else 0
        stale = n > total * args.null_threshold
        status = "OK" if n == 0 else f"{'STALE' if stale else 'WARN'} ({n}/{total} null, {rate:.1%})"
        print(f"  {col}: {status}")

    if fields_to_fetch and args.fix:
        refresh(
            args.trade_date,
            force=args.force,
            batch_size=args.batch_size,
            null_threshold=args.null_threshold,
            retries=args.retries,
        )
        # Re-check
        _, nulls_after = check_freshness(args.trade_date)
        stale_after = [c for c, n in nulls_after.items() if n > total * args.null_threshold]
        if not stale_after:
            print(f"[check] All fields below stale threshold for {args.trade_date}")
        else:
            print(f"[check] Still stale fields: {', '.join(stale_after)}")
    elif fields_to_fetch and not args.fix:
        print(f"[hint] Run with --fix to re-fetch stale fields")
    else:
        print(f"[check] No field exceeds stale threshold {args.null_threshold:.0%}")


if __name__ == "__main__":
    main()
