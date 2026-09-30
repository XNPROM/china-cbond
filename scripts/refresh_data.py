"""Check data freshness and re-fetch stale fields from iFinD.

Usage:
  # Check only (dry-run):
  python3.12 scripts/refresh_data.py --trade-date 2026-04-24

  # Re-fetch missing fields:
  python3.12 scripts/refresh_data.py --trade-date 2026-04-24 --fix

  # Force re-fetch all bond-side fields:
  python3.12 scripts/refresh_data.py --trade-date 2026-04-24 --fix --force
"""
import argparse, csv, json, os, sys, time
from datetime import datetime, timezone
from pathlib import Path

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
from _snapshot_policy import (VALUATION_CRITICAL, VALUATION_WARN, finite_number, null_limit)

INT_COLS = {"surplus_days", "call_trigger_days"}
STR_COLS = {"rating", "maturity_date", "no_call_start", "no_call_end", "has_down_revision",
            "ths_industry", "redemp_stop_date"}
CSV_COLS = dict(zip(
    [f[0] for f in ALL_FIELDS],
    ["转股溢价率(%)", "纯债溢价率(%)", "纯债价值", "纯债YTM(%)", "iFinD双低", "期权价值",
     "隐含波动率(%)", "剩余期限(天)", "剩余期限(年)", "累计转股比例(%)", "转股稀释比例(%)",
     "转股价", "正股PB", "强赎累计触发天数", "评级", "到期日", "到期赎回价", "不强赎起始日",
     "不强赎截止日", "强赎触发比例(%)", "是否有下修条款", "下修触发比例(%)", "同花顺行业", "强赎停止交易日"]
))


def _params(template, trade_date):
    return [trade_date] if template is None else [trade_date if p is None else p for p in template]


def _missing_sql(col):
    if col in STR_COLS:
        return f"({col} IS NULL OR trim({col}) = '')"
    return f"({col} IS NULL OR NOT isfinite({col}))"


def check_freshness(trade_date, cols=None):
    cols = cols or [f[0] for f in ALL_FIELDS]
    con = connect()
    try:
        total = con.execute("SELECT count(*) FROM valuation_daily WHERE trade_date=?", [trade_date]).fetchone()[0]
        nulls = {col: con.execute(f"SELECT count(*) FROM valuation_daily WHERE trade_date=? AND {_missing_sql(col)}", [trade_date]).fetchone()[0]
                 for col in cols} if total else {}
        return total, nulls
    finally:
        con.close()


def _fields_to_fetch(trade_date, force=False, null_threshold=None):
    total, nulls = check_freshness(trade_date)
    fields = []
    for field in ALL_FIELDS:
        col, indicator = field[:2]
        # Try every missing required/display field. Optional terms are often
        # legitimately empty; keep their 50% threshold unless explicitly set.
        limit = null_threshold if null_threshold is not None else (0 if col in VALUATION_CRITICAL + VALUATION_WARN else 0.5)
        if force or nulls.get(col, 0) > total * limit:
            template = field[2] if len(field) > 2 else ([""] if field in STATIC_FIELDS else None)
            fields.append((col, indicator, _params(template, trade_date)))
    return total, nulls, fields


def _convert(col, value):
    if value is None or str(value).strip() in ("", "--", "-"):
        return None
    if col in STR_COLS:
        return str(value)
    if not finite_number(value):
        return None
    number = float(value)
    if col in INT_COLS:
        return int(number)
    return round(number * 100, 2) if col == "implied_vol" else number


def _sync_csv(path, updates):
    if not path or not Path(path).is_file():
        return
    with open(path, encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        names, rows = reader.fieldnames, list(reader)
    for row in rows:
        for col, value in updates.get(row.get("转债代码", "").upper(), {}).items():
            label = CSV_COLS[col]
            if label in names:
                row[label] = value
    temp = str(path) + ".tmp"
    with open(temp, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=names)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temp, path)


def refresh(trade_date, force=False, batch_size=40, null_threshold=None, retries=2,
            csv_path=None, audit_path=None):
    """Request only missing code/field pairs; preserve existing valid values."""
    if batch_size <= 0 or retries < 0:
        raise ValueError("invalid batch size or retry count")
    total, _, fields = _fields_to_fetch(trade_date, force=force, null_threshold=null_threshold)
    if not total or not fields:
        print(f"[refresh] No fields need fetching for {trade_date}")
        return 0
    con = connect()
    try:
        requests = {}
        for field in fields:
            condition = "" if force else " AND " + _missing_sql(field[0])
            codes = [r[0] for r in con.execute("SELECT code FROM valuation_daily WHERE trade_date=?" + condition + " ORDER BY code", [trade_date]).fetchall()]
            if codes:
                requests.setdefault(tuple(codes), []).append(field)
    finally:
        con.close()
    updates, errors = {}, []
    audit = {"trade_date": trade_date, "created_at": datetime.now(timezone.utc).isoformat(), "batches": []}
    for codes, group in requests.items():
        field_names = [field[0] for field in group]
        for batch in batched(list(codes), batch_size):
            response = None
            for attempt in range(retries + 1):
                try:
                    response = basic_data(batch, [{"indicator": indicator, "indiparams": params} for _, indicator, params in group])
                    if str(response.get("errorcode", 0)) != "0":
                        raise RuntimeError(f"iFinD error {response.get('errorcode')}: {response.get('errmsg', '')}")
                    break
                except Exception as exc:
                    if attempt == retries:
                        errors.append({"fields": field_names, "codes": batch, "error": str(exc)})
                    else:
                        time.sleep(0.5 * 2 ** attempt)
            audit["batches"].append({"fields": field_names, "codes": batch, "response": response})
            if response:
                for table in response.get("tables", []):
                    code = str(table.get("thscode", "")).strip().upper()
                    if code not in batch:
                        continue
                    for col, indicator, _ in group:
                        value = _convert(col, (table.get("table", {}).get(indicator) or [None])[0])
                        if value is not None:
                            updates.setdefault(code, {})[col] = value
            time.sleep(0.15)
    if audit_path:
        audit.update(updated_codes=sorted(updates), batch_errors=errors)
        Path(audit_path).parent.mkdir(parents=True, exist_ok=True)
        Path(audit_path).write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")
    con = connect()
    try:
        con.execute("BEGIN TRANSACTION")
        n = db_upsert(con, "valuation_daily", [{"trade_date": trade_date, "code": code, **vals} for code, vals in updates.items()], ["trade_date", "code"])
        con.execute("COMMIT")
    finally:
        con.close()
    _sync_csv(csv_path, updates)
    print(f"[refresh] Updated {n} rows, {len(errors)} failed batches for {trade_date}")
    if errors:
        raise RuntimeError(f"{len(errors)} recovery batches failed; successful updates preserved")
    return n


def main():
    ap = argparse.ArgumentParser(description="Check & refresh iFinD data freshness")
    ap.add_argument("--trade-date", required=True)
    ap.add_argument("--fix", action="store_true")
    ap.add_argument("--force", action="store_true", help="Explicitly refetch existing values as well")
    ap.add_argument("--null-threshold", type=float, default=None, help="Override repair selection threshold; does not relax validation")
    ap.add_argument("--batch-size", type=int, default=40)
    ap.add_argument("--retries", type=int, default=2)
    args = ap.parse_args()
    if args.null_threshold is not None and not 0 <= args.null_threshold <= 1:
        ap.error("--null-threshold must be between 0 and 1")
    raw = Path(__file__).resolve().parents[1] / "data" / "raw" / f"asof={args.trade_date}"
    if args.fix:
        refresh(args.trade_date, force=args.force, batch_size=args.batch_size,
                null_threshold=args.null_threshold, retries=args.retries,
                csv_path=raw / "valuation.csv", audit_path=raw / "field_refresh_audit.json")
    total, nulls = check_freshness(args.trade_date)
    failures = []
    for col, n in sorted(nulls.items()):
        print(f"  {col}: {n}/{total} missing")
        if col in VALUATION_CRITICAL + VALUATION_WARN and n > total * null_limit(col):
            failures.append(col)
    if not total or failures:
        print(f"[check] Incomplete snapshot: {', '.join(failures) or 'no rows'}")
        if args.fix:
            raise SystemExit(1)
    else:
        print(f"[check] Required field thresholds passed for {args.trade_date}")


if __name__ == "__main__":
    main()
