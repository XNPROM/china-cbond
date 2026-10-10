"""Validate one daily convertible-bond snapshot.

Usage:
  python3.12 scripts/validate_snapshot.py --trade-date 2026-04-23
  python3.12 scripts/validate_snapshot.py --trade-date 2026-04-23 --dataset data/raw/asof=2026-04-23/dataset.json
"""
import argparse
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(__file__))
from bs_pricing import MODEL_VERSION
from _db import connect
from _lifecycle import POLICY_VERSION, trade_status, recommendation_reason, next_session, lifecycle, pricing_exclusion
from _snapshot_policy import (VALUATION_CRITICAL, VALUATION_WARN, finite_number, null_limit)


PRIVATE_CB_RE = re.compile(r"(定转|定\d+)")


def _pct(n, d):
    return 0.0 if not d else n / d


def _count_nulls(con, table, trade_date, cols):
    total = con.execute(
        f"SELECT count(*) FROM {table} WHERE trade_date = ?", [trade_date]
    ).fetchone()[0]
    nulls = {}
    for col in cols:
        nulls[col] = con.execute(
            f"SELECT count(*) FROM {table} WHERE trade_date = ? AND ({col} IS NULL OR NOT isfinite({col}))",
            [trade_date],
        ).fetchone()[0]
    return total, nulls


def _load_dataset(path):
    if not path or not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _load_codes(path):
    if not path or not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return list(dict.fromkeys(
            line.strip().upper() for line in f if line.strip()
        ))


def _check_audit(path, trade_date, expected):
    try:
        with open(path, encoding="utf-8") as handle:
            audit = json.load(handle)
        errors = []
        if audit.get("trade_date") != trade_date:
            errors.append("quote audit date mismatch")
        for key in ("expected_codes", "returned_codes"):
            values = audit.get(key, [])
            if len(values) != len(expected) or set(values) != expected:
                errors.append(f"quote audit {key} mismatch")
        if audit.get("expected_count") != len(expected) or audit.get("returned_count") != len(expected):
            errors.append("quote audit count mismatch")
        for key in ("missing_codes", "blank_codes", "batch_errors"):
            if audit.get(key) != []:
                errors.append(f"quote audit {key} is missing or non-empty")
        details = audit.get('quote_details', {})
        if set(details) != expected:
            errors.append('quote metadata coverage mismatch')
        for code in expected:
            detail = details.get(code, {})
            if detail.get('quote_date') != trade_date:
                errors.append(f'quote observation date mismatch: {code}')
            if not finite_number(detail.get('volume')) or float(detail['volume']) < 0:
                errors.append(f'quote volume unavailable: {code}')
        if audit.get("missing_count") != 0:
            errors.append("quote audit missing_count is not zero")
        return errors
    except (OSError, ValueError, TypeError, AttributeError) as exc:
        return [f"quote audit unavailable/invalid: {exc}"]


def validate(trade_date, dataset_path="", strict=False, codes_path="", backtest_path=""):
    raw = os.path.join(os.path.dirname(__file__), "..", "data", "raw", f"asof={trade_date}")
    dataset_path = dataset_path or os.path.join(raw, "dataset.json")
    codes_path = codes_path or os.path.join(raw, "cbond_codes.txt")
    con = connect(read_only=True)
    failures = []
    warnings = []

    universe_total = con.execute("SELECT count(*) FROM universe").fetchone()[0]
    val_total, val_critical_nulls = _count_nulls(
        con, "valuation_daily", trade_date, VALUATION_CRITICAL
    )
    _, val_warn_nulls = _count_nulls(con, "valuation_daily", trade_date, VALUATION_WARN)
    quote_rows = con.execute(
        "SELECT code, price FROM valuation_daily WHERE trade_date = ?", [trade_date]
    ).fetchall()

    vol_total = con.execute(
        "SELECT count(*) FROM vol_daily WHERE trade_date = ?", [trade_date]
    ).fetchone()[0]
    theme_total = con.execute(
        "SELECT count(*) FROM themes WHERE trade_date = ?", [trade_date]
    ).fetchone()[0]
    theme_bad_rows = con.execute(
        """
        SELECT code, business_rewrite
          FROM themes
         WHERE trade_date = ?
           AND (
             business_rewrite LIKE '本次债券募集资金%'
             OR business_rewrite LIKE '%募集资金%用于%'
           )
        """,
        [trade_date],
    ).fetchall()
    theme_bad_business = len(theme_bad_rows)
    theme_empty_business = con.execute(
        """
        SELECT count(*)
          FROM themes
         WHERE trade_date = ?
           AND (business_rewrite IS NULL OR trim(business_rewrite) = '')
        """,
        [trade_date],
    ).fetchone()[0]
    strategy_total = con.execute(
        "SELECT count(*) FROM strategy_picks WHERE trade_date = ?", [trade_date]
    ).fetchone()[0]
    strategy_groups = con.execute(
        "SELECT strategy, count(*) FROM strategy_picks WHERE trade_date = ? GROUP BY 1 ORDER BY 1",
        [trade_date],
    ).fetchall()
    theme_codes = {r[0] for r in con.execute("SELECT code FROM themes WHERE trade_date=?", [trade_date]).fetchall()}
    strategy_codes = {r[0] for r in con.execute("SELECT code FROM strategy_picks WHERE trade_date=?", [trade_date]).fetchall()}
    latest_steps = con.execute("""
        SELECT step, status FROM etl_runs WHERE trade_date=?
        QUALIFY row_number() OVER (PARTITION BY step ORDER BY started_at DESC, run_id DESC)=1
    """, [trade_date]).fetchall()
    con.close()
    # Validation cannot require its own previous invocation to have succeeded.
    # render_html is downstream; backtest is checked only when requested.
    required_steps = {"fetch_cb_universe", "fetch_valuation", "refresh_data", "refresh_underlying_profile",
                      "compute_volatility", "assemble_dataset", "bs_pricing", "strategy_score",
                      "ensure_themes", "generate_themes_direct", "build_overview_md"}
    if backtest_path:
        required_steps.add("backtest_weekly")
    for step, status in latest_steps:
        if step in required_steps and status not in ("ok", "success"):
            failures.append(f"latest required ETL step {step}: {status}")

    expected_codes = _load_codes(codes_path)
    if expected_codes:
        expected_set = set(expected_codes)
        quoted_codes = {
            str(code).strip().upper()
            for code, price in quote_rows
            if finite_number(price) and price > 0
        }
        missing_quote_codes = [
            code for code in expected_codes if code not in quoted_codes
        ]
        unexpected_quote_codes = sorted({str(code).strip().upper() for code, _ in quote_rows} - expected_set)
    else:
        missing_quote_codes = []
        unexpected_quote_codes = []

    print(f"[validate] trade_date={trade_date}")
    print(f"  universe: {universe_total}")
    print(f"  valuation_daily: {val_total}")
    print(f"  vol_daily: {vol_total}")
    print(f"  themes: {theme_total}")
    print(f"  themes bad business_rewrite: {theme_bad_business}")
    print(f"  themes empty business_rewrite: {theme_empty_business}")
    print(f"  strategy_picks: {strategy_total} {strategy_groups}")
    if expected_codes:
        print(
            f"  quote coverage: {len(quoted_codes)}/{len(expected_codes)} "
            f"missing={len(missing_quote_codes)} "
            f"unexpected={len(unexpected_quote_codes)}"
        )
        if missing_quote_codes:
            print("  missing quote codes: " + ", ".join(missing_quote_codes))

    if not expected_codes:
        failures.append(f"expected code list missing or empty: {codes_path}")
    if not val_total:
        failures.append("valuation snapshot empty")
    if expected_codes:
        if theme_codes != expected_set:
            failures.append(f"theme coverage differs from expected list: missing={len(expected_set-theme_codes)} unexpected={len(theme_codes-expected_set)}")
        if strategy_codes - expected_set:
            failures.append("strategy contains codes outside expected list")
        failures.extend(_check_audit(os.path.join(os.path.dirname(dataset_path), "quote_audit.json"), trade_date, expected_set))
    if theme_empty_business > max(10, theme_total * 0.05):
        failures.append(
            f"themes business_rewrite empty too high: "
            f"{theme_empty_business}/{theme_total}"
        )
    if strategy_total == 0:
        warnings.append("strategy_picks empty")
    if expected_codes and missing_quote_codes:
        failures.append(
            f"quote coverage incomplete: {len(missing_quote_codes)}/{len(expected_codes)} missing"
        )
    if expected_codes and unexpected_quote_codes:
        failures.append(
            f"valuation contains {len(unexpected_quote_codes)} codes outside expected list"
        )

    for col, n in val_critical_nulls.items():
        rate = _pct(n, val_total)
        print(f"  critical {col}: {n}/{val_total} null ({rate:.1%})")
        if rate > null_limit(col):
            failures.append(f"{col} critical null rate {rate:.1%}")

    for col, n in val_warn_nulls.items():
        rate = _pct(n, val_total)
        print(f"  warn {col}: {n}/{val_total} null ({rate:.1%})")
        if rate > null_limit(col):
            warnings.append(f"{col} warn null rate {rate:.1%}")

    try:
        dataset = _load_dataset(dataset_path)
    except (OSError, ValueError) as exc:
        failures.append(f"cannot read dataset: {exc}")
        dataset = None
    if dataset is None:
        failures.append(f"dataset missing: {dataset_path}")
    if dataset is not None:
        items = dataset.get("items", [])
        dataset_codes = {x.get("code") for x in items}
        if dataset.get('tradability_policy') != POLICY_VERSION:
            failures.append('dataset uses an obsolete tradability policy')
        if dataset.get('execution_date') != next_session(trade_date):
            failures.append('dataset execution date mismatch')
        try:
            with open(os.path.join(os.path.dirname(dataset_path), "quote_audit.json")) as handle:
                audited_quotes = json.load(handle).get("quote_details", {})
        except (OSError, ValueError, AttributeError):
            audited_quotes = {}
        for item in items:
            exclusion = pricing_exclusion(item, trade_date)
            fields = ('bs_value', 'relative_value', 'bs_delta', 'bs_gamma', 'bs_theta', 'bs_vega')
            if item.get('pricing_exclusion') != exclusion:
                failures.append(f"dataset pricing eligibility mismatch: {item['code']}")
            if exclusion and any(item.get(field) is not None for field in fields):
                failures.append(f"terminal bond retains ordinary BS fields: {item['code']}")
            if any(item.get(field) is not None for field in fields) and item.get('model_version') != MODEL_VERSION:
                failures.append(f"dataset uses obsolete pricing {item['code']}")
            facts = lifecycle(item, trade_date)
            if item.get('conversion_end_date') != facts['conversion_end_date']:
                failures.append(f"dataset conversion deadline differs from dated terms: {item['code']}")
            volume = item.get('quote_volume')
            audited_volume = audited_quotes.get(item.get('code'), {}).get('volume')
            if (not finite_number(volume) or float(volume) < 0
                    or not finite_number(audited_volume) or float(volume) != float(audited_volume)):
                failures.append(f"dataset quote volume differs from audit: {item['code']}")
            if item.get('quote_date') != trade_date:
                failures.append(f"dataset quote date mismatch: {item['code']}")
            state = trade_status(item, trade_date)
            if state != 'trading':
                failures.append(f"dataset contains non-trading bond {item['code']}: {state}")
            if item['code'] in strategy_codes:
                reason = recommendation_reason(item, trade_date)
                if reason:
                    failures.append(f"strategy contains ineligible bond {item['code']}: {reason}")
                if item.get('model_version') != MODEL_VERSION and item.get('bs_delta') is not None:
                    failures.append(f"strategy uses obsolete pricing {item['code']}")
        dataset_bad_business = sum(1 for code, _ in theme_bad_rows if code in dataset_codes)
        print(f"  dataset: {len(items)} items ({dataset_path})")
        if dataset.get("trade_date") != trade_date:
            failures.append("dataset trade_date mismatch")
        if expected_codes and dataset_codes != expected_set:
            failures.append("dataset code set differs from expected list")
        if len(items) != len(dataset_codes) or dataset.get("count") != len(items):
            failures.append("dataset duplicate codes or count mismatch")
        if any(not finite_number(x.get("latest")) or x["latest"] <= 0 for x in items):
            failures.append("dataset has missing/invalid prices")
        for col in ("conv_prem", "pure_prem", "pure_bond_value", "maturity_call_price"):
            if sum(not finite_number(x.get(col)) for x in items) > len(items) * 0.05:
                failures.append(f"dataset critical field {col} missing")
        missing_profile = sum(1 for x in items if not x.get("profile"))
        missing_vol = sum(1 for x in items if not finite_number(x.get("vol_20d")))
        missing_rv = sum(1 for x in items if not finite_number(x.get("relative_value")))
        private_cb = sum(1 for x in items if PRIVATE_CB_RE.search(x.get("name") or ""))
        future_listed = sum(
            1 for x in items
            if x.get("list_date") and x["list_date"] > trade_date.replace("-", "")
        )
        print(
            f"  dataset missing profile={missing_profile} vol={missing_vol} "
            f"relative_value={missing_rv} private_cb={private_cb} future_listed={future_listed}"
        )
        if missing_profile > len(items) * 0.05:
            failures.append(f"dataset profile missing too high: {missing_profile}")
        if missing_vol > len(items) * 0.05:
            failures.append(f"dataset vol missing too high: {missing_vol}")
        if missing_rv > len(items) * 0.20:
            warnings.append(f"dataset relative_value missing high: {missing_rv}")
        if private_cb:
            failures.append(f"dataset contains private-placement bonds: {private_cb}")
        if future_listed:
            failures.append(f"dataset contains not-yet-listed bonds: {future_listed}")
        if dataset_bad_business:
            failures.append(f"dataset themes business_rewrite appears to contain fundraising purpose: {dataset_bad_business}")
    elif theme_bad_business:
        failures.append(f"themes business_rewrite appears to contain fundraising purpose: {theme_bad_business}")

    if backtest_path:
        try:
            backtest = _load_dataset(backtest_path)
            if not backtest or not backtest.get("equity_curve"):
                failures.append("backtest missing or empty")
            elif backtest.get('tradability_policy') != POLICY_VERSION or backtest.get('pricing_model') != MODEL_VERSION:
                failures.append('backtest uses obsolete trading/pricing policy')
            elif str(backtest.get("end_date", "")).replace("-", "") != trade_date.replace("-", ""):
                failures.append("backtest end_date mismatch")
        except (OSError, ValueError) as exc:
            failures.append(f"cannot read backtest: {exc}")
    if warnings:
        print("[warnings]")
        for item in warnings:
            print(f"  - {item}")
    if failures:
        print("[failures]")
        for item in failures:
            print(f"  - {item}")
        return 1
    if strict and warnings:
        return 1
    print("[validate] OK")
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trade-date", required=True)
    ap.add_argument("--dataset", default="")
    ap.add_argument("--codes", default="", help="expected as-of cbond_codes.txt")
    ap.add_argument("--backtest", default="", help="Required dated backtest JSON when enabled")
    ap.add_argument("--strict", action="store_true", help="Treat warnings as failures")
    args = ap.parse_args()
    raise SystemExit(validate(
        args.trade_date, args.dataset, strict=args.strict, codes_path=args.codes, backtest_path=args.backtest
    ))


if __name__ == "__main__":
    main()
