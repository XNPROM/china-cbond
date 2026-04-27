"""Validate one daily convertible-bond snapshot.

Usage:
  python3.12 scripts/validate_snapshot.py --trade-date 2026-04-23
  python3.12 scripts/validate_snapshot.py --trade-date 2026-04-23 --dataset data/raw/asof=2026-04-23/dataset.json
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from _db import connect


VALUATION_CRITICAL = [
    "price",
    "conv_prem_pct",
    "pure_prem_pct",
    "pure_bond_value",
    "maturity_call_price",
]

VALUATION_WARN = [
    "change_pct",
    "implied_vol",
    "pe_ttm",
    "total_mv_yi",
    "relative_value",
    "bs_delta",
]


def _pct(n, d):
    return 0.0 if not d else n / d


def _count_nulls(con, table, trade_date, cols):
    total = con.execute(
        f"SELECT count(*) FROM {table} WHERE trade_date = ?", [trade_date]
    ).fetchone()[0]
    nulls = {}
    for col in cols:
        nulls[col] = con.execute(
            f"SELECT count(*) FROM {table} WHERE trade_date = ? AND {col} IS NULL",
            [trade_date],
        ).fetchone()[0]
    return total, nulls


def _load_dataset(path):
    if not path or not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def validate(trade_date, dataset_path="", strict=False):
    con = connect()
    failures = []
    warnings = []

    universe_total = con.execute("SELECT count(*) FROM universe").fetchone()[0]
    val_total, val_critical_nulls = _count_nulls(
        con, "valuation_daily", trade_date, VALUATION_CRITICAL
    )
    _, val_warn_nulls = _count_nulls(con, "valuation_daily", trade_date, VALUATION_WARN)

    vol_total = con.execute(
        "SELECT count(*) FROM vol_daily WHERE trade_date = ?", [trade_date]
    ).fetchone()[0]
    theme_total = con.execute(
        "SELECT count(*) FROM themes WHERE trade_date = ?", [trade_date]
    ).fetchone()[0]
    strategy_total = con.execute(
        "SELECT count(*) FROM strategy_picks WHERE trade_date = ?", [trade_date]
    ).fetchone()[0]
    strategy_groups = con.execute(
        "SELECT strategy, count(*) FROM strategy_picks WHERE trade_date = ? GROUP BY 1 ORDER BY 1",
        [trade_date],
    ).fetchall()
    con.close()

    print(f"[validate] trade_date={trade_date}")
    print(f"  universe: {universe_total}")
    print(f"  valuation_daily: {val_total}")
    print(f"  vol_daily: {vol_total}")
    print(f"  themes: {theme_total}")
    print(f"  strategy_picks: {strategy_total} {strategy_groups}")

    if universe_total < 250:
        failures.append(f"universe too small: {universe_total}")
    if val_total < max(250, universe_total * 0.75):
        failures.append(f"valuation rows too small: {val_total}")
    if vol_total < max(200, universe_total * 0.6):
        failures.append(f"vol rows too small: {vol_total}")
    if theme_total < max(200, val_total * 0.6):
        failures.append(f"themes rows too small: {theme_total}")
    if strategy_total == 0:
        warnings.append("strategy_picks empty")

    for col, n in val_critical_nulls.items():
        rate = _pct(n, val_total)
        print(f"  critical {col}: {n}/{val_total} null ({rate:.1%})")
        if rate > 0.05:
            failures.append(f"{col} critical null rate {rate:.1%}")

    for col, n in val_warn_nulls.items():
        rate = _pct(n, val_total)
        print(f"  warn {col}: {n}/{val_total} null ({rate:.1%})")
        if rate > 0.20:
            warnings.append(f"{col} warn null rate {rate:.1%}")

    dataset = _load_dataset(dataset_path)
    if dataset is not None:
        items = dataset.get("items", [])
        print(f"  dataset: {len(items)} items ({dataset_path})")
        if len(items) < 250:
            failures.append(f"dataset too small: {len(items)}")
        missing_profile = sum(1 for x in items if not x.get("profile"))
        missing_vol = sum(1 for x in items if x.get("vol_20d") is None)
        missing_rv = sum(1 for x in items if x.get("relative_value") is None)
        print(f"  dataset missing profile={missing_profile} vol={missing_vol} relative_value={missing_rv}")
        if missing_profile > len(items) * 0.05:
            failures.append(f"dataset profile missing too high: {missing_profile}")
        if missing_vol > len(items) * 0.05:
            failures.append(f"dataset vol missing too high: {missing_vol}")
        if missing_rv > len(items) * 0.20:
            warnings.append(f"dataset relative_value missing high: {missing_rv}")

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
    ap.add_argument("--strict", action="store_true", help="Treat warnings as failures")
    args = ap.parse_args()
    raise SystemExit(validate(args.trade_date, args.dataset, strict=args.strict))


if __name__ == "__main__":
    main()
