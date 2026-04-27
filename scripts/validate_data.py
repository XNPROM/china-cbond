"""Data quality validation for cbond pipeline.

Runs sanity checks on the assembled dataset to catch issues early:
- Row count sanity (~335 bonds expected)
- Critical field completeness (price, conv_prem, vol, etc.)
- Value range checks (price > 0, conv_prem > -90, etc.)
- Strategy picks consistency
- Theme coverage

Usage:
  python3 scripts/validate_data.py \
      --dataset data/raw/asof=2026-04-24/dataset.json \
      --trade-date 2026-04-24

Exit codes:
  0 — All checks passed
  1 — Warnings only (data usable but flagged)
  2 — Errors (data should not be used)
"""
import argparse, json, os, sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
from _db import connect

# Expected range for total CB count
MIN_UNIVERSE_SIZE = 200
MAX_UNIVERSE_SIZE = 500

# Critical fields that must be present for every bond
CRITICAL_FIELDS = ["code", "name", "latest", "conv_prem", "vol_20d"]

# Value range checks: (field, min, max, severity)
# severity: "error" = fail the check, "warn" = warning only
RANGE_CHECKS = [
    ("latest",        50,   500,  "error"),  # bond price in yuan
    ("conv_prem",    -90,   500,  "error"),  # conversion premium %
    ("vol_20d",        5,   200,  "warn"),   # 20-day annualized vol %
    ("pe_ttm",        -5,   500,  "warn"),   # PE TTM (allow slight negative)
    ("relative_value", 0.3,  3.0,  "warn"),  # BS relative value
    ("bs_delta",       0,     1.05, "error"), # Delta should be [0, 1]
]


class ValidationResult:
    def __init__(self):
        self.errors = []
        self.warnings = []

    def error(self, msg):
        self.errors.append(msg)

    def warn(self, msg):
        self.warnings.append(msg)

    @property
    def ok(self):
        return len(self.errors) == 0

    @property
    def exit_code(self):
        if self.errors:
            return 2
        if self.warnings:
            return 1
        return 0


def check_universe_size(items, result):
    n = len(items)
    if n < MIN_UNIVERSE_SIZE:
        result.error(f"Universe too small: {n} bonds (expected >={MIN_UNIVERSE_SIZE})")
    elif n > MAX_UNIVERSE_SIZE:
        result.warn(f"Universe unusually large: {n} bonds (expected <={MAX_UNIVERSE_SIZE})")
    else:
        print(f"  [ok] Universe size: {n}")


def check_critical_fields(items, result):
    missing = {field: 0 for field in CRITICAL_FIELDS}
    for it in items:
        for field in CRITICAL_FIELDS:
            if it.get(field) is None:
                missing[field] += 1

    for field, count in missing.items():
        if count > 0:
            pct = count / len(items) * 100
            if pct > 50:
                result.error(f"Missing {field}: {count}/{len(items)} ({pct:.1f}%) — >50% missing")
            else:
                result.warn(f"Missing {field}: {count}/{len(items)} ({pct:.1f}%)")
        else:
            print(f"  [ok] {field}: 100% present")


def check_value_ranges(items, result):
    for field, min_val, max_val, severity in RANGE_CHECKS:
        violations = []
        for it in items:
            val = it.get(field)
            if val is None:
                continue
            if val < min_val or val > max_val:
                violations.append((it["code"], val))

        if violations:
            codes = [c for c, _ in violations[:5]]
            suffix = "..." if len(violations) > 5 else ""
            msg = f"{field} out of range [{min_val}, {max_val}]: {len(violations)} bonds ({', '.join(codes)}{suffix})"
            if severity == "error":
                result.error(msg)
            else:
                result.warn(msg)
        else:
            print(f"  [ok] {field} range check")


def check_zero_balance(items, result):
    """Bonds with 0 balance should be delisted."""
    zero_balance = [it for it in items if it.get("outstanding_yi") is not None and it["outstanding_yi"] == 0]
    if zero_balance:
        result.warn(f"{len(zero_balance)} bonds with 0 balance (likely delisted)")
    else:
        print("  [ok] No zero-balance bonds")


def check_strategy_consistency(trade_date, items, result):
    """Check strategy picks are consistent with dataset."""
    try:
        con = connect()
        strat_rows = con.execute(
            "SELECT code, strategy FROM strategy_picks WHERE trade_date = ?",
            [trade_date]
        ).fetchall()
        con.close()
    except Exception:
        result.warn("Could not query strategy_picks table")
        return

    codes_in_dataset = {it["code"] for it in items}
    orphan_strategies = {r[0] for r in strat_rows} - codes_in_dataset
    if orphan_strategies:
        result.warn(f"Strategy picks reference bonds not in dataset: {orphan_strategies}")
    else:
        print(f"  [ok] Strategy picks: {len(strat_rows)} rows, all codes in dataset")


def check_theme_coverage(trade_date, items, result):
    """Check theme coverage across the universe."""
    try:
        con = connect()
        theme_count = con.execute(
            "SELECT count(*) FROM themes WHERE trade_date = ?",
            [trade_date]
        ).fetchone()[0]
        con.close()
    except Exception:
        result.warn("Could not query themes table")
        return

    coverage = theme_count / len(items) * 100 if items else 0
    if coverage < 80:
        result.warn(f"Theme coverage low: {coverage:.1f}% ({theme_count}/{len(items)})")
    else:
        print(f"  [ok] Theme coverage: {coverage:.1f}%")


def check_bs_pricing_coverage(items, result):
    """Check BS pricing coverage."""
    priced = sum(1 for it in items if it.get("bs_value") is not None)
    coverage = priced / len(items) * 100 if items else 0
    if coverage < 50:
        result.warn(f"BS pricing coverage low: {coverage:.1f}% ({priced}/{len(items)})")
    elif coverage < 80:
        result.warn(f"BS pricing coverage partial: {coverage:.1f}% ({priced}/{len(items)})")
    else:
        print(f"  [ok] BS pricing coverage: {coverage:.1f}%")


def validate(dataset_path, trade_date):
    """Run all validation checks. Returns ValidationResult."""
    result = ValidationResult()
    print(f"\nValidating dataset for {trade_date}...")
    print(f"Source: {dataset_path}\n")

    if not os.path.exists(dataset_path):
        result.error(f"Dataset file not found: {dataset_path}")
        return result

    dataset = json.load(open(dataset_path, encoding="utf-8"))
    items = dataset.get("items", [])

    if not items:
        result.error("Dataset has no items")
        return result

    print("-- Universe checks --")
    check_universe_size(items, result)
    check_critical_fields(items, result)

    print("-- Value range checks --")
    check_value_ranges(items, result)
    check_zero_balance(items, result)

    print("-- Derived data checks --")
    check_bs_pricing_coverage(items, result)
    check_strategy_consistency(trade_date, items, result)
    check_theme_coverage(trade_date, items, result)

    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--trade-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--strict", action="store_true",
                    help="Treat warnings as errors (exit code 1)")
    args = ap.parse_args()

    result = validate(args.dataset, args.trade_date)

    print()
    if result.warnings:
        print(f"WARNINGS ({len(result.warnings)}):")
        for w in result.warnings:
            print(f"  [!] {w}")

    if result.errors:
        print(f"\nERRORS ({len(result.errors)}):")
        for e in result.errors:
            print(f"  [x] {e}")

    if result.ok:
        print("\nAll checks passed.")
    else:
        print(f"\nValidation FAILED ({len(result.errors)} errors, {len(result.warnings)} warnings)")

    sys.exit(result.exit_code if not args.strict else (1 if result.warnings or result.errors else 0))


if __name__ == "__main__":
    main()
