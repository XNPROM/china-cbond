"""Shared data-quality and Delta classification rules."""
import math

VALUATION_CRITICAL = ("price", "conv_prem_pct", "pure_prem_pct", "pure_bond_value", "maturity_call_price")
VALUATION_WARN = ("change_pct", "implied_vol", "pe_ttm", "total_mv_yi", "relative_value", "bs_delta")
CRITICAL_NULL_LIMIT = 0.05
WARNING_NULL_LIMIT = 0.20
EQUITY_DELTA = 0.6
BALANCED_DELTA = 0.3


def finite_number(value):
    try:
        return value is not None and math.isfinite(float(value))
    except (ValueError, TypeError):
        return False


def null_limit(field, fallback=0.5):
    if field == "price":
        return 0.0
    if field in VALUATION_CRITICAL:
        return CRITICAL_NULL_LIMIT
    if field in VALUATION_WARN:
        return WARNING_NULL_LIMIT
    return fallback


def classify_sector(delta):
    if not finite_number(delta):
        return "未分类"
    if delta >= EQUITY_DELTA:
        return "偏股"
    if delta >= BALANCED_DELTA:
        return "平衡"
    return "偏债"
