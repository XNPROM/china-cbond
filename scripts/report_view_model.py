"""Build a normalized dashboard payload for the HTML report."""

from __future__ import annotations

from collections import defaultdict
import re

from render_markdown_parser import (
    build_category_index,
    categorize_theme,
    compute_kpi_metrics,
    num_value,
    parse_stock,
    signed_class,
)


STRATEGY_ICONS = {
    "双低": "双低",
    "双低-偏股": "偏股双低",
    "双低-平衡": "平衡双低",
    "双低-偏债": "偏债双低",
    "低估": "低估",
}


def to_float(value, default=None):
    """Convert common report values like '12.3%' into a float."""
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).replace("%", "").replace(",", "").strip()
    if not text:
        return default
    try:
        return float(text)
    except ValueError:
        raw = num_value(text)
        if raw == "":
            return default
        try:
            return float(raw)
        except ValueError:
            return default


def derive_sector(conv_value):
    if conv_value is None:
        return ""
    if conv_value < 20:
        return "偏股"
    if conv_value < 50:
        return "平衡"
    return "偏债"


def relative_value_state(rv_value):
    if rv_value is None:
        return "na"
    if rv_value < 1.0:
        return "undervalued"
    if rv_value > 1.2:
        return "expensive"
    return "fair"


def call_state(call_text):
    if not call_text:
        return "none"
    if "强赎停牌" in call_text:
        return "danger"
    if "不强赎" in call_text:
        return "safe"
    if "触发" in call_text:
        return "warn"
    return "note"


def normalize_card(card, theme, idx):
    stock_name, stock_code = parse_stock(card.get("stock", ""))
    deduped_themes = list(dict.fromkeys(card.get("themes", [])))
    price_value = to_float(card.get("price"))
    conv_value = to_float(card.get("conv"))
    pure_value = to_float(card.get("pure"))
    vol_value = to_float(card.get("vol"))
    implied_vol_value = to_float(card.get("implied_vol"))
    rv_value = to_float(card.get("relative_value"))
    delta_value = to_float(card.get("delta"))
    balance_value = to_float(card.get("balance"))
    ytm_value = to_float(card.get("pure_bond_ytm"))
    surplus_years_value = to_float(card.get("surplus_years"))
    conv_price_value = to_float(card.get("conv_price"))
    pe_ttm_value = to_float(card.get("pe_ttm"))
    pb_value = to_float(card.get("pb"))
    total_mv_value = to_float(card.get("total_mv"))
    sector = derive_sector(conv_value)
    search_parts = [
        theme,
        card.get("bond_name", ""),
        card.get("bond_code", ""),
        stock_name,
        stock_code,
        card.get("industry", ""),
        card.get("business", ""),
        " ".join(card.get("themes", [])),
        card.get("call_status", ""),
        card.get("down_status", ""),
    ]
    search_text = " ".join(part for part in search_parts if part).lower()
    search_text = search_text.replace(".sh", "").replace(".sz", "").replace(".bj", "")

    return {
        "idx": idx,
        "bond_name": card.get("bond_name", ""),
        "bond_code": card.get("bond_code", ""),
        "stock_name": stock_name,
        "stock_code": stock_code,
        "industry": card.get("industry", ""),
        "theme_group": theme,
        "category": categorize_theme(theme),
        "themes": deduped_themes,
        "business": card.get("business", ""),
        "strategy": card.get("strategy", ""),
        "sector": sector,
        "search_text": search_text,
        "price": {"text": card.get("price", ""), "value": price_value},
        "day_chg": {
            "text": card.get("day_chg", ""),
            "value": to_float(card.get("day_chg")),
            "class_name": signed_class(card.get("day_chg", "")),
        },
        "conv": {"text": card.get("conv", ""), "value": conv_value},
        "pure": {"text": card.get("pure", ""), "value": pure_value},
        "vol": {"text": card.get("vol", ""), "value": vol_value},
        "implied_vol": {"text": card.get("implied_vol", ""), "value": implied_vol_value},
        "pure_bond_ytm": {"text": card.get("pure_bond_ytm", ""), "value": ytm_value},
        "relative_value": {
            "text": card.get("relative_value", ""),
            "value": rv_value,
            "state": relative_value_state(rv_value),
        },
        "delta": {"text": card.get("delta", ""), "value": delta_value},
        "balance": {"text": card.get("balance", ""), "value": balance_value},
        "surplus_years": {"text": card.get("surplus_years", ""), "value": surplus_years_value},
        "conv_price": {"text": card.get("conv_price", ""), "value": conv_price_value},
        "pe_ttm": {"text": card.get("pe_ttm", ""), "value": pe_ttm_value},
        "pb": {"text": card.get("pb", ""), "value": pb_value},
        "total_mv": {"text": card.get("total_mv", ""), "value": total_mv_value},
        "sw_l2": card.get("sw_l2", ""),
        "rating": card.get("rating", ""),
        "maturity": card.get("maturity", ""),
        "call_status": {
            "text": card.get("call_status", ""),
            "state": call_state(card.get("call_status", "")),
        },
        "down_status": {
            "text": card.get("down_status", ""),
            "state": "safe" if card.get("down_status") else "none",
        },
        "trend": {
            "dates": card.get("sparkline", {}).get("dates", []),
            "delta": card.get("sparkline", {}).get("delta", []),
            "rv": card.get("sparkline", {}).get("rv", []),
        },
        "detail": {
            "bond_name": card.get("bond_name", ""),
            "bond_code": card.get("bond_code", ""),
            "stock_name": stock_name,
            "stock_code": stock_code,
            "industry": card.get("industry", ""),
            "sw_l2": card.get("sw_l2", ""),
            "business": card.get("business", ""),
            "themes": deduped_themes,
            "strategy": card.get("strategy", ""),
            "call_status": card.get("call_status", ""),
            "down_status": card.get("down_status", ""),
        },
    }


def build_highlights(items):
    candidates = [item for item in items if item["relative_value"]["value"] is not None]
    ranked = sorted(
        candidates,
        key=lambda item: (
            item["relative_value"]["value"],
            item["conv"]["value"] if item["conv"]["value"] is not None else 999.0,
            -(item["delta"]["value"] or 0.0),
        ),
    )
    highlights = []
    labels = ["最低相对价值", "低溢价关注", "高弹性关注", "高波动关注"]
    for label, item in zip(labels, ranked[:4]):
        highlights.append(
            {
                "label": label,
                "bond_name": item["bond_name"],
                "bond_code": item["bond_code"],
                "theme_group": item["theme_group"],
                "sector": item["sector"],
                "rv": item["relative_value"]["text"],
                "conv": item["conv"]["text"],
                "delta": item["delta"]["text"],
            }
        )
    return highlights


def build_market_radar(items, categories):
    theme_counter = defaultdict(int)
    for item in items:
        theme_counter[item["theme_group"]] += 1

    return {
        "scatter_points": [
            {
                "idx": item["idx"],
                "bond_name": item["bond_name"],
                "bond_code": item["bond_code"],
                "theme_group": item["theme_group"],
                "sector": item["sector"],
                "price": item["price"]["value"],
                "conv": item["conv"]["value"],
                "relative_value": item["relative_value"]["value"],
                "delta": item["delta"]["value"],
                "balance": item["balance"]["value"],
            }
            for item in items
            if item["price"]["value"] is not None and item["conv"]["value"] is not None
        ],
        "theme_heat": [
            {"theme": theme, "count": count}
            for theme, count in sorted(theme_counter.items(), key=lambda pair: (-pair[1], pair[0]))[:12]
        ],
        "categories": [
            {"name": category["name"], "bond_count": category["bond_count"], "theme_count": len(category["themes"])}
            for category in categories
        ],
    }


def build_strategy_panels(strategy_picks):
    grouped = defaultdict(lambda: {"desc": "", "rows": []})
    for item in strategy_picks:
        name = item.get("strategy", "")
        grouped[name]["desc"] = item.get("desc", "")
        grouped[name]["rows"].append(item.get("cells", []))

    panels = []
    for name, payload in grouped.items():
        panels.append(
            {
                "name": name,
                "icon": STRATEGY_ICONS.get(name, name),
                "desc": payload["desc"],
                "rows": [row[:8] for row in payload["rows"]],
            }
        )
    panels.sort(key=lambda panel: list(STRATEGY_ICONS).index(panel["name"]) if panel["name"] in STRATEGY_ICONS else 999)
    return panels


def build_backtest_payload(backtest):
    if not backtest:
        return None
    return {
        "summary": {
            "start_date": backtest.get("start_date", ""),
            "end_date": backtest.get("end_date", ""),
            "trading_days": backtest.get("trading_days", 0),
            "n_rebalances": backtest.get("n_rebalances", 0),
            "cum_return_dl_pct": backtest.get("cum_return_dl_pct", 0),
            "annualized_dl_pct": backtest.get("annualized_dl_pct", 0),
            "cum_return_sn_pct": backtest.get("cum_return_sn_pct", 0),
            "annualized_sn_pct": backtest.get("annualized_sn_pct", 0),
            "cum_return_mkt_pct": backtest.get("cum_return_mkt_pct", 0),
            "annualized_mkt_pct": backtest.get("annualized_mkt_pct", 0),
        },
        "equity_curve": backtest.get("equity_curve", []),
    }


def parse_summary_kpi_overrides(summary_lines):
    overrides = {}
    for line in summary_lines:
        total_match = re.search(r"总数\s*(\d+)\s*只", line)
        if total_match:
            overrides["total"] = int(total_match.group(1))

        rv_match = re.search(r"相对价值中位数\s*([0-9.]+)；(\d+)\s*只低估", line)
        if rv_match:
            overrides["median_rv"] = float(rv_match.group(1))
            overrides["undervalued"] = int(rv_match.group(2))

        sector_match = re.search(r"分域分布：偏股(\d+)只\s*/\s*平衡(\d+)只\s*/\s*偏债(\d+)只", line)
        if sector_match:
            overrides["n_equity"] = int(sector_match.group(1))
            overrides["n_balanced"] = int(sector_match.group(2))
            overrides["n_debt"] = int(sector_match.group(3))
    return overrides


def build_dashboard_view_model(report, trade_date, backtest):
    categories = build_category_index(report.get("sections", []))
    items = []
    idx = 0
    for section in report.get("sections", []):
        for card in section.get("cards", []):
            items.append(normalize_card(card, section.get("theme", ""), idx))
            idx += 1

    kpis = compute_kpi_metrics(report)
    kpis.update(parse_summary_kpi_overrides(report.get("summary", [])))
    theme_options = sorted({theme for item in items for theme in item.get("themes", [])})
    groups = [
        {
            "category": category["name"],
            "themes": [
                {
                    "theme": theme["theme"],
                    "count": theme["count"],
                }
                for theme in category["themes"]
            ],
        }
        for category in categories
    ]

    return {
        "hero": {
            "title": report.get("title", ""),
            "trade_date": trade_date,
            "summary": report.get("summary", []),
            "summary_lead": report.get("summary", [])[:3],
        },
        "kpis": kpis,
        "highlights": build_highlights(items),
        "market_radar": build_market_radar(items, categories),
        "strategy_panels": build_strategy_panels(report.get("strategy_picks", [])),
        "explorer": {
            "theme_options": theme_options,
            "groups": groups,
            "items": items,
        },
        "appendix": report.get("appendix", []),
        "backtest": build_backtest_payload(backtest),
    }
