"""Render the overview Markdown to a modern, interactive HTML dashboard."""
import argparse
import html
import json
import os

from jinja2 import Environment, FileSystemLoader
from render_markdown_parser import (
    build_category_index,
    categorize_theme,
    compute_kpi_metrics,
    num_value,
    parse_markdown,
    parse_stock,
    render_sparkline,
    signed_class,
    slugify,
)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

STRAT_ICONS = {
    "双低": "📊",
    "双低-偏股": "📈",
    "双低-平衡": "⚖️",
    "双低-偏债": "🛡️",
    "低估": "💎",
}


def enrich_card(card, theme, idx):
    """Add computed display fields to a card dict."""
    card["idx"] = idx
    stock_name, stock_code = parse_stock(card["stock"])
    card["stock_name"] = stock_name
    card["stock_code"] = stock_code
    card["chg_class"] = signed_class(card["day_chg"])

    # Sector badge
    try:
        conv_val = float(card.get("conv", "0%").replace("%", ""))
        if conv_val < 20:
            card["sector_badge"] = '<span class="sector-badge sector-equity">偏股</span>'
        elif conv_val < 50:
            card["sector_badge"] = '<span class="sector-badge sector-balanced">平衡</span>'
        else:
            card["sector_badge"] = '<span class="sector-badge sector-debt">偏债</span>'
    except (ValueError, TypeError):
        card["sector_badge"] = ""

    # Relative value
    rv_raw = card.get("relative_value", "")
    try:
        rv_val = float(rv_raw)
        card["rv_text"] = f"{rv_val:.2f}"
        card["rv_class"] = "rv-low" if rv_val < 1.0 else ("rv-high" if rv_val > 1.2 else "")
    except (ValueError, TypeError):
        card["rv_text"] = rv_raw or ""
        card["rv_class"] = ""

    # Delta
    delta_raw = card.get("delta", "")
    try:
        card["delta_text"] = f"{float(delta_raw):.2f}"
    except (ValueError, TypeError):
        card["delta_text"] = delta_raw or ""

    # Sparklines
    sp = card.get("sparkline", {})
    card["rv_spark"] = render_sparkline(sp.get("rv", []), color="#22c55e")
    card["delta_spark"] = render_sparkline(sp.get("delta", []), color="#3b82f6")

    # Call / Down badges
    call_raw = card.get("call_status", "")
    if call_raw:
        if "强赎停牌" in call_raw:
            card["call_html"] = f'<span class="call-badge call-danger">{html.escape(call_raw)}</span>'
        elif "不强赎" in call_raw:
            card["call_html"] = f'<span class="call-badge call-safe">{html.escape(call_raw)}</span>'
        elif "触发" in call_raw:
            card["call_html"] = f'<span class="call-badge call-warn">{html.escape(call_raw)}</span>'
        else:
            card["call_html"] = html.escape(call_raw)
    else:
        card["call_html"] = ""

    down_raw = card.get("down_status", "")
    card["down_html"] = f'<span class="down-badge down-safe">{html.escape(down_raw)}</span>' if down_raw else ""

    return card


def build_bond_data(report):
    """Build a flat list for the JS data model."""
    items = []
    idx = 0
    for section in report["sections"]:
        for card in section["cards"]:
            stock_name, stock_code = parse_stock(card["stock"])
            search_parts = [
                section["theme"], card["bond_name"], card["bond_code"],
                stock_name, stock_code, card["industry"],
                card.get("business", ""), " ".join(card.get("themes", [])),
            ]
            search_text = " ".join(search_parts).lower()
            search_text = search_text.replace(".sh", "").replace(".sz", "").replace(".bj", "")

            try:
                conv_val = float(card.get("conv", "0%").replace("%", ""))
                sector = "偏股" if conv_val < 20 else ("平衡" if conv_val < 50 else "偏债")
            except (ValueError, TypeError):
                sector = ""

            items.append({
                "idx": idx,
                "bond_code": card["bond_code"],
                "bond_name": card["bond_name"],
                "stock_code": stock_code,
                "stock_name": stock_name,
                "price": num_value(card["price"]),
                "day_chg": num_value(card["day_chg"]),
                "conv": num_value(card["conv"]),
                "pure": num_value(card["pure"]),
                "vol": num_value(card["vol"]),
                "pure_bond_ytm": card.get("pure_bond_ytm", ""),
                "relative_value": card.get("relative_value", ""),
                "rv": num_value(card.get("relative_value", "")),
                "delta": num_value(card.get("delta", "")),
                "balance": num_value(card["balance"]),
                "rating": card.get("rating", ""),
                "maturity": card.get("maturity", ""),
                "strategy": card.get("strategy", ""),
                "themes": card.get("themes", []),
                "sector": sector,
                "theme_group": section["theme"],
                "search_text": search_text,
            })
            idx += 1
    return items


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--title", default="可转债概览")
    ap.add_argument("--trade-date", default="")
    ap.add_argument("--backtest", default="")
    args = ap.parse_args()

    with open(args.inp, encoding="utf-8") as f:
        report = parse_markdown(f.read())

    category_index = build_category_index(report["sections"])
    kpi = compute_kpi_metrics(report)

    # Enrich cards with display fields
    idx = 0
    for section in report["sections"]:
        for card in section["cards"]:
            enrich_card(card, section["theme"], idx)
            idx += 1

    # Build sections_by_theme for template lookup
    sections_by_theme = {s["theme"]: s for s in report["sections"]}

    # Build strategy data
    by_strat = {}
    for item in report["strategy_picks"]:
        s = item.get("strategy", "")
        by_strat.setdefault(s, {"desc": item.get("desc", ""), "rows": []})
        by_strat[s]["rows"].append((item["cells"] + [""] * 8)[:8])

    # Load backtest
    backtest = None
    if args.backtest:
        with open(args.backtest, encoding="utf-8") as f:
            backtest = json.load(f)

    # Build JS data model
    bond_data = build_bond_data(report)

    # Collect unique themes
    all_themes = sorted({t for s in report["sections"] for c in s["cards"] for t in c.get("themes", [])})

    # Read static files
    with open(os.path.join(SCRIPT_DIR, "static", "style.css"), encoding="utf-8") as f:
        css = f.read()
    with open(os.path.join(SCRIPT_DIR, "static", "app.js"), encoding="utf-8") as f:
        js = f.read()

    # Date display
    date_display = args.trade_date or (report["title"].split("·")[-1].strip() if "·" in report.get("title", "") else "")
    summary_text = " · ".join(html.escape(s) for s in report["summary"][:3]) if report["summary"] else ""

    # Render template
    env = Environment(loader=FileSystemLoader(os.path.join(SCRIPT_DIR, "templates")))
    tpl = env.get_template("base.html.j2")

    html_out = tpl.render(
        title=args.title,
        report=report,
        date_display=date_display,
        summary_text=summary_text,
        kpi=kpi,
        category_index=category_index,
        sections_by_theme=sections_by_theme,
        strategy_picks=by_strat,
        strat_icons=STRAT_ICONS,
        backtest=backtest,
        appendix=report.get("appendix", []),
        all_themes=all_themes,
        bond_data_json=json.dumps(bond_data, ensure_ascii=False),
        backtest_json=json.dumps(backtest or {}, ensure_ascii=False),
        css=css,
        js=js,
    )

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(html_out)
    print(f"[done] → {args.out} ({os.path.getsize(args.out)} bytes)")


if __name__ == "__main__":
    main()
