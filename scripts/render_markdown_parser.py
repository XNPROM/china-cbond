"""Markdown parser for cbond overview report + helper utilities."""
import html
import re

CATEGORY_ORDER = [
    "科技TMT",
    "新能源电力",
    "高端制造",
    "医药医疗",
    "材料化工资源",
    "消费服务",
    "建筑地产",
    "金融交运公用",
    "其他",
]


def parse_markdown(text):
    lines = text.splitlines()
    report = {
        "title": "",
        "summary": [],
        "theme_index": [],
        "strategy_picks": [],
        "sections": [],
        "appendix": [],
    }
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith("# "):
            report["title"] = line[2:].strip()
            i += 1
            continue
        if line == "## 摘要":
            i += 1
            while i < len(lines) and not lines[i].startswith("## "):
                if lines[i].strip().startswith("- "):
                    report["summary"].append(lines[i].strip()[2:])
                i += 1
            continue
        if line == "## 题材索引":
            i += 1
            while i < len(lines) and not lines[i].startswith("## "):
                cur = lines[i].strip()
                if cur.startswith("- ["):
                    m = re.match(r"- \[(.+?)\]\(#.+?\) \((\d+) 只\)", cur)
                    if m:
                        report["theme_index"].append({"theme": m.group(1), "count": int(m.group(2))})
                i += 1
            continue
        if line == "## 策略推荐":
            i += 1
            current_strat = ""
            current_desc = ""
            while i < len(lines) and not lines[i].startswith("## "):
                cur = lines[i].strip()
                if cur.startswith("### "):
                    current_strat = cur[4:].strip()
                elif cur.startswith("*") and cur.endswith("*"):
                    current_desc = cur.strip("*").strip()
                elif cur.startswith("|") and not cur.startswith("|---"):
                    cells = [c.strip() for c in cur.split("|")[1:-1]]
                    if cells and cells[0] != "排名":
                        report["strategy_picks"].append({
                            "strategy": current_strat,
                            "desc": current_desc,
                            "cells": cells,
                        })
                i += 1
            continue
        if line.startswith("## 附录"):
            i += 1
            while i < len(lines):
                cur = lines[i].strip()
                if cur.startswith("- "):
                    report["appendix"].append(cur[2:])
                i += 1
            continue
        if line.startswith("## "):
            theme = line[3:].strip()
            section = {"theme": theme, "cards": []}
            i += 1
            while i < len(lines) and not lines[i].startswith("## "):
                if lines[i].startswith("### "):
                    card, i = parse_card(lines, i)
                    section["cards"].append(card)
                else:
                    i += 1
            report["sections"].append(section)
            continue
        i += 1
    return report


def parse_card(lines, start_idx):
    title_line = lines[start_idx].strip()[4:]
    m = re.match(r"(.+?) \(([^)]+)\)", title_line)
    bond_name = m.group(1) if m else title_line
    bond_code = m.group(2) if m else ""

    row = []
    i = start_idx + 1
    while i < len(lines):
        cur = lines[i].strip()
        if cur.startswith("|"):
            if cur.startswith("|---"):
                i += 1
                continue
            row.append(cur)
            i += 1
            continue
        if row:
            break
        i += 1

    header_cells = parse_table_row(row[0] if row else "")
    value_cells = parse_table_row(row[-1] if row else "")
    metrics = dict(zip(header_cells, value_cells))
    main_business = ""
    themes = []
    sparkline_data = {}
    while i < len(lines):
        cur = lines[i].strip()
        if cur.startswith("### ") or cur.startswith("## "):
            break
        if cur.startswith("**主营**："):
            main_business = cur.replace("**主营**：", "", 1).strip()
        if cur.startswith("**题材**："):
            themes = re.findall(r"`#([^`]+)`", cur)
        if cur.startswith("**时序**："):
            payload = cur.replace("**时序**：", "", 1).strip()
            try:
                parts = dict(p.split("=", 1) for p in payload.split() if "=" in p)
                sparkline_data = {
                    "dates": parts.get("dates", "").split(","),
                    "delta": [float(v) for v in parts.get("delta", "").split(",") if v],
                    "rv": [float(v) for v in parts.get("rv", "").split(",") if v],
                }
            except (ValueError, KeyError):
                pass
        i += 1

    return {
        "bond_name": bond_name,
        "bond_code": bond_code,
        "stock": metrics.get("正股", ""),
        "industry": metrics.get("申万一级", metrics.get("行业", "")),
        "sw_l2": metrics.get("申万二级", ""),
        "price": metrics.get("价格", ""),
        "day_chg": metrics.get("涨跌幅", ""),
        "conv": metrics.get("转股溢价率", ""),
        "pure": metrics.get("纯债溢价率", ""),
        "vol": metrics.get("20日年化σ", ""),
        "pure_bond_ytm": metrics.get("纯债YTM", ""),
        "relative_value": metrics.get("相对价值", ""),
        "delta": metrics.get("Delta", ""),
        "balance": metrics.get("余额(亿)", ""),
        "rating": metrics.get("评级", ""),
        "maturity": metrics.get("到期", ""),
        "surplus_years": metrics.get("剩余(年)", ""),
        "conv_price": metrics.get("转股价", ""),
        "pe_ttm": metrics.get("PE", ""),
        "pb": metrics.get("PB", ""),
        "total_mv": metrics.get("市值(亿)", ""),
        "call_status": metrics.get("强赎", ""),
        "down_status": metrics.get("下修", ""),
        "strategy": metrics.get("策略分", ""),
        "business": main_business,
        "themes": themes,
        "sparkline": sparkline_data,
    }, i


def parse_table_row(row):
    if not row:
        return []
    return [part.strip() for part in row.strip().strip("|").split("|")]


def categorize_theme(theme):
    if theme.startswith(("半导体", "AI", "消费电子", "XR", "折叠屏", "面板", "信创", "通信", "卫星互联网")):
        return "科技TMT"
    if theme.startswith(("光伏", "储能", "动力电池", "电池", "风电", "氢能", "核电", "电网", "虚拟电厂", "电力", "燃气-水务")):
        return "新能源电力"
    if theme.startswith(("汽车", "新能源车", "智能驾驶", "车载传感器", "一体化压铸", "工控", "机器人", "工程机械", "机床刀具", "激光设备", "3D打印")):
        return "高端制造"
    if theme.startswith(("创新药", "CXO", "原料药", "仿制药", "医疗器械", "中药", "疫苗", "医美", "血制品")):
        return "医药医疗"
    if theme.startswith(("基础化工", "化工", "新材料", "有色", "钢铁", "玻璃基材", "特种纸")):
        return "材料化工资源"
    if theme.startswith(("食品饮料", "农业", "纺织", "家电", "轻工", "商贸零售", "美妆", "免税")):
        return "消费服务"
    if theme.startswith(("建筑装饰", "建材", "基建", "地产")):
        return "建筑地产"
    if theme.startswith(("银行", "证券", "保险", "环保", "物流", "航运", "公路铁路", "航空机场")):
        return "金融交运公用"
    if theme.startswith(("军工", "商业航天", "低空经济")):
        return "科技TMT"
    return "其他"


def build_category_index(sections):
    buckets = {}
    for section in sections:
        category = categorize_theme(section["theme"])
        section["category"] = category
        bucket = buckets.setdefault(category, {"name": category, "themes": [], "bond_count": 0})
        bucket["themes"].append({"theme": section["theme"], "count": len(section["cards"])})
        bucket["bond_count"] += len(section["cards"])
    ordered = []
    for name in CATEGORY_ORDER:
        if name in buckets:
            ordered.append(buckets[name])
    for name, value in buckets.items():
        if name not in CATEGORY_ORDER:
            ordered.append(value)
    return ordered


def compute_kpi_metrics(report):
    all_cards = [c for s in report["sections"] for c in s["cards"]]
    prices, convs, rvs, deltas = [], [], [], []
    for c in all_cards:
        try:
            prices.append(float(c.get("price", "0").replace(",", "")))
        except (ValueError, TypeError):
            pass
        try:
            convs.append(float(c.get("conv", "0%").replace("%", "")))
        except (ValueError, TypeError):
            pass
        try:
            rv = float(c.get("relative_value", "0"))
            if 0 < rv < 3:
                rvs.append(rv)
        except (ValueError, TypeError):
            pass
        try:
            d = float(c.get("delta", "0"))
            if 0 <= d <= 1:
                deltas.append(d)
        except (ValueError, TypeError):
            pass

    n = len(all_cards)
    return {
        "total": n,
        "avg_price": round(sum(prices) / len(prices), 1) if prices else 0,
        "median_conv": round(sorted(convs)[len(convs) // 2], 1) if convs else 0,
        "median_rv": round(sorted(rvs)[len(rvs) // 2], 2) if rvs else 0,
        "undervalued": sum(1 for v in rvs if v < 1.0),
        "n_equity": sum(1 for d in deltas if d >= 0.7),
        "n_balanced": sum(1 for d in deltas if 0.4 <= d < 0.7),
        "n_debt": sum(1 for d in deltas if d < 0.4),
    }


def slugify(text):
    return re.sub(r"[^\w一-鿿-]+", "-", text).strip("-") or "section"


def num_value(text):
    m = re.search(r"-?\d+(?:\.\d+)?", text or "")
    return m.group(0) if m else ""


def parse_stock(text):
    m = re.match(r"(.+?) \(([^)]+)\)", text or "")
    if m:
        return m.group(1), m.group(2)
    return text or "", ""


def signed_class(text):
    value = num_value(text)
    if not value:
        return "flat"
    try:
        num = float(value)
    except Exception:
        return "flat"
    if num > 0:
        return "up"
    if num < 0:
        return "down"
    return "flat"


def render_sparkline(values, width=70, height=22, color="#3b82f6"):
    if not values or len(values) < 2:
        return ""
    mn, mx = min(values), max(values)
    rng = mx - mn
    if rng < 1e-9:
        rng = 1.0
    pts = []
    for i, v in enumerate(values):
        x = (i / (len(values) - 1)) * (width - 4) + 2
        y = height - 2 - ((v - mn) / rng) * (height - 6) - 2
        pts.append(f"{x:.1f},{y:.1f}")
    fill_pts = pts + [f"{width - 2},{height - 2}", f"2,{height - 2}"]
    return (
        f'<svg width="{width}" height="{height}" style="vertical-align:middle" '
        f'viewBox="0 0 {width} {height}">'
        f'<defs><linearGradient id="sg{abs(hash(tuple(values)))%9999}" x1="0" y1="0" x2="0" y2="1">'
        f'<stop offset="0%" stop-color="{color}" stop-opacity="0.25"/>'
        f'<stop offset="100%" stop-color="{color}" stop-opacity="0.02"/>'
        f'</linearGradient></defs>'
        f'<polygon points="{" ".join(fill_pts)}" fill="url(#sg{abs(hash(tuple(values)))%9999})"/>'
        f'<polyline points="{" ".join(pts)}" fill="none" stroke="{color}" stroke-width="1.5" stroke-linejoin="round"/>'
        f'<circle cx="{pts[-1].split(",")[0]}" cy="{pts[-1].split(",")[1]}" r="2.5" fill="{color}"/>'
        f'</svg>'
    )
