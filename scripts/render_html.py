"""Render the overview Markdown to a polished, interactive HTML report."""
import argparse
import html
import os
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


CSS = r"""
:root{
  --bg:#fff;
  --text:#1a1a1a;
  --muted:#888;
  --border:#e5e5e5;
  --accent:#2563eb;
  --red:#dc2626;
  --green:#16a34a;
  --gold:#b8860b;
  --font:-apple-system,"Segoe UI","Microsoft YaHei","PingFang SC",sans-serif;
}
*{box-sizing:border-box;margin:0;padding:0}
html{scroll-behavior:smooth}
body{font:14px/1.6 var(--font);color:var(--text);background:var(--bg);-webkit-font-smoothing:antialiased}

/* ---- Header ---- */
header{max-width:1200px;margin:0 auto;padding:32px 24px 20px;border-bottom:1px solid var(--border)}
header h1{font-size:22px;font-weight:700;margin:0 0 4px}
.subtitle{color:var(--muted);font-size:13px;margin:0 0 16px}
.controls{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.controls input[type=search]{
  flex:1;min-width:200px;padding:7px 12px;border:1px solid var(--border);border-radius:6px;
  font:13px/1.4 var(--font);outline:none;
}
.controls input[type=search]:focus{border-color:var(--accent)}
.controls select{
  padding:7px 8px;border:1px solid var(--border);border-radius:6px;font:13px/1.4 var(--font);
  background:var(--bg);cursor:pointer;
}
.ftag{
  padding:5px 10px;border:1px solid var(--border);border-radius:4px;font-size:12px;
  cursor:pointer;background:var(--bg);transition:border-color .15s;
}
.ftag.is-active{border-color:var(--accent);color:var(--accent);font-weight:600}
.ftag:hover{border-color:#999}
.actions{margin-left:auto;display:flex;gap:6px}
.abtn{
  padding:5px 10px;border:1px solid var(--border);border-radius:4px;font-size:12px;
  cursor:pointer;background:var(--bg);color:var(--text);
}
.abtn:hover{background:#f5f5f5}

/* ---- Main ---- */
.main{max-width:1200px;margin:0 auto;padding:0 24px}
.result-info{padding:12px 0;font-size:12px;color:var(--muted);border-bottom:1px solid var(--border);margin-bottom:24px}

/* ---- Strategy ---- */
.strategy-section{margin:0 0 32px}
.strategy-section h2{font-size:16px;font-weight:600;padding:12px 0 8px;border-bottom:2px solid var(--text)}
.strat-card{margin:0 0 20px;padding:12px 16px;border:1px solid var(--border);border-radius:8px;background:#fafafa}
.strat-card h3{font-size:14px;font-weight:600;margin:0 0 4px}
.strat-card .desc{font-size:12px;color:var(--muted);margin:0 0 10px}
.stable{width:100%;border-collapse:collapse;font-size:13px}
.stable th{text-align:left;padding:6px 8px;border-bottom:2px solid var(--border);color:var(--muted);font-weight:500;font-size:12px;white-space:nowrap}
.stable td{padding:5px 8px;border-bottom:1px solid var(--border);white-space:nowrap}
.stable .srank{color:var(--gold);font-weight:700;font-size:12px}
.stable .sname{font-weight:600}

/* ---- Theme groups ---- */
.group{margin:0 0 28px}
.group-head{display:flex;align-items:baseline;gap:8px;padding:8px 0;border-bottom:2px solid var(--text);cursor:pointer;user-select:none}
.group-head h3{font-size:15px;font-weight:600}
.group-head .cnt{font-size:12px;color:var(--muted)}
.group-head .toggle{font-size:11px;color:var(--accent);margin-left:auto}
.group-body{overflow-x:auto}
.group-body.collapsed{display:none}

/* ---- Bond table ---- */
.btable{width:100%;border-collapse:collapse;font-size:13px}
.btable th{text-align:left;padding:6px 8px;border-bottom:1px solid var(--border);color:var(--muted);font-weight:500;font-size:11px;white-space:nowrap;position:sticky;top:0;background:var(--bg);z-index:1}
.btable th.num,.btable td.num{text-align:right;white-space:nowrap}
.btable td{padding:6px 8px;border-bottom:1px solid #f0f0f0;vertical-align:middle}
.btable tr:hover{background:#fafafa}
.btable .bname{font-weight:600;white-space:nowrap}
.btable .bname small{display:block;font-weight:400;color:var(--muted);font-size:11px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:160px}
.btable .bprice{font-weight:700;font-size:14px;white-space:nowrap}
.btable .up{color:var(--red)}
.btable .down{color:var(--green)}
.btable .flat{color:var(--muted)}
.btable .bstrat{color:var(--gold);font-weight:600;font-size:12px}
.btable .rv-low{color:var(--green);font-weight:600}
.btable .rv-high{color:var(--red);font-weight:600}
.btable .sector-badge{display:inline-block;padding:0 4px;border-radius:2px;font-size:10px;font-weight:600;vertical-align:middle;margin-left:4px}
.btable .sector-equity{background:#e8f5e9;color:#2e7d32}
.btable .sector-balanced{background:#fff3e0;color:#e65100}
.btable .sector-debt{background:#e3f2fd;color:#1565c0}
.btable .bcode{color:var(--muted);font-size:11px;font-family:monospace;white-space:nowrap}
.btable .bbiz{color:var(--muted);font-size:12px;max-width:180px;overflow:hidden;text-overflow:ellipsis;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;white-space:normal;line-height:1.3}
.btable .bthemes{white-space:normal;line-height:1.3;max-width:140px}
.btable .bthemes span{display:inline-block;padding:1px 5px;margin:1px 2px;border:1px solid var(--border);border-radius:3px;font-size:10px;color:var(--muted)}

/* ---- Empty state ---- */
.empty{display:none;text-align:center;padding:40px 20px;color:var(--muted);font-size:14px}

/* ---- Footer ---- */
footer{max-width:1200px;margin:32px auto 0;padding:20px 24px;border-top:1px solid var(--border);color:var(--muted);font-size:12px;line-height:1.6}
footer h2{font-size:14px;font-weight:600;color:var(--text);margin:0 0 8px}

/* ---- Responsive ---- */
@media(max-width:900px){
  .controls{flex-direction:column;align-items:stretch}
  .actions{margin-left:0}
  .btable{font-size:12px}
  .btable td,.btable th{padding:4px 6px}
}
@media(max-width:640px){
  header{padding:20px 16px 16px}
  .main{padding:0 16px}
  .btable .bbiz{display:none}
  .btable .bthemes{display:none}
}
"""


JS = r"""
const state = { query: "", quick: "all", sort: "default" };
const searchInput = document.querySelector("#search");
const sortSelect = document.querySelector("#sort");
const resultCount = document.querySelector("#resultCount");
const emptyEl = document.querySelector("#empty");
const quickButtons = [...document.querySelectorAll("[data-quick]")];
const groups = [...document.querySelectorAll(".group")];
const bondRows = [...document.querySelectorAll(".bond-row")];

function toNumber(v) { const n = parseFloat(v || "0"); return Number.isFinite(n) ? n : 0; }
function normQ(v) { return String(v||"").toLowerCase().replace(/[\s_\/()（）#`]+/g," ").replace(/\.(sh|sz|bj)\b/g,"").trim(); }

function matchQuick(row) {
  if (state.quick === "all") return true;
  const p = toNumber(row.dataset.price);
  const c = toNumber(row.dataset.conv);
  const v = toNumber(row.dataset.vol);
  if (state.quick === "highPrice") return p > 130;
  if (state.quick === "lowPrice") return p < 100;
  if (state.quick === "lowPremium") return c < 20;
  if (state.quick === "midPremium") return c >= 20 && c < 50;
  if (state.quick === "highPremium") return c >= 50;
  if (state.quick === "highVol") return v > 1.0;
  if (state.quick === "lowVol") return v < 0.5;
  return true;
}
function matchQuery(row) { return !state.query || (row.dataset.search||"").includes(state.query); }

function applyFilters() {
  let vis = 0;
  bondRows.forEach(row => {
    const show = matchQuick(row) && matchQuery(row);
    row.hidden = !show;
    if (show) vis++;
  });
  groups.forEach(g => {
    const hasVis = [...g.querySelectorAll(".bond-row")].some(r => !r.hidden);
    g.hidden = !hasVis;
  });
  resultCount.textContent = String(vis);
  emptyEl.style.display = vis ? "none" : "block";
}

function sortRows() {
  groups.forEach(g => {
    const tbody = g.querySelector("tbody");
    if (!tbody) return;
    const rows = [...tbody.querySelectorAll(".bond-row")];
    rows.sort((a, b) => {
      if (state.sort === "priceDesc") return toNumber(b.dataset.price) - toNumber(a.dataset.price);
      if (state.sort === "convAsc") return toNumber(a.dataset.conv) - toNumber(b.dataset.conv);
      if (state.sort === "dayChgDesc") return toNumber(b.dataset.daychg) - toNumber(a.dataset.daychg);
      if (state.sort === "dayChgAsc") return toNumber(a.dataset.daychg) - toNumber(b.dataset.daychg);
      if (state.sort === "volDesc") return toNumber(b.dataset.vol) - toNumber(a.dataset.vol);
      if (state.sort === "balanceDesc") return toNumber(b.dataset.balance) - toNumber(a.dataset.balance);
      return toNumber(a.dataset.order) - toNumber(b.dataset.order);
    });
    rows.forEach(r => tbody.appendChild(r));
  });
}

searchInput.addEventListener("input", e => { state.query = normQ(e.target.value); applyFilters(); });
sortSelect.addEventListener("change", e => { state.sort = e.target.value; sortRows(); applyFilters(); });
quickButtons.forEach(btn => {
  btn.addEventListener("click", () => {
    state.quick = btn.dataset.quick;
    quickButtons.forEach(b => b.classList.toggle("is-active", b === btn));
    applyFilters();
  });
});
document.querySelectorAll(".group-head").forEach(h => {
  h.addEventListener("click", () => {
    const body = h.nextElementSibling;
    body.classList.toggle("collapsed");
    h.querySelector(".toggle").textContent = body.classList.contains("collapsed") ? "展开" : "收起";
  });
});
document.querySelector("#copyCodes").addEventListener("click", async () => {
  const codes = bondRows.filter(r => !r.hidden).map(r => r.dataset.bondCode).filter(Boolean);
  if (!codes.length) return;
  try {
    await navigator.clipboard.writeText(codes.join("\n"));
    document.querySelector("#exportStatus").textContent = "已复制 " + codes.length + " 个代码";
    setTimeout(() => document.querySelector("#exportStatus").textContent = "", 2000);
  } catch(_) {
    document.querySelector("#exportStatus").textContent = "复制失败，请手动复制";
    setTimeout(() => document.querySelector("#exportStatus").textContent = "", 2000);
  }
});
document.querySelector("#exportCsv").addEventListener("click", () => {
  const vis = bondRows.filter(r => !r.hidden);
  if (!vis.length) return;
  const esc = v => { const s = String(v||""); return s.includes(",")||s.includes('"')||s.includes("\n") ? '"'+s.replace(/"/g,'""')+'"' : s; };
  const h = "bond_code,bond_name,stock_code,stock_name,price,day_chg,conv_prem,pure_prem,vol,balance,rating,maturity";
  const rows = vis.map(r => [r.dataset.bondCode,r.dataset.bondName,r.dataset.stockCode,r.dataset.stockName,r.dataset.price,r.dataset.daychg,r.dataset.conv,r.dataset.pure,r.dataset.vol,r.dataset.balance,r.dataset.rating,r.dataset.maturity].map(esc).join(","));
  const blob = new Blob(["﻿"+h+"\n"+rows.join("\n")], {type:"text/csv;charset=utf-8;"});
  const a = document.createElement("a"); a.href = URL.createObjectURL(blob); a.download = "cbond_"+Date.now()+".csv";
  document.body.appendChild(a); a.click(); a.remove(); URL.revokeObjectURL(a.href);
  document.querySelector("#exportStatus").textContent = "已导出 " + vis.length + " 条";
  setTimeout(() => document.querySelector("#exportStatus").textContent = "", 2000);
});
sortRows(); applyFilters();
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--title", default="可转债概览")
    ap.add_argument("--trade-date", default="", help="YYYY-MM-DD shown in header")
    args = ap.parse_args()

    with open(args.inp, encoding="utf-8") as f:
        report = parse_markdown(f.read())
    html_out = build_html(report, args.title, args.trade_date)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(html_out)
    print(f"[done] → {args.out} ({os.path.getsize(args.out)} bytes)")


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
                            "cells": cells
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
    while i < len(lines):
        cur = lines[i].strip()
        if cur.startswith("### ") or cur.startswith("## "):
            break
        if cur.startswith("**主营**："):
            main_business = cur.replace("**主营**：", "", 1).strip()
        if cur.startswith("**题材**："):
            themes = re.findall(r"`#([^`]+)`", cur)
        i += 1

    return {
        "bond_name": bond_name,
        "bond_code": bond_code,
        "stock": metrics.get("正股", ""),
        "industry": metrics.get("行业", ""),
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
        "strategy": metrics.get("策略分", ""),
        "business": main_business,
        "themes": themes,
    }, i


def parse_table_row(row):
    if not row:
        return []
    return [part.strip() for part in row.strip().strip("|").split("|")]


def slugify(text):
    return re.sub(r"[^\w一-鿿-]+", "-", text).strip("-") or "section"


def search_blob(card, theme):
    stock_name, stock_code = parse_stock(card["stock"])
    bond_code_short = normalize_code(card["bond_code"])
    stock_code_short = normalize_code(stock_code)
    parts = [
        theme, card["bond_name"], card["bond_code"], bond_code_short,
        card["stock"], stock_name, stock_code, stock_code_short,
        card["industry"], card["business"], " ".join(card["themes"]),
    ]
    return html.escape(normalize_search_text(" ".join(parts)), quote=True)


def normalize_code(text):
    return re.sub(r"\.(SH|SZ|BJ)$", "", (text or "").strip(), flags=re.I)


def normalize_search_text(text):
    text = (text or "").lower()
    text = re.sub(r"[\s_/()（）#`]+", " ", text)
    text = text.replace(".sh", "").replace(".sz", "").replace(".bj", "")
    return text


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


def render_strategy(strategy_picks):
    if not strategy_picks:
        return ""
    # Group by strategy
    by_strat = {}
    for item in strategy_picks:
        s = item.get("strategy", "")
        by_strat.setdefault(s, {"desc": item.get("desc", ""), "rows": []})
        by_strat[s]["rows"].append(item["cells"])

    STRAT_META = {
        "双低": {"icon": "📊"},
        "双低-偏股": {"icon": "📈"},
        "双低-平衡": {"icon": "⚖️"},
        "双低-偏债": {"icon": "🛡️"},
        "低估": {"icon": "💎"},
    }

    sections_html = ""
    for strat_name, data in by_strat.items():
        meta = STRAT_META.get(strat_name, {"icon": "📋", "color": "#666"})
        rows_html = ""
        for row in data["rows"]:
            rank, bond, stock, price, conv_prem, pe, vol, score = (row + [""] * 8)[:8]
            rows_html += (
                f'<tr>'
                f'<td class="srank">#{html.escape(rank)}</td>'
                f'<td class="sname">{html.escape(bond)}</td>'
                f'<td>{html.escape(stock)}</td>'
                f'<td>{html.escape(price)}</td>'
                f'<td>{html.escape(conv_prem)}</td>'
                f'<td>{html.escape(pe)}</td>'
                f'<td>{html.escape(vol)}</td>'
                f'<td>{html.escape(score)}</td>'
                f'</tr>'
            )
        sections_html += (
            f'<div class="strat-card">'
            f'<h3><span style="font-size:18px">{meta["icon"]}</span> {html.escape(strat_name)}</h3>'
            f'<p class="desc">{html.escape(data["desc"])}</p>'
            f'<table class="stable"><tr><th>#</th><th>转债</th><th>正股</th><th>价格</th><th>转股溢价率</th><th>PE</th><th>20日σ</th><th>得分</th></tr>'
            f'{rows_html}</table></div>'
        )

    return (
        '<section class="strategy-section" id="策略推荐">'
        f'<h2>策略推荐</h2>'
        f'{sections_html}</section>'
    )


def render_group(section, idx):
    theme = section["theme"]
    sid = slugify(theme)
    cards = section["cards"]
    if not cards:
        return ""
    rows_html = ""
    for ci, card in enumerate(cards):
        stock_name, stock_code = parse_stock(card["stock"])
        sc = signed_class(card["day_chg"])
        chg_text = card["day_chg"] or "--"
        strat_html = f'<span class="bstrat">{html.escape(card["strategy"])}</span>' if card.get("strategy") else ""
        themes_html = "".join(f'<span>{html.escape(t)}</span>' for t in card["themes"])
        # Sector badge from conv premium
        sector_badge = ""
        try:
            conv_val = float(card.get("conv", "").replace("%", ""))
            if conv_val < 20:
                sector_badge = '<span class="sector-badge sector-equity">偏股</span>'
            elif conv_val < 50:
                sector_badge = '<span class="sector-badge sector-balanced">平衡</span>'
            else:
                sector_badge = '<span class="sector-badge sector-debt">偏债</span>'
        except (ValueError, TypeError):
            pass
        # Relative value color coding
        rv_raw = card.get("relative_value", "")
        try:
            rv_val = float(rv_raw)
            if rv_val < 1.0:
                rv_class, rv_text = "rv-low", f"{rv_val:.2f}"
            elif rv_val > 1.2:
                rv_class, rv_text = "rv-high", f"{rv_val:.2f}"
            else:
                rv_class, rv_text = "", f"{rv_val:.2f}"
        except (ValueError, TypeError):
            rv_class, rv_text = "", rv_raw or ""
        # Delta formatting
        delta_raw = card.get("delta", "")
        try:
            delta_val = float(delta_raw)
            delta_text = f"{delta_val:.2f}"
        except (ValueError, TypeError):
            delta_text = delta_raw or ""
        rows_html += (
            f'<tr class="bond-row" data-search="{search_blob(card, theme)}" '
            f'data-price="{num_value(card["price"])}" data-daychg="{num_value(card["day_chg"])}" '
            f'data-conv="{num_value(card["conv"])}" data-vol="{num_value(card["vol"])}" '
            f'data-balance="{num_value(card["balance"])}" data-order="{ci}" '
            f'data-bond-code="{html.escape(card["bond_code"], quote=True)}" '
            f'data-bond-name="{html.escape(card["bond_name"], quote=True)}" '
            f'data-stock-code="{html.escape(stock_code, quote=True)}" '
            f'data-stock-name="{html.escape(stock_name, quote=True)}" '
            f'data-rating="{html.escape(card["rating"], quote=True)}" '
            f'data-maturity="{html.escape(card["maturity"], quote=True)}">'
            f'<td class="bname">{html.escape(card["bond_name"])}{sector_badge}<small>{html.escape(stock_name)} · {html.escape(card["industry"])}</small></td>'
            f'<td class="bcode">{html.escape(card["bond_code"])}</td>'
            f'<td class="bprice">{html.escape(card["price"])}<br><span class="{sc}" style="font-size:11px;font-weight:400">{html.escape(chg_text)}</span></td>'
            f'<td class="num">{html.escape(card["conv"])}</td>'
            f'<td class="num">{html.escape(card["pure"])}</td>'
            f'<td class="num">{html.escape(card["vol"])}</td>'
            f'<td class="num">{html.escape(card.get("pure_bond_ytm", ""))}</td>'
            f'<td class="num {rv_class}">{html.escape(rv_text)}</td>'
            f'<td class="num">{html.escape(delta_text)}</td>'
            f'<td class="num">{html.escape(card["balance"])}</td>'
            f'<td>{html.escape(card["rating"])}</td>'
            f'<td>{html.escape(card["maturity"])}</td>'
            f'<td>{strat_html}</td>'
            f'<td class="bbiz">{html.escape(card.get("business", ""))}</td>'
            f'<td class="bthemes">{themes_html}</td>'
            f'</tr>'
        )
    return (
        f'<div class="group" id="{sid}">'
        f'<div class="group-head"><h3>{html.escape(theme)}</h3><span class="cnt">({len(cards)})</span><span class="toggle">收起</span></div>'
        f'<div class="group-body">'
        f'<table class="btable"><thead><tr>'
        '<th>转债</th><th>代码</th><th>价格/涨跌</th><th class="num">转股溢价率</th><th class="num">纯债溢价率</th><th class="num">20日σ</th><th class="num">纯债YTM</th><th class="num">相对价值</th><th class="num">Delta</th><th class="num">余额(亿)</th><th>评级</th><th>到期</th><th>策略</th><th>主营</th><th>题材</th>'
        '</tr></thead><tbody>'
        f'{rows_html}</tbody></table></div></div>'
    )


def build_html(report, title, trade_date=""):
    build_category_index(report["sections"])
    date_display = trade_date or (report["title"].split("·")[-1].strip() if "·" in report.get("title", "") else "")
    groups_html = "".join(render_group(s, i) for i, s in enumerate(report["sections"]))
    strategy_html = render_strategy(report["strategy_picks"])
    appendix_html = ""
    if report["appendix"]:
        items = "".join(f"<li>{html.escape(line)}</li>" for line in report["appendix"])
        appendix_html = f'<footer><h2>附录 · 字段说明</h2><ul>{items}</ul></footer>'

    summary_text = " · ".join(html.escape(s) for s in report["summary"][:3]) if report["summary"] else ""

    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{html.escape(title)}</title>
<style>{CSS}</style>
</head>
<body>
<header>
  <h1>{html.escape(report["title"] or title)}</h1>
  <p class="subtitle">{summary_text} · 数据截至 {html.escape(date_display)}</p>
  <div class="controls">
    <input id="search" type="search" placeholder="搜索转债、正股、代码、题材...">
    <select id="sort">
      <option value="default">原始顺序</option>
      <option value="priceDesc">价格 高→低</option>
      <option value="convAsc">溢价率 低→高</option>
      <option value="dayChgDesc">涨幅 高→低</option>
      <option value="dayChgAsc">涨幅 低→高</option>
      <option value="volDesc">波动率 高→低</option>
      <option value="balanceDesc">余额 高→低</option>
    </select>
    <button class="ftag is-active" type="button" data-quick="all">全部</button>
    <button class="ftag" type="button" data-quick="highPrice">价格&gt;130</button>
    <button class="ftag" type="button" data-quick="lowPrice">价格&lt;100</button>
    <button class="ftag" type="button" data-quick="lowPremium">偏股(&lt;20%)</button>
    <button class="ftag" type="button" data-quick="midPremium">平衡(20-50%)</button>
    <button class="ftag" type="button" data-quick="highPremium">偏债(&ge;50%)</button>
    <div class="actions">
      <button class="abtn" id="copyCodes" type="button">复制代码</button>
      <button class="abtn" id="exportCsv" type="button">导出CSV</button>
    </div>
  </div>
</header>

<div class="main">
  <div class="result-info">当前命中 <strong id="resultCount">0</strong> 只转债 <span id="exportStatus"></span></div>
  <div id="empty" class="empty">没有匹配结果</div>
  {strategy_html}
  {groups_html}
</div>
{appendix_html}

<script>{JS}</script>
</body>
</html>"""


if __name__ == "__main__":
    main()
