"""Build the standalone strategy recommendation page (MD + HTML).

Reads `strategy_picks` joined with valuation_daily / universe / themes,
outputs one MD with 5 strategy tables and one self-contained HTML that
reuses the CSS palette from render_html.py.

Usage:
  python3 scripts/build_strategy_page.py \\
    --trade-date 2026-04-20 \\
    --out-md   reports/2026-04-20/cbond_strategy.md \\
    --out-html reports/2026-04-20/cbond_strategy.html \\
    --title-date 2026-04-20
"""
import argparse
import html
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
from _db import connect
from render_html import CSS  # reuse palette / tokens


STRAT_ORDER = ["双低", "双低-偏股", "双低-平衡", "双低-偏债", "低估"]

STRAT_META = {
    "双低":     {"color": "#ae3f2f", "desc": "PE>0 + 波动率>Q1 过滤后，按 1.5×转股溢价率排名 + 价格排名，取综合分最低 30 只。"},
    "双低-偏股": {"color": "#d47a44", "desc": "转股溢价率<20% 的偏股型双低，取分域综合分最低 10 只。"},
    "双低-平衡": {"color": "#205b53", "desc": "转股溢价率 20%~50% 的平衡型双低，取分域综合分最低 10 只。"},
    "双低-偏债": {"color": "#3a6ea5", "desc": "转股溢价率≥50% 的偏债型双低，取分域综合分最低 10 只。"},
    "低估":     {"color": "#b78935", "desc": "BS 相对价值在 [0.5, 2.0] 合理范围内，取相对价值最小 10 只（市场价 / BS 理论价）。"},
}


# ---------- formatting helpers ----------

def _fmt_num(x, digits=2):
    return "" if x is None else f"{x:.{digits}f}"


def _fmt_pct(x):
    return "" if x is None else f"{x:.2f}%"


def _fmt_signed_pct(x):
    if x is None:
        return ""
    return f"+{x:.2f}%" if x > 0 else f"{x:.2f}%"


def _fmt_vol(x):
    """vol stored as decimal (0.3452) → display as 34.52%."""
    return "" if x is None else f"{x * 100:.2f}%"


def _fmt_rv(x):
    return "" if x is None else f"{x:.3f}"


def _signed_class(x):
    if x is None:
        return "is-flat"
    if x > 0:
        return "is-up"
    if x < 0:
        return "is-down"
    return "is-flat"


# ---------- data loading ----------

def load_picks(con, trade_date):
    """Return { strategy_name: [row, ...] } ordered by rank_overall."""
    sql = """
      SELECT sp.strategy, sp.rank_overall, sp.rank_conv_prem, sp.rank_price, sp.note,
             sp.code, u.name AS bond_name, u.uname, u.ucode,
             vd.price, vd.change_pct, vd.conv_prem_pct, vd.pure_prem_pct,
             vd.relative_value, vd.bs_delta, vd.pe_ttm, vd.implied_vol,
             vd.surplus_years, vd.pure_bond_ytm, vd.total_mv_yi, vd.outstanding_yi,
             vd.rating, vd.ths_industry,
             t.theme_l1, t.industry AS theme_industry
        FROM strategy_picks sp
        LEFT JOIN universe u        ON u.code = sp.code
        LEFT JOIN valuation_daily vd ON vd.code = sp.code AND vd.trade_date = sp.trade_date
        LEFT JOIN themes t          ON t.code = sp.code  AND t.trade_date  = sp.trade_date
       WHERE sp.trade_date = ?
       ORDER BY sp.strategy, sp.rank_overall
    """
    rows = con.execute(sql, [trade_date]).fetchall()
    cols = [d[0] for d in con.description]
    groups = defaultdict(list)
    for r in rows:
        d = dict(zip(cols, r))
        groups[d["strategy"]].append(d)
    return groups


# ---------- MD writer ----------

def write_md(groups, trade_date, title_date, out_path):
    lines = []
    lines.append(f"# 可转债策略推荐 · {title_date}")
    lines.append("")
    lines.append(f"数据时点：{title_date} 收盘。点击 [← 返回总览](cbond_overview.html) 浏览全部转债。")
    lines.append("")
    lines.append("## 策略概览")
    for name in STRAT_ORDER:
        cnt = len(groups.get(name, []))
        lines.append(f"- **{name}**（{cnt} 只）：{STRAT_META[name]['desc']}")
    lines.append("")

    for name in STRAT_ORDER:
        picks = groups.get(name, [])
        if not picks:
            continue
        lines.append(f"## {name}")
        lines.append("")
        lines.append(STRAT_META[name]["desc"])
        lines.append("")
        lines.append("| # | 代码 | 名称 | 正股 | 行业 | 价格 | 涨跌幅 | 转股溢价率 | 相对价值 | Delta | 隐含波动率 | 剩余年限 | 评级 | 备注 |")
        lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
        for i, p in enumerate(picks, 1):
            industry = p.get("ths_industry") or p.get("theme_industry") or ""
            stock = f"{p.get('uname') or ''} ({p.get('ucode') or ''})" if p.get("uname") else ""
            lines.append(
                f"| {i} | {p['code']} | {p.get('bond_name') or ''} | {stock} | {industry} | "
                f"{_fmt_num(p.get('price'))} | {_fmt_signed_pct(p.get('change_pct'))} | "
                f"{_fmt_pct(p.get('conv_prem_pct'))} | {_fmt_rv(p.get('relative_value'))} | "
                f"{_fmt_num(p.get('bs_delta'), 3)} | {_fmt_pct(p.get('implied_vol'))} | "
                f"{_fmt_num(p.get('surplus_years'), 2)} | {p.get('rating') or ''} | {p.get('note') or ''} |"
            )
        lines.append("")

    lines.append("## 附录 · 口径说明")
    lines.append("- **相对价值 (relative_value)**：市场价 / BS 理论价。<1 偏低估，>1 偏高估；范围限定 [0.5, 2.0] 避开极端样本。")
    lines.append("- **Delta**：BS 模型对正股价格的敏感度，0 ≈ 纯债性、1 ≈ 纯股性。")
    lines.append("- **双低分域**：按转股溢价率把样本切成 偏股<20% / 平衡 20-50% / 偏债≥50%，再各自 1.5×rank(转股溢价率) + rank(价格) 最小前 10。")
    lines.append("- 数据来源：iFinD 收盘字段 + 本地 BS 定价。")
    lines.append("")

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"[md]  → {out_path}")


# ---------- HTML writer ----------

EXTRA_CSS = r"""
.strategy-shell{max-width:1400px;margin:32px auto 64px;padding:0 20px;}
.strategy-hero{border:1px solid var(--line);border-radius:28px;padding:32px 36px;background:linear-gradient(135deg,rgba(255,255,255,.88),rgba(251,248,242,.78));box-shadow:var(--shadow);margin-bottom:26px;}
.strategy-hero h1{margin:0 0 10px;font:700 34px/1.1 var(--display);}
.strategy-hero .kicker{color:var(--muted);font:600 12px/1 var(--mono);letter-spacing:.14em;text-transform:uppercase;}
.strategy-hero .sub{margin-top:14px;color:var(--muted);font-size:14px;}
.strategy-hero a.back{color:var(--accent);font-weight:600;}
.strat-overview{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:14px;margin-bottom:28px;}
.strat-overview .card{border:1px solid var(--line);border-radius:18px;padding:16px 18px;background:rgba(251,248,242,.92);}
.strat-overview .card .name{display:inline-flex;align-items:center;gap:8px;font:700 16px/1 var(--display);}
.strat-overview .card .dot{width:10px;height:10px;border-radius:50%;display:inline-block;}
.strat-overview .card .count{margin-top:8px;font:700 28px/1 var(--display);font-variant-numeric:tabular-nums;}
.strat-overview .card .desc{margin-top:10px;color:var(--muted);font-size:12.5px;line-height:1.55;}
.strat-section{border:1px solid var(--line);border-radius:24px;background:rgba(251,248,242,.92);box-shadow:var(--shadow);padding:26px 28px;margin-bottom:26px;}
.strat-section .head{display:flex;align-items:baseline;justify-content:space-between;gap:16px;margin-bottom:12px;flex-wrap:wrap;}
.strat-section h2{margin:0;font:700 24px/1.1 var(--display);display:flex;align-items:center;gap:10px;}
.strat-section h2 .dot{width:14px;height:14px;border-radius:50%;display:inline-block;}
.strat-section .desc{color:var(--muted);font-size:13px;max-width:680px;}
.strat-table-wrap{overflow-x:auto;border:1px solid var(--line);border-radius:14px;}
table.strat-table{width:100%;min-width:1100px;border-collapse:collapse;font-size:13px;font-variant-numeric:tabular-nums;}
table.strat-table th,table.strat-table td{padding:9px 12px;border-bottom:1px solid var(--line);white-space:nowrap;text-align:right;}
table.strat-table th{background:rgba(28,26,23,.04);color:var(--muted);font-weight:600;text-transform:uppercase;letter-spacing:.04em;font-size:11px;text-align:right;}
table.strat-table th:nth-child(1),table.strat-table td:nth-child(1){width:40px;text-align:center;color:var(--muted);}
table.strat-table th:nth-child(2),table.strat-table td:nth-child(2),
table.strat-table th:nth-child(3),table.strat-table td:nth-child(3),
table.strat-table th:nth-child(4),table.strat-table td:nth-child(4),
table.strat-table th:nth-child(5),table.strat-table td:nth-child(5),
table.strat-table th:nth-child(13),table.strat-table td:nth-child(13),
table.strat-table th:nth-child(14),table.strat-table td:nth-child(14){text-align:left;}
table.strat-table td.code{font-family:var(--mono);color:var(--muted);}
table.strat-table tr:hover td{background:rgba(174,63,47,.035);}
.is-up{color:#8a1f13;font-weight:600;}
.is-down{color:#145548;font-weight:600;}
.is-flat{color:var(--muted);}
.appendix-strat{margin-top:12px;padding:20px 22px;border:1px solid var(--line);border-radius:18px;background:rgba(251,248,242,.85);color:var(--muted);font-size:13px;}
.appendix-strat h3{margin:0 0 10px;color:var(--ink);font:700 18px/1.2 var(--display);}
.appendix-strat ul{margin:0;padding-left:20px;}
.appendix-strat li{margin-bottom:4px;}
"""


def _render_overview_cards(groups):
    chunks = []
    for name in STRAT_ORDER:
        meta = STRAT_META[name]
        cnt = len(groups.get(name, []))
        chunks.append(
            f'<div class="card" style="border-top:3px solid {meta["color"]}">'
            f'<div class="name"><span class="dot" style="background:{meta["color"]}"></span>{html.escape(name)}</div>'
            f'<div class="count">{cnt}</div>'
            f'<div class="desc">{html.escape(meta["desc"])}</div>'
            '</div>'
        )
    return '<div class="strat-overview">' + "".join(chunks) + '</div>'


def _render_strategy_section(name, picks):
    meta = STRAT_META[name]
    rows = []
    for i, p in enumerate(picks, 1):
        industry = p.get("ths_industry") or p.get("theme_industry") or ""
        stock = f"{p.get('uname') or ''} ({p.get('ucode') or ''})" if p.get("uname") else ""
        day_cls = _signed_class(p.get("change_pct"))
        rows.append(
            "<tr>"
            f'<td>{i}</td>'
            f'<td class="code">{html.escape(p["code"])}</td>'
            f'<td>{html.escape(p.get("bond_name") or "")}</td>'
            f'<td>{html.escape(stock)}</td>'
            f'<td>{html.escape(industry)}</td>'
            f'<td>{html.escape(_fmt_num(p.get("price")))}</td>'
            f'<td class="{day_cls}">{html.escape(_fmt_signed_pct(p.get("change_pct")))}</td>'
            f'<td>{html.escape(_fmt_pct(p.get("conv_prem_pct")))}</td>'
            f'<td>{html.escape(_fmt_rv(p.get("relative_value")))}</td>'
            f'<td>{html.escape(_fmt_num(p.get("bs_delta"), 3))}</td>'
            f'<td>{html.escape(_fmt_pct(p.get("implied_vol")))}</td>'
            f'<td>{html.escape(_fmt_num(p.get("surplus_years"), 2))}</td>'
            f'<td>{html.escape(p.get("rating") or "")}</td>'
            f'<td>{html.escape(p.get("note") or "")}</td>'
            "</tr>"
        )
    return (
        '<section class="strat-section">'
        '<div class="head">'
        f'<h2><span class="dot" style="background:{meta["color"]}"></span>{html.escape(name)} · {len(picks)} 只</h2>'
        f'<div class="desc">{html.escape(meta["desc"])}</div>'
        '</div>'
        '<div class="strat-table-wrap">'
        '<table class="strat-table">'
        '<thead><tr>'
        '<th>#</th><th>代码</th><th>名称</th><th>正股</th><th>行业</th>'
        '<th>价格</th><th>涨跌幅</th><th>转股溢价率</th><th>相对价值</th><th>Delta</th>'
        '<th>隐含波动率</th><th>剩余年限</th><th>评级</th><th>备注</th>'
        '</tr></thead>'
        f'<tbody>{"".join(rows)}</tbody>'
        '</table></div></section>'
    )


def write_html(groups, trade_date, title_date, out_path):
    overview_cards = _render_overview_cards(groups)
    sections = "".join(
        _render_strategy_section(name, groups.get(name, []))
        for name in STRAT_ORDER if groups.get(name)
    )
    total = sum(len(v) for v in groups.values())

    page = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>可转债策略推荐 · {html.escape(title_date)}</title>
<style>{CSS}{EXTRA_CSS}</style>
</head>
<body>
<div class="strategy-shell">
  <div class="strategy-hero">
    <div class="kicker">Convertible Bond · Strategy Picks</div>
    <h1>今日策略推荐 · {html.escape(title_date)}</h1>
    <div class="sub">共 {total} 条入选记录，覆盖 5 个策略口径。<a class="back" href="cbond_overview.html">← 返回总览</a></div>
  </div>
  {overview_cards}
  {sections}
  <div class="appendix-strat">
    <h3>附录 · 口径说明</h3>
    <ul>
      <li><b>相对价值 (relative_value)</b>：市场价 / BS 理论价。&lt;1 偏低估，&gt;1 偏高估；范围限定 [0.5, 2.0] 避开极端样本。</li>
      <li><b>Delta</b>：BS 模型对正股价格的敏感度，0 ≈ 纯债性、1 ≈ 纯股性。</li>
      <li><b>双低</b>：PE&gt;0 + 波动率&gt;Q1 过滤后，按 1.5×rank(转股溢价率) + rank(价格) 最小前 30。</li>
      <li><b>双低-偏股/平衡/偏债</b>：按转股溢价率切三档，各自分域再取 top 10。</li>
      <li><b>低估</b>：相对价值在 [0.5, 2.0] 合理范围内，取最小 10 只。</li>
      <li>数据时点：{html.escape(title_date)} 收盘。</li>
    </ul>
  </div>
</div>
</body>
</html>
"""
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(page)
    print(f"[html]→ {out_path} ({os.path.getsize(out_path)} bytes)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trade-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--out-md",   required=True)
    ap.add_argument("--out-html", required=True)
    ap.add_argument("--title-date", default="", help="display date; defaults to --trade-date")
    args = ap.parse_args()

    title_date = args.title_date or args.trade_date

    con = connect()
    groups = load_picks(con, args.trade_date)
    con.close()

    if not groups:
        print(f"[warn] no strategy_picks rows for {args.trade_date}; did you run strategy_score.py?")
        return

    total = sum(len(v) for v in groups.values())
    print(f"[load] {total} picks across {len(groups)} strategies")

    write_md(groups, args.trade_date, title_date, args.out_md)
    write_html(groups, args.trade_date, title_date, args.out_html)


if __name__ == "__main__":
    main()
