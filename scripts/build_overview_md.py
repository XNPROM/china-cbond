"""Build the convertible-bond overview Markdown grouped by primary theme."""
import argparse
import json
import os
import statistics
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(__file__))
from _db import connect


def _fmt_num(x, digits=2):
    return "" if x is None else f"{x:.{digits}f}"


def _fmt_pct(x):
    return "" if x is None else f"{x:.2f}%"


def _fmt_vol(x):
    """Format vol_20d (stored as decimal, e.g. 0.35=35%) as percentage string."""
    return "" if x is None else f"{x * 100:.2f}%"


def _fmt_signed_pct(x):
    if x is None:
        return ""
    if x > 0:
        return f"+{x:.2f}%"
    return f"{x:.2f}%"


def _fmt_ytm(x):
    if x is None:
        return ""
    if abs(x) > 100:
        return ""  # Extreme YTM values are data errors
    return f"{x:.2f}%"


def _fmt_rv(x):
    if x is None:
        return ""
    if x < 0.5 or x > 3.0:
        return ""  # Extreme relative value, likely BS model breakdown
    return f"{x:.2f}"


def _fmt_date(yyyymmdd):
    if not yyyymmdd or len(yyyymmdd) != 8:
        return yyyymmdd or ""
    return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:]}"


def _call_status(row):
    """Derive 强赎 status label from no_call_start/end and call_trigger_days."""
    no_start = row.get("no_call_start") or ""
    no_end = row.get("no_call_end") or ""
    days = row.get("call_trigger_days")
    redemp_stop = row.get("redemp_stop_date") or ""
    if redemp_stop:
        return f"强赎停牌{_fmt_date(redemp_stop)}"
    if no_start and no_end:
        return f"不强赎至{_fmt_date(no_end)}"
    if isinstance(days, int) and days > 0:
        return f"已触发{days}天"
    return ""


def _down_status(row):
    """Derive 下修 status label."""
    has = row.get("has_down_revision") or ""
    ratio = row.get("down_trigger_ratio")
    if has == "是" and ratio:
        return f"触发≤{_fmt_num(ratio, 0)}%"
    if has == "是":
        return "有下修条款"
    return ""


def _load_themes_from_db(trade_date):
    con = connect()
    rows_raw = con.execute(
        "SELECT code, theme_l1, all_themes_json, business_rewrite, industry "
        "FROM themes WHERE trade_date = ?",
        [trade_date]
    ).fetchall()
    con.close()
    return {
        r[0]: {
            "code": r[0],
            "theme_l1": r[1],
            "themes": json.loads(r[2] or "[]"),
            "business_rewrite": r[3] or "",
            "industry": r[4] or "",
        }
        for r in rows_raw
    }


def _load_strategy_from_db(trade_date):
    con = connect()
    rows_raw = con.execute(
        "SELECT code, strategy, rank_overall, note "
        "FROM strategy_picks WHERE trade_date = ? ORDER BY rank_overall",
        [trade_date]
    ).fetchall()
    con.close()
    return {
        r[0]: {"strategy": r[1], "rank_overall": r[2], "note": r[3] or ""}
        for r in rows_raw
    }, [
        {"code": r[0], "strategy": r[1], "rank_overall": r[2], "note": r[3] or ""}
        for r in rows_raw
    ]


def _load_history_from_db(trade_date):
    """Load historical delta and relative_value for sparkline charts."""
    con = connect()
    rows_raw = con.execute(
        "SELECT code, trade_date, bs_delta, relative_value "
        "FROM valuation_daily "
        "WHERE bs_delta IS NOT NULL AND trade_date <= ? "
        "ORDER BY code, trade_date",
        [trade_date]
    ).fetchall()
    con.close()
    hist = {}
    for code, td, delta, rv in rows_raw:
        hist.setdefault(code, {"dates": [], "delta": [], "rv": []})
        hist[code]["dates"].append(td)
        hist[code]["delta"].append(round(delta, 3))
        hist[code]["rv"].append(round(rv, 3))
    return hist


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--trade-date", required=True, help="YYYY-MM-DD, used to load themes from DB")
    ap.add_argument("--out", required=True)
    ap.add_argument("--title-date", required=True)
    args = ap.parse_args()

    dataset = json.load(open(args.dataset, encoding="utf-8"))
    themes = _load_themes_from_db(args.trade_date)
    strategy_map, strategy_list = _load_strategy_from_db(args.trade_date)
    hist_data = _load_history_from_db(args.trade_date)

    rows = []
    for item in dataset["items"]:
        theme_row = themes.get(item["code"])
        if not theme_row:
            continue
        merged = {**item, **theme_row}
        merged["primary_theme"] = merged["themes"][0] if merged["themes"] else "其他"
        rows.append(merged)

    rows.sort(key=lambda x: (x["primary_theme"], x["name"]))
    by_theme = defaultdict(list)
    for row in rows:
        by_theme[row["primary_theme"]].append(row)

    price_values = [r["latest"] for r in rows if r.get("latest") is not None]
    conv_values = [r["conv_prem"] for r in rows if r.get("conv_prem") is not None]
    pure_values = [r["pure_prem"] for r in rows if r.get("pure_prem") is not None]
    rv_values = [r["relative_value"] for r in rows if r.get("relative_value") is not None and 0.5 <= r["relative_value"] <= 3.0]
    top_themes = Counter(r["primary_theme"] for r in rows).most_common(10)

    # Sector classification counts
    n_equity = sum(1 for v in conv_values if v < 20)
    n_balanced = sum(1 for v in conv_values if 20 <= v < 50)
    n_debt = sum(1 for v in conv_values if v >= 50)

    lines = []
    lines.append(f"# 可转债概览 · {args.title_date}")
    lines.append("")
    lines.append("## 摘要")
    lines.append(f"- 总数 {len(rows)} 只；{sum(1 for v in price_values if v > 130)} 只价格 >130，{sum(1 for v in price_values if v < 90)} 只价格 <90")
    lines.append(f"- 转股溢价率中位数 {statistics.median(conv_values):.2f}%，纯债溢价率中位数 {statistics.median(pure_values):.2f}%" if conv_values and pure_values else "")
    if rv_values:
        rv_under = sum(1 for v in rv_values if v < 1.0)
        lines.append(f"- 相对价值中位数 {statistics.median(rv_values):.2f}；{rv_under} 只低估（<1.0），{len(rv_values)-rv_under} 只合理或高估")
    lines.append(f"- 分域分布：偏股{n_equity}只 / 平衡{n_balanced}只 / 偏债{n_debt}只")
    lines.append("- 按题材分布（Top 10）：" + " / ".join(f"{name}({cnt})" for name, cnt in top_themes))
    lines.append("")
    lines.append("## 题材索引")
    for theme, items in sorted(by_theme.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        lines.append(f"- [{theme}](#{theme}) ({len(items)} 只)")

    # Strategy picks section
    STRAT_DESC = {
        "双低": "经典双低：PE>0 + 波动率>Q1，按 1.5×rank(转股溢价率) + rank(价格) 排名",
        "双低-偏股": "分域双低·偏股型（转股溢价率<20%）：域内独立双低排名",
        "双低-平衡": "分域双低·平衡型（20%≤转股溢价率<50%）：域内独立双低排名",
        "双低-偏债": "分域双低·偏债型（转股溢价率≥50%）：域内独立双低排名",
        "低估": "相对价值策略：市价/BS理论价值最低，低于1=低估",
    }
    lines.append("")
    lines.append("## 策略推荐")
    STRAT_ORDER = ["双低", "双低-偏股", "双低-平衡", "双低-偏债", "低估"]
    by_strat = defaultdict(list)
    for sp in strategy_list:
        by_strat[sp["strategy"]].append(sp)
    if strategy_list:

        for strat_name in STRAT_ORDER:
            picks = by_strat.get(strat_name)
            if not picks:
                continue
            lines.append("")
            lines.append(f"### {strat_name}")
            desc = STRAT_DESC.get(strat_name, "")
            if desc:
                lines.append(f"\n*{desc}*\n")
            lines.append("")
            lines.append("| 排名 | 转债 | 正股 | 价格 | 转股溢价率 | PE | 20日σ | 综合得分 |")
            lines.append("|---|---|---|---|---|---|---|---|")
            for i, sp in enumerate(picks):
                row = next((r for r in rows if r["code"] == sp["code"]), None)
                if not row:
                    continue
                lines.append(
                    f"| {i+1} | {row['name']} ({row['code']}) | {row.get('uname','')} | "
                    f"{_fmt_num(row.get('latest'))} | {_fmt_pct(row.get('conv_prem'))} | "
                    f"{_fmt_num(row.get('pe_ttm'))} | {_fmt_vol(row.get('vol_20d'))} | "
                    f"{sp['rank_overall']:.1f} |"
                )
    else:
        lines.append("")
        lines.append("暂无策略数据。请先运行 `strategy_score.py`。")

    for theme, items in sorted(by_theme.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        lines.append("")
        lines.append(f"## {theme}")
        for row in items:
            lines.append("")
            lines.append(f"### {row['name']} ({row['code']})")
            lines.append("")
            lines.append("| 正股 | 申万一级 | 申万二级 | 价格 | 涨跌幅 | 转股溢价率 | 纯债溢价率 | 20日年化σ | 隐含波动率 | 纯债YTM | 余额(亿) | 评级 | 到期 | 剩余(年) | 转股价 | PE | PB | 市值(亿) | 相对价值 | Delta | 强赎 | 下修 | 策略分 |")
            lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")

            call_label = _call_status(row)
            down_label = _down_status(row)
            # Use theme-inferred 申万一级 + 同花顺行业 as 申万二级 substitute
            sw_l1 = row.get('industry', '')
            sw_l2 = row.get('ths_industry', '')
            strat_label_parts = []
            for s in strategy_list:
                if s["code"] == row['code']:
                    rank_in_strat = sum(1 for s2 in by_strat.get(s["strategy"], []) if s2["rank_overall"] <= s["rank_overall"])
                    strat_label_parts.append(f"{s['strategy']}#{rank_in_strat}")
            strat_label = " ".join(strat_label_parts) if strat_label_parts else ""

            lines.append(
                f"| {row['uname']} ({row['ucode']}) | {sw_l1} | {sw_l2} | "
                f"{_fmt_num(row.get('latest'))} | {_fmt_signed_pct(row.get('day_chg'))} | {_fmt_pct(row.get('conv_prem'))} | "
                f"{_fmt_pct(row.get('pure_prem'))} | {_fmt_vol(row.get('vol_20d'))} | "
                f"{_fmt_pct(row.get('implied_vol'))} | "
                f"{_fmt_ytm(row.get('pure_bond_ytm'))} | "
                f"{_fmt_num(row.get('balance'))} | {row.get('rating','')} | {_fmt_date(row.get('maturity'))} | "
                f"{_fmt_num(row.get('surplus_years'))} | "
                f"{_fmt_num(row.get('conv_price'))} | {_fmt_num(row.get('pe_ttm'))} | {_fmt_num(row.get('pb'))} | "
                f"{_fmt_num(row.get('total_mv_yi'))} | "
                f"{_fmt_rv(row.get('relative_value'))} | {_fmt_num(row.get('bs_delta'))} | {call_label} | {down_label} | {strat_label} |"
            )
            lines.append("")
            lines.append(f"**主营**：{row.get('business_rewrite','').strip()}")
            # Historical sparkline data
            h = hist_data.get(row["code"])
            if h and len(h["dates"]) > 1:
                lines.append(f"**时序**：dates={','.join(h['dates'])} delta={','.join(str(d) for d in h['delta'])} rv={','.join(str(v) for v in h['rv'])}")
            lines.append("")
            lines.append("**题材**：" + " ".join(f"`#{theme_name}`" for theme_name in row.get("themes", [])))
    lines.append("")
    lines.append("## 附录 · 字段说明")
    lines.append("- 转股溢价率：(转债价格 / 转股价值 − 1) × 100%。")
    lines.append("- 纯债溢价率：(转债价格 / 纯债价值 − 1) × 100%。")
    lines.append("- 20日年化波动率：过去20个交易日收盘价对数收益率标准差 × √252。")
    lines.append("- PE：正股滚动市盈率（TTM）。PB：正股市净率。市值：正股总市值（亿元）。")
    lines.append("- 纯债YTM：纯债到期收益率（%），负值表示转债价格高于纯债价值到期可收回金额。")
    lines.append("- 相对价值：市价 / BS理论价值。低于1=低估，高于1.2=高估。BS理论价值=BS看涨期权+纯债价值，不含赎回/下修条款。")
    lines.append("- Delta：BS模型正股敏感度。0=纯债属性，1=纯股属性，0.3-0.7=平衡型。")
    lines.append("- 分域：偏股（转股溢价率<20%）、平衡（20-50%）、偏债（≥50%）。")
    lines.append("- 强赎：空=尚未触发且无不强赎承诺；'不强赎至XX'=发行人承诺不强赎至该日；'已触发N天'=正股收盘价已连续N天超过转股价×强赎触发比例。")
    lines.append("- 下修：'触发≤N%'=正股收盘价低于转股价×N%时可触发下修。")
    lines.append(f"- 数据时点：{args.title_date} 收盘。")

    with open(args.out, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"[done] {len(rows)} rows → {args.out}")


if __name__ == "__main__":
    main()
