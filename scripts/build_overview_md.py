"""Build the convertible-bond overview Markdown grouped by primary theme."""
import argparse
import json
import os
import statistics
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(__file__))
from _db import connect


# --- formatting helpers ---

def _fmt_num(x, digits=2):
    return "" if x is None else f"{x:.{digits}f}"


def _fmt_pct(x):
    return "" if x is None else f"{x:.2f}%"


def _fmt_signed_pct(x):
    if x is None:
        return ""
    if x > 0:
        return f"+{x:.2f}%"
    return f"{x:.2f}%"


def _fmt_vol(x):
    """vol_20d stored as pct (34.52 means 34.52%)."""
    return "" if x is None else f"{x:.2f}%"


def _fmt_rv(x):
    return "" if x is None else f"{x:.3f}"


def _fmt_ytm(x):
    return "" if x is None else f"{x:.2f}%"


def _fmt_date(yyyymmdd):
    if not yyyymmdd or len(yyyymmdd) != 8:
        return yyyymmdd or ""
    return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:]}"


def _call_status(row):
    """强赎状态。已触发停牌 → '已强赎'；活跃条款 → 'N/30'；否则 '—'。"""
    stop = row.get("redemp_stop_date")
    if stop:
        return "已强赎"
    days = row.get("call_trigger_days")
    ratio = row.get("call_trigger_ratio")
    if days is not None and ratio:
        return f"{int(days)}/30"
    return "—"


def _down_status(row):
    """下修状态。'是' → 'N/M 日 x%'；否则 '否'。"""
    has = row.get("has_down_revision")
    if has != "是":
        return "否"
    ratio = row.get("down_trigger_ratio")
    if ratio:
        return f"{ratio:.0f}%"
    return "是"


def _bond_status(row, trade_date):
    """异常状态判定（仅在 BS 不可得时输出，正常返回空串）。

    判断顺序：
      1. maturity_date ≤ trade_date → 已到期/到期日，BS 公式 T→0 退化
      2. latest 无值 → 停牌/无当日报价
      3. surplus_years≈0（兜底，避免 maturity_date 缺失） → 临近到期
      4. pure_bond_value 缺 → 纯债价值缺失，BS 不可得
    """
    maturity = (row.get("maturity") or "").strip()  # YYYYMMDD
    if maturity and len(maturity) == 8:
        td_compact = trade_date.replace("-", "")
        if maturity == td_compact:
            return "到期日（BS 不适用）"
        if maturity < td_compact:
            return "已到期（BS 不适用）"
    if row.get("latest") is None:
        return "停牌/无当日报价"
    sy = row.get("surplus_years")
    if sy is not None and sy <= 0.05:
        return "临近到期（BS 不适用）"
    if row.get("relative_value") is None and row.get("pure_bond_value") is None:
        return "纯债价值缺失（BS 估值不可得）"
    return ""


# --- strategy label display map ---

STRATEGY_LABELS = {
    "双低":     "双低",
    "双低-偏股": "偏股双低",
    "双低-平衡": "平衡双低",
    "双低-偏债": "偏债双低",
    "低估":     "低估",
}


# --- data loading ---

def _load_themes_from_db(con, trade_date):
    rows_raw = con.execute(
        "SELECT code, theme_l1, all_themes_json, business_rewrite, industry "
        "FROM themes WHERE trade_date = ?",
        [trade_date]
    ).fetchall()
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


def _load_strategy_picks(con, trade_date):
    """Return { code: [strategy_display_label, ...] } keeping input order."""
    order = ["双低", "双低-偏股", "双低-平衡", "双低-偏债", "低估"]
    rows = con.execute(
        "SELECT code, strategy FROM strategy_picks WHERE trade_date = ?",
        [trade_date]
    ).fetchall()
    by_code = defaultdict(list)
    for code, strat in rows:
        by_code[code].append(strat)
    # normalize per-code to a stable order using STRATEGY_LABELS
    out = {}
    for code, strats in by_code.items():
        ordered = sorted(set(strats), key=lambda s: order.index(s) if s in order else 99)
        out[code] = [STRATEGY_LABELS.get(s, s) for s in ordered]
    return out


def _load_strategy_rows(con, trade_date):
    rows = con.execute(
        "SELECT code, strategy, rank_overall, note "
        "FROM strategy_picks WHERE trade_date = ? "
        "ORDER BY strategy, rank_overall",
        [trade_date]
    ).fetchall()
    return [
        {"code": r[0], "strategy": r[1], "rank_overall": r[2], "note": r[3] or ""}
        for r in rows
    ]


def _load_history_from_db(con, trade_date):
    rows = con.execute(
        "SELECT code, trade_date, bs_delta, relative_value "
        "FROM valuation_daily "
        "WHERE bs_delta IS NOT NULL AND trade_date <= ? "
        "ORDER BY code, trade_date",
        [trade_date]
    ).fetchall()
    hist = {}
    for code, td, delta, rv in rows:
        hist.setdefault(code, {"dates": [], "delta": [], "rv": []})
        hist[code]["dates"].append(td)
        hist[code]["delta"].append(round(delta, 3))
        hist[code]["rv"].append(round(rv, 3) if rv is not None else "")
    return hist


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--trade-date", required=True, help="YYYY-MM-DD, used to load themes/strategy from DB")
    ap.add_argument("--out", required=True)
    ap.add_argument("--title-date", required=True)
    ap.add_argument("--strategy-page", default="cbond_strategy.html",
                    help="relative link to strategy HTML page in the summary")
    args = ap.parse_args()

    dataset = json.load(open(args.dataset, encoding="utf-8"))
    con = connect()
    themes = _load_themes_from_db(con, args.trade_date)
    strategy_map = _load_strategy_picks(con, args.trade_date)
    strategy_rows = _load_strategy_rows(con, args.trade_date)
    hist_data = _load_history_from_db(con, args.trade_date)
    con.close()

    rows = []
    for item in dataset["items"]:
        theme_row = themes.get(item["code"])
        if not theme_row:
            continue
        merged = {**item, **theme_row}
        merged["primary_theme"] = merged["themes"][0] if merged["themes"] else "其他综合"
        merged["_strategies"] = strategy_map.get(item["code"], [])
        rows.append(merged)

    rows.sort(key=lambda x: (x["primary_theme"], x["name"]))
    by_theme = defaultdict(list)
    for row in rows:
        by_theme[row["primary_theme"]].append(row)

    price_values = [r["latest"] for r in rows if r.get("latest") is not None]
    conv_values = [r["conv_prem"] for r in rows if r.get("conv_prem") is not None]
    pure_values = [r["pure_prem"] for r in rows if r.get("pure_prem") is not None]
    top_themes = Counter(r["primary_theme"] for r in rows).most_common(10)
    rows_by_code = {r["code"]: r for r in rows}

    lines = []
    lines.append(f"# 可转债概览 · {args.title_date}")
    lines.append("")
    lines.append("## 摘要")
    lines.append(f"- 总数 {len(rows)} 只；{sum(1 for v in price_values if v > 130)} 只价格 >130，{sum(1 for v in price_values if v < 90)} 只价格 <90")
    lines.append(f"- 转股溢价率中位数 {statistics.median(conv_values):.2f}%，纯债溢价率中位数 {statistics.median(pure_values):.2f}%")
    lines.append("- 按题材分布（Top 10）：" + " / ".join(f"{name}({cnt})" for name, cnt in top_themes))
    lines.append(f"- HTML 支持“概览 / 策略”页签切换；独立页见 [→ 今日策略推荐]({args.strategy_page})")
    lines.append("")
    lines.append("## 题材索引")
    for theme, items in sorted(by_theme.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        lines.append(f"- [{theme}](#{theme}) ({len(items)} 只)")
    lines.append("")
    lines.append("## 策略推荐")
    if strategy_rows:
        strat_desc = {
            "双低": "经典双低：PE>0 + 波动率>Q1，按 1.5×rank(转股溢价率) + rank(价格) 排名",
            "双低-偏股": "分域双低·偏股型（BS Delta≥0.6）：域内独立双低排名",
            "双低-平衡": "分域双低·平衡型（0.3≤Delta<0.6）：域内独立双低排名",
            "双低-偏债": "分域双低·偏债型（Delta<0.3）：域内独立双低排名",
            "低估": "相对价值策略：市价 / BS 理论价值最低，低于 1 表示偏低估",
        }
        strat_order = ["双低", "双低-偏股", "双低-平衡", "双低-偏债", "低估"]
        grouped = defaultdict(list)
        for row in strategy_rows:
            grouped[row["strategy"]].append(row)
        for strat_name in strat_order:
            picks = grouped.get(strat_name, [])
            if not picks:
                continue
            lines.append("")
            lines.append(f"### {strat_name}")
            lines.append("")
            lines.append(f"*{strat_desc.get(strat_name, '')}*")
            lines.append("")
            lines.append("| 排名 | 转债 | 正股 | 价格 | 转股溢价率 | PE | 20日σ | 综合得分 |")
            lines.append("|---|---|---|---|---|---|---|---|")
            for idx, pick in enumerate(picks, 1):
                row = rows_by_code.get(pick["code"])
                if not row:
                    continue
                lines.append(
                    f"| {idx} | {row['name']} ({row['code']}) | {row.get('uname', '')} | "
                    f"{_fmt_num(row.get('latest'))} | {_fmt_pct(row.get('conv_prem'))} | "
                    f"{_fmt_num(row.get('pe_ttm'))} | {_fmt_vol(row.get('vol_20d'))} | "
                    f"{pick['rank_overall']:.1f} |"
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
            lines.append(
                "| 正股 | 行业 | 价格 | 涨跌幅 | 转股溢价率 | 纯债溢价率 | 20日年化σ | 隐含波动率 | "
                "相对价值 | Delta | 纯债YTM | 剩余年限 | 强赎 | 下修 | 余额(亿) | 评级 | 到期 |"
            )
            lines.append(
                "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"
            )
            lines.append(
                f"| {row['uname']} ({row['ucode']}) | {row.get('ths_industry') or row.get('industry','')} | "
                f"{_fmt_num(row.get('latest'))} | {_fmt_signed_pct(row.get('day_chg'))} | "
                f"{_fmt_pct(row.get('conv_prem'))} | {_fmt_pct(row.get('pure_prem'))} | "
                f"{_fmt_vol(row.get('vol_20d'))} | {_fmt_pct(row.get('implied_vol'))} | "
                f"{_fmt_rv(row.get('relative_value'))} | {_fmt_num(row.get('bs_delta'), 3)} | "
                f"{_fmt_pct(row.get('pure_bond_ytm'))} | {_fmt_num(row.get('surplus_years'), 2)} | "
                f"{_call_status(row)} | {_down_status(row)} | "
                f"{_fmt_num(row.get('balance'))} | {row.get('rating','')} | {_fmt_date(row.get('maturity'))} |"
            )
            lines.append("")
            status_note = _bond_status(row, args.trade_date)
            if status_note:
                lines.append(f"**状态**：⚠ {status_note}")
            lines.append(f"**主营**：{row.get('business_rewrite','').strip()}")
            hist = hist_data.get(row["code"])
            if hist and len(hist["dates"]) > 1:
                rv_seq = ",".join("" if v == "" else str(v) for v in hist["rv"])
                lines.append(
                    f"**时序**：dates={','.join(hist['dates'])} "
                    f"delta={','.join(str(v) for v in hist['delta'])} "
                    f"rv={rv_seq}"
                )
            lines.append("")
            lines.append("**题材**：" + " ".join(f"`#{theme_name}`" for theme_name in row.get("themes", [])))
    lines.append("")
    lines.append("## 附录 · 字段说明")
    lines.append("- 转股溢价率：(转债价格 / 转股价值 − 1) × 100%。")
    lines.append("- 纯债溢价率：(转债价格 / 纯债价值 − 1) × 100%。")
    lines.append("- 20日年化波动率：过去 20 个交易日正股对数收益率标准差 × √252，显示为百分比。")
    lines.append("- 隐含波动率：iFinD 按 BS 模型由转债价格反推的正股波动率。")
    lines.append("- 相对价值：市场价 / BS 理论价，<1 偏低估，>1 偏高估。")
    lines.append("- Delta：BS 模型对正股价格的敏感度，0 ≈ 纯债性、1 ≈ 纯股性。")
    lines.append("- 纯债YTM：按纯债价值折算到期收益率。")
    lines.append("- 时序：展示历史可得样本中的 Delta 与相对价值轨迹，供观察股性和估值漂移。")
    lines.append("- 强赎 `N/30 · X%`：若连续 N 个交易日正股收盘价触及转股价 × X%，即触发强制赎回。")
    lines.append("- 下修 `X%`：向下修正转股价条款触发阈值（收盘价 / 转股价）。")
    lines.append(f"- 数据时点：{args.title_date} 收盘。")

    with open(args.out, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"[done] {len(rows)} rows → {args.out}")


if __name__ == "__main__":
    main()
