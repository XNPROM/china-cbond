"""Double-low strategy scoring for convertible bonds.

Two strategies:
1. 双低 (vanilla): PE>0, vol>Q1, rank = 1.5*rank(conv_prem) + rank(price), top 30
2. 双低-分域 (sector-neutral): Same filter, then classify into 3 sectors by
   conv_prem (偏股<20%, 平衡20-50%, 偏债≥50%), rank independently within each
   sector, pick top 10 per sector.

Usage:
  python3 strategy_score.py \
      --dataset data/raw/asof=2026-04-23/dataset.json \
      --trade-date 2026-04-23 \
      --out data/raw/asof=2026-04-23/strategy_picks.jsonl
"""
import argparse, json, os, sys

sys.path.insert(0, os.path.dirname(__file__))
from _db import connect, init_schema, upsert as db_upsert


def _percentile(sorted_vals, pct):
    if not sorted_vals:
        return 0
    k = (len(sorted_vals) - 1) * pct / 100
    lo = int(k)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = k - lo
    return sorted_vals[lo] + frac * (sorted_vals[hi] - sorted_vals[lo])


def _classify_sector(delta):
    """Classify by BS Delta: 偏股≥0.6, 平衡0.3–0.6, 偏债<0.3."""
    if delta is None:
        return "偏债"
    if delta >= 0.6:
        return "偏股"
    if delta >= 0.3:
        return "平衡"
    return "偏债"


def _rank_and_score(candidates):
    by_conv = sorted(candidates, key=lambda r: r["conv_prem"])
    by_price = sorted(candidates, key=lambda r: r["latest"])
    rank_conv = {r["code"]: i + 1 for i, r in enumerate(by_conv)}
    rank_price = {r["code"]: i + 1 for i, r in enumerate(by_price)}

    scored = []
    for r in candidates:
        rc = rank_conv[r["code"]]
        rp = rank_price[r["code"]]
        overall = 1.5 * rc + rp
        scored.append({
            "code": r["code"],
            "name": r["name"],
            "ucode": r.get("ucode", ""),
            "uname": r.get("uname", ""),
            "sector": _classify_sector(r.get("bs_delta")),
            "rank_conv_prem": rc,
            "rank_price": rp,
            "rank_overall": overall,
            "conv_prem": r["conv_prem"],
            "latest": r["latest"],
            "pe_ttm": r["pe_ttm"],
            "vol_20d": r.get("vol_20d"),
            "day_chg": r.get("day_chg"),
            "bs_delta": r.get("bs_delta"),
        })
    return scored


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--trade-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--out", required=True)
    ap.add_argument("--top", type=int, default=30)
    ap.add_argument("--sector-top", type=int, default=10)
    args = ap.parse_args()

    dataset = json.load(open(args.dataset, encoding="utf-8"))
    items = dataset["items"]

    # Filter: PE > 0, vol_20d present
    candidates = [
        r for r in items
        if r.get("pe_ttm") is not None and r["pe_ttm"] > 0
        and r.get("vol_20d") is not None
        and r.get("conv_prem") is not None
        and r.get("latest") is not None
    ]
    print(f"[filter] {len(candidates)}/{len(items)} after PE>0 + data completeness")

    # Filter: vol > Q1
    vol_q1 = _percentile(sorted(r["vol_20d"] for r in candidates), 25)
    candidates = [r for r in candidates if r["vol_20d"] >= vol_q1]
    print(f"[filter] {len(candidates)} after vol>={vol_q1:.2f}%")

    # --- Strategy 1: Vanilla double-low ---
    scored = _rank_and_score(candidates)
    scored.sort(key=lambda x: x["rank_overall"])
    vanilla_top = scored[:args.top]
    for i, row in enumerate(vanilla_top):
        row["strategy"] = "双低"
        row["note"] = f"转股溢价率{row['conv_prem']:.1f}%，价格{row['latest']:.1f}"

    # --- Strategy 2: Sector-neutral double-low (classified by BS Delta) ---
    sector_groups = {"偏股": [], "平衡": [], "偏债": []}
    for r in candidates:
        s = _classify_sector(r.get("bs_delta"))
        sector_groups[s].append(r)

    sector_picks = []
    for sector_name, group in sector_groups.items():
        if not group:
            continue
        scored_s = _rank_and_score(group)
        scored_s.sort(key=lambda x: x["rank_overall"])
        top_s = scored_s[:args.sector_top]
        n_sector = len(group)
        for i, row in enumerate(top_s):
            row["strategy"] = f"双低-{sector_name}"
            row["note"] = f"{sector_name}({n_sector}只) Delta={row.get('bs_delta', ''):.2f}，转股溢价率{row['conv_prem']:.1f}%，价格{row['latest']:.1f}"
            sector_picks.append(row)
        print(f"[sector] {sector_name}: {n_sector} candidates, top {len(top_s)}")

    # --- Strategy 3: Relative value (低估策略) ---
    # Read relative_value from DB (written by bs_pricing.py)
    con = connect()
    rv_rows = con.execute(
        "SELECT code, relative_value FROM valuation_daily WHERE trade_date = ? AND relative_value IS NOT NULL",
        [args.trade_date]
    ).fetchall()
    con.close()
    rv_map = {r[0]: r[1] for r in rv_rows}

    rv_picks = []
    rv_candidates = [r for r in candidates if rv_map.get(r["code"]) is not None and 0.5 <= rv_map[r["code"]] <= 2.0]
    if rv_candidates:
        rv_sorted = sorted(rv_candidates, key=lambda r: rv_map[r["code"]])
        for i, r in enumerate(rv_sorted[:args.sector_top]):
            rv = rv_map[r["code"]]
            rv_picks.append({
                "code": r["code"],
                "name": r["name"],
                "ucode": r.get("ucode", ""),
                "uname": r.get("uname", ""),
                "sector": _classify_sector(r.get("bs_delta")),
                "rank_conv_prem": 0,
                "rank_price": 0,
                "rank_overall": float(i + 1),
                "conv_prem": r["conv_prem"],
                "latest": r["latest"],
                "pe_ttm": r.get("pe_ttm"),
                "vol_20d": r.get("vol_20d"),
                "day_chg": r.get("day_chg"),
                "strategy": "低估",
                "note": f"相对价值{rv:.2f}，转股溢价率{r['conv_prem']:.1f}%"
            })
        print(f"[rv] 低估: {len(rv_candidates)} candidates, top {len(rv_picks)}")

    # Merge and write
    all_picks = vanilla_top + sector_picks + rv_picks
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        for row in all_picks:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(f"[done] vanilla {len(vanilla_top)} + sector {len(sector_picks)} + rv {len(rv_picks)} → {args.out}")

    # Upsert to DB
    db_rows = [
        {
            "trade_date": args.trade_date,
            "code": row["code"],
            "strategy": row["strategy"],
            "rank_overall": row["rank_overall"],
            "rank_conv_prem": row["rank_conv_prem"],
            "rank_price": row["rank_price"],
            "note": row["note"],
        }
        for row in all_picks
    ]
    con = connect()
    # Recompute is date-scoped: remove stale picks from previous runs before inserting
    # the current strategy set, otherwise changed filters leave old rows behind.
    con.execute("DELETE FROM strategy_picks WHERE trade_date = ?", [args.trade_date])
    n = db_upsert(con, "strategy_picks", db_rows, ["trade_date", "code", "strategy"])
    con.close()
    print(f"[db] strategy_picks upserted {n} rows")


if __name__ == "__main__":
    main()
