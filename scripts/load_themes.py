"""Load Sonnet-generated themes.jsonl into DuckDB themes table.

Run this after the Claude theme-classification step produces a new themes.jsonl.

Usage:
  python3 scripts/load_themes.py \
      --themes data/raw/asof=2026-04-22/themes.jsonl \
      --trade-date 2026-04-22
"""
import argparse, json, os, sys

sys.path.insert(0, os.path.dirname(__file__))
from _db import connect, init_schema, upsert


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--themes", required=True, help="path to themes.jsonl")
    ap.add_argument("--trade-date", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    rows = []
    with open(args.themes, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            themes = r.get("themes", [])
            rows.append({
                "trade_date": args.trade_date,
                "code": r["code"],
                "theme_l1": themes[0] if themes else "",
                "all_themes_json": json.dumps(themes, ensure_ascii=False),
                "business_rewrite": r.get("business_rewrite", ""),
                "industry": r.get("industry", ""),
            })

    con = connect()
    init_schema(con)
    n = upsert(con, "themes", rows, ["trade_date", "code"])
    con.close()
    print(f"[done] themes upserted {n} rows (trade_date={args.trade_date})")


if __name__ == "__main__":
    main()
