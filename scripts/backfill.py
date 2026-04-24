"""One-shot backfill: load archived 4-20 data into DuckDB.

Usage:
  python3 scripts/backfill.py --raw data/raw/asof=2026-04-20 --trade-date 2026-04-20
"""
import argparse, csv, json, os, sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
from _db import connect, init_schema, upsert


def load_universe(con, raw_dir, trade_date):
    path = os.path.join(raw_dir, "cbond_universe.json")
    uni = json.load(open(path))
    now = datetime.utcnow().isoformat()
    rows = [
        {
            "code": r["code"], "name": r["name"],
            "ucode": r["ucode"], "uname": r["uname"],
            "list_date": r.get("listed", ""),
            "maturity_date": r.get("maturity", ""),
            "updated_at": now,
        }
        for r in uni["items"]
    ]
    n = upsert(con, "universe", rows, ["code"])
    print(f"[universe] {n} rows upserted")


def load_valuation(con, raw_dir, trade_date):
    path = os.path.join(raw_dir, "valuation.csv")
    rows = []
    with open(path) as f:
        for r in csv.DictReader(f):
            def _f(v):
                try: return float(v)
                except: return None
            rows.append({
                "trade_date": trade_date,
                "code": r["转债代码"],
                "price": _f(r.get("最新价")),
                "change_pct": _f(r.get("当日涨跌幅(%)")),
                "conv_prem_pct": _f(r.get("转股溢价率(%)")),
                "pure_prem_pct": _f(r.get("纯债溢价率(%)")),
                "outstanding_yi": _f(r.get("余额(亿元)")),
                "rating": r.get("评级", ""),
                "maturity_date": r.get("到期日", ""),
            })
    n = upsert(con, "valuation_daily", rows, ["trade_date", "code"])
    print(f"[valuation_daily] {n} rows upserted")


def load_vol(con, raw_dir, trade_date):
    path = os.path.join(raw_dir, "vol_20d.csv")
    seen = set()
    rows = []
    with open(path) as f:
        for r in csv.DictReader(f):
            ucode = r.get("正股代码", "")
            if not ucode or ucode in seen:
                continue
            seen.add(ucode)
            def _f(v):
                try: return float(v)
                except: return None
            vol = _f(r.get("20日年化波动率(%)"))
            rows.append({
                "trade_date": trade_date,
                "ucode": ucode,
                "vol_20d_pct": vol,
                "n_samples": int(r.get("样本数") or 0),
            })
    n = upsert(con, "vol_daily", rows, ["trade_date", "ucode"])
    print(f"[vol_daily] {n} rows upserted")


def load_profile(con, raw_dir):
    path = os.path.join(raw_dir, "underlying_profile.json")
    data = json.load(open(path))
    now = datetime.utcnow().isoformat()
    seen = set()
    rows = []
    for r in data["items"]:
        ucode = r.get("ucode", "")
        if not ucode or ucode in seen:
            continue
        seen.add(ucode)
        rows.append({
            "ucode": ucode,
            "uname": r.get("uname", ""),
            "industry": r.get("industry", ""),
            "main_business": r.get("profile", ""),
            "updated_at": now,
        })
    n = upsert(con, "underlying_profile", rows, ["ucode"])
    print(f"[underlying_profile] {n} rows upserted")


def load_themes(con, raw_dir, trade_date):
    path = os.path.join(raw_dir, "themes.jsonl")
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            themes = r.get("themes", [])
            rows.append({
                "trade_date": trade_date,
                "code": r["code"],
                "theme_l1": themes[0] if themes else "",
                "all_themes_json": json.dumps(themes, ensure_ascii=False),
                "business_rewrite": r.get("business_rewrite", ""),
                "industry": r.get("industry", ""),
            })
    n = upsert(con, "themes", rows, ["trade_date", "code"])
    print(f"[themes] {n} rows upserted")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", required=True, help="path to data/raw/asof=YYYY-MM-DD")
    ap.add_argument("--trade-date", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    con = connect()
    init_schema(con)
    load_universe(con, args.raw, args.trade_date)
    load_valuation(con, args.raw, args.trade_date)
    load_vol(con, args.raw, args.trade_date)
    load_profile(con, args.raw)
    load_themes(con, args.raw, args.trade_date)
    con.close()
    print("[done] backfill complete")


if __name__ == "__main__":
    main()
