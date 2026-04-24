"""Batch-fetch underlying stock profile (main business) + industry.

For each underlying stock code, pull:
  - ths_corp_profile (公司简介 / 主营业务)
  - ths_industry (申万行业)

Input is the cbond_universe.json (needs underlying stock codes).

Usage:
  python3 fetch_underlying_profile.py \
      --universe cbond_universe.json \
      --out cbond_underlying_profile.json
"""
import argparse, json, os, sys, time
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
from _ifind import basic_data, batched
from _db import connect, init_schema, upsert as db_upsert


STOCK_FIELDS = [
    {"indicator": "ths_corp_profile", "indiparams": [""]},
    {"indicator": "ths_industry", "indiparams": [""]},
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--batch-size", type=int, default=30)
    args = ap.parse_args()

    uni = json.load(open(args.universe, encoding="utf-8"))
    items = uni["items"]
    ucodes = sorted({r["ucode"] for r in items if r.get("ucode")})
    print(f"[stocks] {len(ucodes)} unique underlying codes")

    profiles = {}
    for b in batched(ucodes, args.batch_size):
        try:
            r = basic_data(b, STOCK_FIELDS)
            for t in r.get("tables", []):
                tbl = t.get("table", {})
                profiles[t["thscode"]] = {
                    "profile": (tbl.get("ths_corp_profile") or [""])[0],
                    "industry": (tbl.get("ths_industry") or [""])[0],
                }
        except Exception as e:
            print(f"[warn] profile batch err: {e}")
        time.sleep(0.18)

    # Attach profile onto each bond record (keyed by ucode)
    out_items = []
    for r in items:
        p = profiles.get(r["ucode"], {})
        out_items.append({
            **r,
            "profile": p.get("profile", ""),
            "industry": p.get("industry", ""),
        })
    json.dump({"asof": uni["asof"], "count": len(out_items), "items": out_items},
              open(args.out, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    print(f"[done] profile for {len(profiles)}/{len(ucodes)} stocks → {args.out}")

    # upsert to DuckDB
    now = datetime.utcnow().isoformat()
    db_rows = [
        {
            "ucode": ucode,
            "uname": next((r["uname"] for r in items if r.get("ucode") == ucode), ""),
            "industry": profiles.get(ucode, {}).get("industry", ""),
            "main_business": profiles.get(ucode, {}).get("profile", ""),
            "updated_at": now,
        }
        for ucode in ucodes
    ]
    con = connect()
    init_schema(con)
    n = db_upsert(con, "underlying_profile", db_rows, ["ucode"])
    con.close()
    print(f"[db] underlying_profile upserted {n} rows")


if __name__ == "__main__":
    main()
