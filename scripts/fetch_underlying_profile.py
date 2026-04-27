"""Batch-fetch underlying stock profile (main business) + industry.

For each underlying stock code, pull:
  - ths_corp_profile (公司简介 / 主营业务)
  - industry (try iFinD basic_data first; fall back to Eastmoney public quote API)

Input is the cbond_universe.json (needs underlying stock codes).

Usage:
  python3 fetch_underlying_profile.py \\
      --universe cbond_universe.json \\
      --out cbond_underlying_profile.json
"""
import argparse, json, os, ssl, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
from _ifind import basic_data, batched
from _db import connect, init_schema, upsert as db_upsert


STOCK_FIELDS = [
    {"indicator": "ths_corp_profile", "indiparams": [""]},
    {"indicator": "ths_industry", "indiparams": [""]},  # 行业 (may return empty; supplementary)
]


def _to_secid(code: str) -> str:
    if code.endswith(".SH"):
        return "1." + code.split(".")[0]
    if code.endswith(".SZ"):
        return "0." + code.split(".")[0]
    return ""


def _fetch_em_industry(code: str, retries: int = 3) -> str:
    secid = _to_secid(code)
    if not secid:
        return ""
    url = f"https://push2.eastmoney.com/api/qt/stock/get?secid={secid}&fields=f127"
    last_err = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=15, context=ssl.create_default_context()) as r:
                payload = json.loads(r.read().decode("utf-8"))
            value = ((payload or {}).get("data") or {}).get("f127")
            return value.strip() if isinstance(value, str) else ""
        except Exception as e:
            last_err = e
            time.sleep(0.5 * (2 ** i))
    print(f"[warn] eastmoney industry err: {code}: {last_err}")
    return ""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--batch-size", type=int, default=30)
    args = ap.parse_args()

    uni = json.load(open(args.universe))
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

    missing_industry = [code for code in ucodes if not (profiles.get(code, {}).get("industry") or "").strip()]
    if missing_industry:
        print(f"[industry-fallback] eastmoney for {len(missing_industry)} stocks")
        with ThreadPoolExecutor(max_workers=8) as pool:
            futs = {pool.submit(_fetch_em_industry, code): code for code in missing_industry}
            for fut in as_completed(futs):
                code = futs[fut]
                industry = fut.result() or ""
                profiles.setdefault(code, {})["industry"] = industry

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
              open(args.out, "w"), ensure_ascii=False, indent=2)
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
