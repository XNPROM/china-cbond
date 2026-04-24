"""Discover all currently-tradable convertible bonds.

Logic:
  1. Start from a seed pool (either a local list or the 历史 seed file).
  2. For each code, query bond name, listed/delist/maturity dates, balance.
  3. Keep only: name exists AND listed <= today AND (delist is empty OR delist > today) AND balance > 0.
  4. Probe beyond the seed max codes for any newly issued bonds.

Usage:
  python3 discover_universe.py \\
      --seed /path/to/seed_codes.txt \\
      --asof 2026-04-20 \\
      --out-json /path/to/cbond_universe.json \\
      --out-csv /path/to/cbond_universe.csv
"""
import argparse, csv, json, os, sys, time
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
from _ifind import basic_data, realtime, batched
from _db import connect, init_schema, upsert as db_upsert


BOND_FIELDS = [
    {"indicator": "ths_bond_short_name_bond", "indiparams": [""]},
    {"indicator": "ths_listed_date_bond", "indiparams": [""]},
    {"indicator": "ths_delist_date_bond", "indiparams": [""]},
    {"indicator": "ths_maturity_date_bond", "indiparams": [""]},
    {"indicator": "ths_stock_code_cbond", "indiparams": [""]},
    {"indicator": "ths_stock_short_name_cbond", "indiparams": [""]},
]


def _yyyymmdd(d: str) -> str:
    return d.replace("-", "")


def _query_bond_meta(codes, asof_ymd):
    """Query basic metadata + balance snapshot as of asof."""
    indipara = BOND_FIELDS + [
        {"indicator": "ths_bond_balance_cbond",
         "indiparams": [asof_ymd[:4] + "-" + asof_ymd[4:6] + "-" + asof_ymd[6:]]},
    ]
    recs = []
    for batch in batched(codes, 50):
        r = basic_data(batch, indipara)
        for t in r.get("tables", []):
            tbl = t.get("table", {})
            name = (tbl.get("ths_bond_short_name_bond") or [""])[0]
            if not name:
                continue
            recs.append({
                "code": t["thscode"],
                "name": name,
                "listed": (tbl.get("ths_listed_date_bond") or [""])[0],
                "delist": (tbl.get("ths_delist_date_bond") or [""])[0],
                "maturity": (tbl.get("ths_maturity_date_bond") or [""])[0],
                "balance": (tbl.get("ths_bond_balance_cbond") or [None])[0],
                "ucode": (tbl.get("ths_stock_code_cbond") or [""])[0],
                "uname": (tbl.get("ths_stock_short_name_cbond") or [""])[0],
            })
        time.sleep(0.12)
    return recs


def _is_alive(r, asof_ymd):
    listed_ok = r["listed"] and r["listed"] <= asof_ymd
    not_delisted = (not r["delist"]) or r["delist"] > asof_ymd
    has_balance = isinstance(r["balance"], (int, float)) and r["balance"] > 0
    return listed_ok and not_delisted and has_balance


def _attach_prices(recs):
    codes = [r["code"] for r in recs]
    px = {}
    for batch in batched(codes, 80):
        try:
            r = realtime(batch, "latest")
            for t in r.get("tables", []):
                v = (t.get("table", {}).get("latest") or [None])[0]
                if isinstance(v, (int, float)) and v > 0:
                    px[t["thscode"]] = v
        except Exception as e:
            print(f"[warn] price batch err: {e}")
        time.sleep(0.15)
    for r in recs:
        r["latest"] = px.get(r["code"])


def _full_scan_ranges():
    """Generate all possible CB code ranges for systematic enumeration.

    CB code patterns: 110xxx.SH, 111xxx.SH, 113xxx.SH, 118xxx.SH,
                      123xxx.SZ, 127xxx.SZ, 128xxx.SZ
    """
    codes = []
    for prefix, market in [("110", "SH"), ("111", "SH"), ("113", "SH"), ("118", "SH"),
                            ("123", "SZ"), ("127", "SZ"), ("128", "SZ")]:
        for n in range(1, 1000):
            codes.append(f"{prefix}{n:03d}.{market}")
    return codes



    """Probe beyond seen ranges for any newly issued CBs."""
    # Build probe codes: next 30 past each seed max per prefix, plus fixed canonical ranges.
    from collections import defaultdict
    by_pref = defaultdict(list)
    for c in seen_codes:
        by_pref[c[:3]].append(c)
    probes = []
    for pref, cs in by_pref.items():
        # numeric suffix
        suf = sorted(int(c[3:].split(".")[0]) for c in cs)
        mx = suf[-1] if suf else 0
        mkt = cs[0].split(".")[1]
        for n in range(mx + 1, mx + 35):
            probes.append(f"{pref}{n:03d}.{mkt}" if len(str(n)) <= 3 else f"{pref}{n}.{mkt}")
    # Always include top of canonical ranges to catch gaps
    for c in ["118051.SH", "123251.SZ", "127107.SZ", "128145.SZ", "113693.SH", "110098.SH"]:
        if c not in seen_codes and c not in probes:
            probes.append(c)
    return list(set(probes))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", required=True, help="seed codes txt, one per line")
    ap.add_argument("--asof", default="2026-04-20", help="YYYY-MM-DD as-of date")
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--out-csv", required=True)
    ap.add_argument("--out-codes", default=None, help="optional plain codes.txt")
    ap.add_argument("--probe", action="store_true", help="probe beyond seed ranges")
    ap.add_argument("--full-scan", action="store_true", help="scan all possible CB code ranges (slow but exhaustive)")
    args = ap.parse_args()

    asof_ymd = _yyyymmdd(args.asof)
    # 1) load seed
    codes = []
    with open(args.seed) as f:
        for line in f:
            line = line.strip()
            if line and "." in line and not line.lower().startswith("thscode"):
                codes.append(line)
    print(f"[seed] {len(codes)} codes from {args.seed}")

    # 2) query metadata & balance
    recs = _query_bond_meta(codes, asof_ymd)
    print(f"[meta] {len(recs)} returned")

    # 3) filter alive
    alive = [r for r in recs if _is_alive(r, asof_ymd)]
    print(f"[alive-seed] {len(alive)}")

    # 4) probe new or full scan
    if args.full_scan:
        scan_codes = _full_scan_ranges()
        # Exclude codes already queried
        seen = {r["code"] for r in recs}
        scan_codes = [c for c in scan_codes if c not in seen]
        print(f"[full-scan] {len(scan_codes)} codes to scan")
        scan_recs = _query_bond_meta(scan_codes, asof_ymd)
        scan_alive = [r for r in scan_recs if _is_alive(r, asof_ymd)]
        print(f"[alive-full-scan] {len(scan_alive)} new bonds found")
        seen_alive = {r["code"] for r in alive}
        for r in scan_alive:
            if r["code"] not in seen_alive:
                alive.append(r)
    elif args.probe:
        seen = {r["code"] for r in recs}
        probes = _probe_ranges(seen, asof_ymd)
        print(f"[probe] {len(probes)} extra codes")
        new_recs = _query_bond_meta(probes, asof_ymd)
        new_alive = [r for r in new_recs if _is_alive(r, asof_ymd)]
        print(f"[alive-probe] {len(new_alive)}")
        # merge (dedupe by code)
        seen_alive = {r["code"] for r in alive}
        for r in new_alive:
            if r["code"] not in seen_alive:
                alive.append(r)

    # 5) attach prices
    _attach_prices(alive)
    alive.sort(key=lambda r: r["code"])

    # 6) write out
    with open(args.out_json, "w") as f:
        json.dump({"asof": args.asof, "count": len(alive), "items": alive},
                  f, ensure_ascii=False, indent=2)
    with open(args.out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["转债代码", "转债简称", "正股代码", "正股简称",
                    "最新价", "余额(亿元)", "到期日", "上市日"])
        for r in alive:
            w.writerow([r["code"], r["name"], r["ucode"], r["uname"],
                        r.get("latest", ""), r.get("balance", ""),
                        r.get("maturity", ""), r.get("listed", "")])
    if args.out_codes:
        with open(args.out_codes, "w") as f:
            for r in alive:
                f.write(r["code"] + "\n")
    print(f"[done] {len(alive)} alive bonds → {args.out_json}")

    # upsert to DuckDB
    now = datetime.utcnow().isoformat()
    db_rows = [
        {
            "code": r["code"], "name": r["name"],
            "ucode": r["ucode"], "uname": r["uname"],
            "list_date": r.get("listed", ""),
            "maturity_date": r.get("maturity", ""),
            "updated_at": now,
        }
        for r in alive
    ]
    con = connect()
    init_schema(con)
    n = db_upsert(con, "universe", db_rows, ["code"])
    con.close()
    print(f"[db] universe upserted {n} rows (asof={args.asof})")


if __name__ == "__main__":
    main()
