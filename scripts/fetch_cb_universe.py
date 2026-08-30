"""Fetch full CB universe from iFinD data_pool API (p05479).

Replaces discover_universe.py (seed+range scan) with a single authoritative
API call that returns ALL tradable convertible bonds plus 申万 L1/L2/L3
industry, ratings, conv price, prospectus, etc.

Condition decoding:
  jyzt=2       交易状态=正常交易
  sfdb=1       是否担保（筛选位，实际不过滤）
  jysc=1       交易所市场
  sszt=213001  证券类型=可转债
  edate=       截止日期 (YYYYMMDD)
  gnfl=0       公募/私募

Usage:
  python3 scripts/fetch_cb_universe.py --date 2026-04-20
"""
import argparse
import csv
import json
import os
import re
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
from _db import connect, init_schema, upsert as db_upsert
from _ifind import history, ths_dr


FIELDS = (
    "jydm:Y,jydm_mc:Y,"
    + ",".join(f"p05479_f{i:03d}:Y" for i in [
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
        18, 19, 20, 21, 22, 25, 26, 27, 28, 29, 30, 31, 32, 33,
        35, 36, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55
    ])
)

# Public exchange CB codes: SH 110/111/113/118, SZ 123/127/128.
# Keep the exchange suffix paired with its prefix; accepting 110xxx.SZ (or
# 123xxx.SH) would create a false code in both the universe and quote audit.
VALID_CB_RE = re.compile(r"^(?:11[0138]\d{3}\.SH|12[378]\d{3}\.SZ)$")
PRIVATE_CB_RE = re.compile(r"(定转|定\d+)")


def _safe(arr, idx, default=""):
    if idx >= len(arr):
        return default
    v = arr[idx]
    return v if v and v != "--" else default


def _date_norm(s):
    """Normalize 2030/04/01 / 2030-04-01 → 20300401."""
    if not s:
        return ""
    return s.replace("/", "").replace("-", "")


def _recovery_candidates(rows, existing_codes, date_ymd):
    candidates = []
    for code, name, ucode, uname, listed, maturity in rows:
        if not VALID_CB_RE.match(code or ""):
            continue
        if PRIVATE_CB_RE.search(name or ""):
            continue
        if code in existing_codes or not listed or listed > date_ymd:
            continue
        if maturity and maturity < date_ymd:
            continue
        candidates.append({
            "code": code,
            "name": name or "",
            "ucode": ucode or "",
            "uname": uname or "",
            "conv_price": "",
            "issue_date": "",
            "maturity": maturity or "",
            "listed": listed or "",
            "face_value": "",
            "coupon": "",
            "rating_issuer": "",
            "rating_bond": "",
            "sw_l1": "",
            "sw_l2": "",
            "sw_l3": "",
            "guarantee": "",
            "prospectus": "",
        })
    return candidates


def _recover_trading_bonds(bonds, date_ymd):
    """Recover p05479 omissions only when the bond has a close on target date."""
    con = connect()
    rows = con.execute(
        "SELECT code, name, ucode, uname, list_date, maturity_date FROM universe"
    ).fetchall()
    con.close()

    existing_codes = {b["code"] for b in bonds}
    candidates = _recovery_candidates(rows, existing_codes, date_ymd)
    if not candidates:
        return bonds

    by_code = {b["code"]: b for b in candidates}
    recovered = []
    for batch in (candidates[i:i + 80] for i in range(0, len(candidates), 80)):
        codes = [b["code"] for b in batch]
        response = history(codes, "close", date_ymd, date_ymd)
        if response.get("errorcode") != 0:
            raise RuntimeError(f"iFinD universe recovery failed: {response.get('errmsg', 'unknown')}")
        for table in response.get("tables", []):
            close = (table.get("table", {}).get("close") or [None])[0]
            code = table.get("thscode")
            if code in by_code and close is not None:
                recovered.append(by_code[code])

    if recovered:
        print(
            f"[recover] restored {len(recovered)} p05479 omissions with target-date close: "
            + ", ".join(f"{b['code']} {b['name']}" for b in recovered)
        )
        bonds.extend(recovered)
        bonds.sort(key=lambda b: b["code"])
    return bonds


def fetch_universe(date_ymd):
    condition = f"jyzt=2;sfdb=1;jysc=1;sszt=213001;edate={date_ymd};gnfl=0"
    r = ths_dr("p05479", condition, FIELDS)

    if r.get("errorcode") != 0:
        raise RuntimeError(f"iFinD data_pool error: {r.get('errmsg', 'unknown')}")

    tables = r.get("tables", [])
    if not tables:
        raise RuntimeError("No data returned from iFinD data_pool")

    tbl = tables[0]["table"]
    n = len(tbl.get("jydm", []))
    print(f"[fetch] {n} bonds from data_pool (edate={date_ymd})")

    bonds = []
    for i in range(n):
        bonds.append({
            "code":          _safe(tbl.get("jydm", []), i),
            "name":          _safe(tbl.get("jydm_mc", []), i),
            "ucode":         _safe(tbl.get("p05479_f021", []), i),   # 正股代码
            "uname":         _safe(tbl.get("p05479_f022", []), i),   # 正股简称
            "conv_price":    _safe(tbl.get("p05479_f009", []), i),   # 转股价
            "issue_date":    _safe(tbl.get("p05479_f007", []), i),   # 发行日
            "maturity":      _safe(tbl.get("p05479_f008", []), i),   # 到期日
            "listed":        _safe(tbl.get("p05479_f019", []), i),   # 上市日
            "face_value":    _safe(tbl.get("p05479_f001", []), i),   # 面值
            "coupon":        _safe(tbl.get("p05479_f005", []), i),   # 票面利率
            "rating_issuer": _safe(tbl.get("p05479_f025", []), i),   # 主体评级
            "rating_bond":   _safe(tbl.get("p05479_f026", []), i),   # 债项评级
            "sw_l1":         _safe(tbl.get("p05479_f041", []), i),   # 申万一级
            "sw_l2":         _safe(tbl.get("p05479_f042", []), i),   # 申万二级
            "sw_l3":         _safe(tbl.get("p05479_f043", []), i),   # 申万三级
            "guarantee":     _safe(tbl.get("p05479_f004", []), i),   # 是否担保
            "prospectus":    _safe(tbl.get("p05479_f036", []), i),   # 募集说明摘要
        })

    # Keep only main-board exchange bonds with continuous auction pricing.
    # Valid prefixes: SH 110/111/113/118, SZ 123/127/128.
    # Excludes: 810xxx (北交所/新三板定转), 145xxx (非标定向转债), etc.
    before = len(bonds)
    bonds = [b for b in bonds if VALID_CB_RE.match(b.get("code", ""))]
    excluded = before - len(bonds)
    if excluded:
        print(f"[filter] excluded {excluded} non-exchange bonds (北交所/新三板/定向)")

    # Exclude 定向转债 that share code prefix with public bonds but are privately placed.
    # Name patterns include 定转, 定01, 定02, etc.
    before = len(bonds)
    bonds = [b for b in bonds if not PRIVATE_CB_RE.search(b.get("name", ""))]
    excluded = before - len(bonds)
    if excluded:
        print(f"[filter] excluded {excluded} 定向转债 by name pattern")
    # Some p05479 snapshots retain already-matured bonds despite the normal
    # trading-state condition. They cannot have a close on the as-of date and
    # would make the exact quote audit impossible, so remove them explicitly.
    before = len(bonds)
    bonds = [
        b for b in bonds
        if not _date_norm(b.get("maturity", ""))
        or _date_norm(b.get("maturity", "")) >= date_ymd
    ]
    excluded = before - len(bonds)
    if excluded:
        print(f"[filter] excluded {excluded} matured bonds before {date_ymd}")
    # p05479 is the authoritative as-of universe.  Do not silently add codes
    # from the persistent local DB here: the subsequent quote audit must
    # compare returned quotes against exactly this fetched list.  The legacy
    # recovery path remains available explicitly via --recover-existing.
    return bonds


def _delete_universe_orphans(con, active_codes, min_active_ratio=0.90):
    """Delete orphans unless the candidate snapshot looks materially partial."""
    codes = sorted(set(active_codes))
    if not codes:
        raise ValueError("refusing to clean universe with an empty active code set")

    existing_count = con.execute("SELECT count(*) FROM universe").fetchone()[0]
    active_ratio = len(codes) / existing_count if existing_count else 1.0
    if active_ratio < min_active_ratio:
        return None

    con.execute(
        "CREATE OR REPLACE TEMP TABLE active_universe_codes "
        "(code TEXT PRIMARY KEY)"
    )
    con.executemany(
        "INSERT INTO active_universe_codes VALUES (?)",
        [(code,) for code in codes],
    )
    orphan_codes = [row[0] for row in con.execute(
        "SELECT u.code FROM universe u "
        "WHERE NOT EXISTS ("
        "SELECT 1 FROM active_universe_codes a WHERE a.code = u.code"
        ") ORDER BY u.code"
    ).fetchall()]
    con.execute(
        "DELETE FROM universe u WHERE NOT EXISTS ("
        "SELECT 1 FROM active_universe_codes a WHERE a.code = u.code"
        ")"
    )
    return orphan_codes


def save_to_db(bonds, date_ymd):
    con = connect()
    init_schema(con)

    try:
        con.execute("BEGIN")
        now = datetime.utcnow().isoformat()
        universe_rows = [{
            "code":          b["code"],
            "name":          b["name"],
            "ucode":         b["ucode"],
            "uname":         b["uname"],
            "list_date":     _date_norm(b["listed"]),
            "maturity_date": _date_norm(b["maturity"]),
            "updated_at":    now,
        } for b in bonds]
        n_u = db_upsert(con, "universe", universe_rows, ["code"])
        print(f"[db] universe upserted {n_u} rows")

        orphan_codes = _delete_universe_orphans(
            con, (b["code"] for b in bonds)
        )
        if orphan_codes is None:
            existing_count = con.execute(
                "SELECT count(*) FROM universe"
            ).fetchone()[0]
            active_count = len({b["code"] for b in bonds})
            print(
                "[db][warning] skipped universe orphan cleanup: "
                f"active snapshot {active_count}/{existing_count} "
                f"({active_count / existing_count:.1%}) is below 90%; "
                "upsert kept, existing rows preserved"
            )
        else:
            print(f"[db] universe deleted {len(orphan_codes)} orphan rows")
            if orphan_codes:
                print("[db] deleted orphan codes: " + ", ".join(orphan_codes))

        # themes table: 申万行业作为一级主题兜底，正式题材仍由 generate_themes_* 覆写
        theme_rows = [{
            "trade_date": f"{date_ymd[:4]}-{date_ymd[4:6]}-{date_ymd[6:]}",
            "code":       b["code"],
            "theme_l1":   b["sw_l2"] or b["sw_l1"] or "其他综合",
            "all_themes_json": json.dumps(
                [t for t in [b["sw_l1"], b["sw_l2"], b["sw_l3"]] if t],
                ensure_ascii=False
            ),
            "business_rewrite": "",
            "industry": b["sw_l1"] or "",
        } for b in bonds]
        n_t = db_upsert(con, "themes", theme_rows, ["trade_date", "code"])
        print(f"[db] themes(申万兜底) upserted {n_t} rows")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD or YYYYMMDD")
    ap.add_argument("--out-json",  default="", help="output JSON path")
    ap.add_argument("--out-csv",   default="", help="output CSV path")
    ap.add_argument("--out-codes", default="", help="output codes.txt path")
    ap.add_argument("--skip-db",   action="store_true")
    ap.add_argument(
        "--recover-existing",
        action="store_true",
        help="Reuse --out-json and recover omitted bonds by target-date close without calling p05479",
    )
    ap.add_argument(
        "--reuse-existing",
        action="store_true",
        help="Reuse --out-json locally, apply current filters, and do not call iFinD",
    )
    args = ap.parse_args()

    date_ymd = args.date.replace("-", "")
    if args.recover_existing and args.reuse_existing:
        raise RuntimeError("--recover-existing and --reuse-existing are mutually exclusive")
    if args.reuse_existing:
        if not args.out_json or not os.path.exists(args.out_json):
            raise RuntimeError("--reuse-existing requires an existing --out-json file")
        with open(args.out_json, encoding="utf-8") as f:
            bonds = json.load(f).get("items", [])
        before = len(bonds)
        bonds = [
            b for b in bonds
            if VALID_CB_RE.match(b.get("code", ""))
            and not PRIVATE_CB_RE.search(b.get("name", ""))
            and (
                not _date_norm(b.get("maturity", ""))
                or _date_norm(b.get("maturity", "")) >= date_ymd
            )
        ]
        print(f"[reuse] local universe {before} → {len(bonds)} bonds after as-of filters")
    elif args.recover_existing:
        if not args.out_json or not os.path.exists(args.out_json):
            raise RuntimeError("--recover-existing requires an existing --out-json file")
        with open(args.out_json, encoding="utf-8") as f:
            bonds = json.load(f).get("items", [])
        bonds = [
            b for b in bonds
            if VALID_CB_RE.match(b.get("code", ""))
            and not PRIVATE_CB_RE.search(b.get("name", ""))
            and (
                not _date_norm(b.get("maturity", ""))
                or _date_norm(b.get("maturity", "")) >= date_ymd
            )
        ]
        bonds = _recover_trading_bonds(bonds, date_ymd)
        print(f"[recover] reused existing universe with {len(bonds)} bonds")
    else:
        bonds = fetch_universe(date_ymd)

    asof = f"{date_ymd[:4]}-{date_ymd[4:6]}-{date_ymd[6:]}"
    base_dir = f"data/raw/asof={asof}"

    json_path = args.out_json or f"{base_dir}/cbond_universe.json"
    os.makedirs(os.path.dirname(json_path), exist_ok=True)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({"asof": asof, "count": len(bonds), "items": bonds},
                  f, ensure_ascii=False, indent=2)
    print(f"[json] → {json_path}")

    csv_path = args.out_csv or f"{base_dir}/cbond_universe.csv"
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["代码", "名称", "正股代码", "正股简称",
                    "申万一级", "申万二级", "申万三级",
                    "转股价", "到期日", "上市日",
                    "主体评级", "债项评级", "是否担保"])
        for b in bonds:
            w.writerow([b["code"], b["name"], b["ucode"], b["uname"],
                        b["sw_l1"], b["sw_l2"], b["sw_l3"],
                        b["conv_price"], b["maturity"], b["listed"],
                        b["rating_issuer"], b["rating_bond"], b["guarantee"]])
    print(f"[csv]  → {csv_path}")

    codes_path = args.out_codes or f"{base_dir}/cbond_codes.txt"
    with open(codes_path, "w") as f:
        for b in bonds:
            f.write(b["code"] + "\n")
    print(f"[txt]  → {codes_path}")

    if not args.skip_db:
        save_to_db(bonds, date_ymd)

    # 申万一级分布
    sw1 = {}
    for b in bonds:
        k = b["sw_l1"] or "未知"
        sw1[k] = sw1.get(k, 0) + 1
    print(f"\n=== 申万一级行业分布 ({len(bonds)} 只) ===")
    for k, c in sorted(sw1.items(), key=lambda x: -x[1]):
        print(f"  {k}: {c}")

    print(f"\n[done] {len(bonds)} bonds for {asof}")


if __name__ == "__main__":
    main()
